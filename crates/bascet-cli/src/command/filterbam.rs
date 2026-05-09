use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use bytesize::ByteSize;
use clap::{Args, ValueEnum};
use tracing::info;

use super::determine_thread_counts_1;
use super::samtools_rs::{bam, bgzf};
use crate::command::bamsort::DEFAULT_PATH_TEMP;
use crate::utils::{atomic_temp_path_in_dir, publish_atomic_output};

const FILTERBAM_OUTPUT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BamFilterMode {
    /// Keep records matching the "mapped" branch of the legacy samtools flag logic.
    Mapped,
    /// Keep records matching the "unmapped" branch of the legacy samtools flag logic.
    Unmapped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BamPairingMode {
    /// Detect from the first record of the first BAM.
    Auto,
    /// Treat the input as paired alignment and filter on BAM flag 0x2.
    Paired,
    /// Treat the input as single-end alignment and filter on BAM flag 0x4.
    Single,
}

#[derive(Args)]
pub struct FilterBamCMD {
    /// Input BAM file. Repeat once to concatenate/filter two BAM inputs.
    #[arg(short = 'i', long = "in", value_parser, required = true, num_args = 1..=2)]
    pub path_in: Vec<PathBuf>,

    /// Output BAM file.
    #[arg(short = 'o', long = "out", value_parser)]
    pub path_out: PathBuf,

    /// Temp directory for incomplete output.
    #[arg(short = 't', long = "temp", value_parser, default_value = DEFAULT_PATH_TEMP)]
    pub path_temp: PathBuf,

    /// Which record class to keep.
    #[arg(long = "keep", value_enum, default_value_t = BamFilterMode::Mapped)]
    pub keep: BamFilterMode,

    /// Paired/single-end interpretation of the input.
    #[arg(long = "pairing", value_enum, default_value_t = BamPairingMode::Auto)]
    pub pairing: BamPairingMode,

    /// BGZF worker threads used by each input reader and by the output writer.
    #[arg(short = '@', long = "threads", value_parser = clap::value_parser!(usize))]
    pub num_threads: Option<usize>,

    /// Total memory budget. `filterbam` is streaming, but this is accepted for consistency
    /// with other Bascet commands generated from runner memory settings.
    #[arg(
        short = 'm',
        long = "memory",
        value_parser = clap::value_parser!(ByteSize),
    )]
    pub total_mem: Option<ByteSize>,
}

impl FilterBamCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let num_threads = determine_thread_counts_1(self.num_threads)?;
        filter_bam(
            &self.path_in,
            &self.path_out,
            &self.path_temp,
            self.keep,
            self.pairing,
            num_threads,
            self.total_mem,
        )
    }
}

pub fn filter_bam(
    paths_in: &[PathBuf],
    path_out: &Path,
    path_temp: &Path,
    keep: BamFilterMode,
    pairing: BamPairingMode,
    num_threads: usize,
    total_mem: Option<ByteSize>,
) -> Result<()> {
    if paths_in.is_empty() || paths_in.len() > 2 {
        bail!("filterbam expects one or two input BAM files");
    }
    std::fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create temp dir {}", path_temp.display()))?;

    let paired = match pairing {
        BamPairingMode::Auto => detect_paired_alignment(&paths_in[0])?,
        BamPairingMode::Paired => true,
        BamPairingMode::Single => false,
    };
    let flag_mask = if paired { 0x2 } else { 0x4 };
    let keep_set_flag = matches!(keep, BamFilterMode::Unmapped);

    info!(
        inputs = paths_in.len(),
        output = %path_out.display(),
        temp_dir = %path_temp.display(),
        paired_alignment = paired,
        keep = ?keep,
        flag_mask = flag_mask,
        threads = num_threads,
        memory = ?total_mem,
        "FilterBam: starting"
    );

    let path_tmp = atomic_temp_path_in_dir(path_out, path_temp);
    let output = File::create(&path_tmp)
        .with_context(|| format!("create output BAM tmp {}", path_tmp.display()))?;
    let output = BufWriter::with_capacity(FILTERBAM_OUTPUT_BUFFER_SIZE, output);
    let mut writer = bgzf::ParallelWriter::new(output, 6, num_threads);

    let mut expected_header: Option<bam::Header> = None;
    let mut total_read = 0_u64;
    let mut total_written = 0_u64;

    for path_in in paths_in {
        let (header, read, written) = filter_one_bam(
            path_in,
            &mut writer,
            expected_header.as_ref(),
            flag_mask,
            keep_set_flag,
            num_threads,
        )?;
        if expected_header.is_none() {
            expected_header = Some(header);
        }
        total_read += read;
        total_written += written;
    }

    writer
        .finish()
        .with_context(|| format!("finish output BAM {}", path_tmp.display()))?;
    publish_atomic_output(&path_tmp, path_out)
        .with_context(|| format!("publish output BAM {}", path_out.display()))?;

    info!(
        records_read = total_read,
        records_written = total_written,
        "FilterBam: complete"
    );
    Ok(())
}

fn filter_one_bam(
    path_in: &Path,
    writer: &mut bgzf::ParallelWriter,
    expected_header: Option<&bam::Header>,
    flag_mask: u16,
    keep_set_flag: bool,
    num_threads: usize,
) -> Result<(bam::Header, u64, u64)> {
    let input =
        File::open(path_in).with_context(|| format!("open input BAM {}", path_in.display()))?;
    let mut reader = bgzf::ParallelReader::new(input, num_threads);
    let header = bam::Header::read(&mut reader)
        .with_context(|| format!("read BAM header {}", path_in.display()))?;

    if let Some(expected) = expected_header {
        ensure_compatible_headers(expected, &header, path_in)?;
    } else {
        header
            .write(writer)
            .with_context(|| format!("write output BAM header from {}", path_in.display()))?;
    }

    let mut records_read = 0_u64;
    let mut records_written = 0_u64;
    let mut scratch = Vec::new();
    while let Some(record) = bam::Record::read_into(&mut reader, scratch)
        .with_context(|| format!("read BAM record {}", path_in.display()))?
    {
        records_read += 1;
        let has_flag = record.flag() & flag_mask != 0;
        if has_flag == keep_set_flag {
            record
                .write(writer)
                .with_context(|| format!("write filtered BAM record from {}", path_in.display()))?;
            records_written += 1;
        }
        scratch = record.data;
    }

    info!(
        input = %path_in.display(),
        records_read,
        records_written,
        "FilterBam: input complete"
    );
    Ok((header, records_read, records_written))
}

fn detect_paired_alignment(path_in: &Path) -> Result<bool> {
    let input =
        File::open(path_in).with_context(|| format!("open input BAM {}", path_in.display()))?;
    let mut reader = bgzf::Reader::new(input);
    let _header = bam::Header::read(&mut reader)
        .with_context(|| format!("read BAM header {}", path_in.display()))?;
    let record = bam::Record::read(&mut reader)
        .with_context(|| format!("read first BAM record {}", path_in.display()))?;
    let Some(record) = record else {
        bail!(
            "cannot detect paired alignment from empty BAM {}",
            path_in.display()
        );
    };
    Ok(record.flag() & 0x1 != 0)
}

fn ensure_compatible_headers(
    expected: &bam::Header,
    actual: &bam::Header,
    path_in: &Path,
) -> Result<()> {
    if expected.refs.len() != actual.refs.len() {
        bail!(
            "BAM header reference count mismatch in {}: expected {}, found {}",
            path_in.display(),
            expected.refs.len(),
            actual.refs.len()
        );
    }
    for (index, (a, b)) in expected.refs.iter().zip(&actual.refs).enumerate() {
        if a.name != b.name || a.length != b.length {
            return Err(anyhow!(
                "BAM header reference mismatch in {} at index {}",
                path_in.display(),
                index
            ));
        }
    }
    Ok(())
}
