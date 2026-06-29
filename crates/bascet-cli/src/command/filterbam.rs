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
const BAM_FLAG_UNMAPPED: u16 = 0x4;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BamFilterMode {
    /// Keep records where BAM flag 0x4 (read unmapped) is not set.
    Mapped,
    /// Keep records where BAM flag 0x4 (read unmapped) is set.
    Unmapped,
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
    num_threads: usize,
    total_mem: Option<ByteSize>,
) -> Result<()> {
    if paths_in.is_empty() || paths_in.len() > 2 {
        bail!("filterbam expects one or two input BAM files");
    }
    std::fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create temp dir {}", path_temp.display()))?;

    info!(
        inputs = paths_in.len(),
        output = %path_out.display(),
        temp_dir = %path_temp.display(),
        keep = ?keep,
        flag_mask = BAM_FLAG_UNMAPPED,
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
            keep,
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
    keep: BamFilterMode,
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
        if should_keep_record_flag(record.flag(), keep) {
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

fn should_keep_record_flag(flag: u16, keep: BamFilterMode) -> bool {
    let is_unmapped = flag & BAM_FLAG_UNMAPPED != 0;
    match keep {
        BamFilterMode::Mapped => !is_unmapped,
        BamFilterMode::Unmapped => is_unmapped,
    }
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

#[cfg(test)]
mod tests {
    use super::{BamFilterMode, should_keep_record_flag};

    #[test]
    fn keep_mapped_uses_unmapped_flag_not_proper_pair_flag() {
        assert!(should_keep_record_flag(0x0, BamFilterMode::Mapped));
        assert!(should_keep_record_flag(0x2, BamFilterMode::Mapped));
        assert!(!should_keep_record_flag(0x4, BamFilterMode::Mapped));
        assert!(!should_keep_record_flag(0x6, BamFilterMode::Mapped));
    }

    #[test]
    fn keep_unmapped_uses_unmapped_flag_not_proper_pair_flag() {
        assert!(!should_keep_record_flag(0x0, BamFilterMode::Unmapped));
        assert!(!should_keep_record_flag(0x2, BamFilterMode::Unmapped));
        assert!(should_keep_record_flag(0x4, BamFilterMode::Unmapped));
        assert!(should_keep_record_flag(0x6, BamFilterMode::Unmapped));
    }
}
