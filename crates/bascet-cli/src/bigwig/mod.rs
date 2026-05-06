use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use bytesize::ByteSize;
use tracing::info;

use crate::command::samtools_rs::{bam, bgzf};
use crate::utils::{atomic_temp_path, publish_atomic_output};

mod writer;

use writer::beddata::BedParserStreamingIterator;
use writer::{BigWigWrite, Value};

#[derive(Clone, Copy)]
pub struct ToBigWigOptions {
    pub bin_size: u32,
    pub skip_unmapped: bool,
    pub skip_secondary: bool,
    pub skip_supplementary: bool,
    pub scale_factor: f32,
    pub total_mem: ByteSize,
    pub num_threads: usize,
}

pub fn bam_to_bigwig(path_in: &Path, path_out: &Path, opts: ToBigWigOptions) -> Result<()> {
    if opts.bin_size == 0 {
        bail!("--bin-size must be greater than zero");
    }
    if !opts.scale_factor.is_finite() {
        bail!("--scale-factor must be finite");
    }

    info!(
        input = %path_in.display(),
        output = %path_out.display(),
        bin_size = opts.bin_size,
        memory = %opts.total_mem,
        threads = opts.num_threads,
        "ToBigWig: starting"
    );

    let (header, mut coverage, records_read, records_used) =
        collect_binned_coverage(path_in, opts)?;
    let chrom_sizes = chrom_sizes(&header)?;
    let values = coverage_values(&header, &mut coverage, opts.bin_size, opts.scale_factor);

    let path_tmp = atomic_temp_path(path_out);
    let writer = BigWigWrite::create_file(&path_tmp, chrom_sizes)
        .map_err(|err| anyhow!("create BigWig {}: {err}", path_tmp.display()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(opts.num_threads.max(1))
        .build()
        .context("create BigWig runtime")?;
    let data = BedParserStreamingIterator::wrap_infallible_iter(values.into_iter(), false);
    writer
        .write(data, runtime)
        .map_err(|err| anyhow!("write BigWig {}: {err}", path_tmp.display()))?;

    publish_atomic_output(&path_tmp, path_out)
        .with_context(|| format!("publish BigWig {}", path_out.display()))?;
    info!(
        records_read,
        records_used,
        output = %path_out.display(),
        "ToBigWig: complete"
    );
    Ok(())
}

fn collect_binned_coverage(
    path_in: &Path,
    opts: ToBigWigOptions,
) -> Result<(bam::Header, Vec<Vec<u32>>, u64, u64)> {
    let input =
        File::open(path_in).with_context(|| format!("open input BAM {}", path_in.display()))?;
    let mut reader = bgzf::ParallelReader::new(input, opts.num_threads);
    let header = bam::Header::read(&mut reader)
        .with_context(|| format!("read BAM header {}", path_in.display()))?;
    let estimated_coverage_memory = estimate_binned_coverage_memory(&header, opts.bin_size)?;
    if estimated_coverage_memory.as_u64() > opts.total_mem.as_u64() {
        bail!(
            "estimated tobigwig coverage memory {} exceeds --memory {}. Increase --memory or use a larger --bin-size.",
            estimated_coverage_memory,
            opts.total_mem
        );
    }
    info!(
        estimated_coverage_memory = %estimated_coverage_memory,
        memory = %opts.total_mem,
        "ToBigWig: coverage memory estimate"
    );
    let mut coverage = init_coverage(&header, opts.bin_size)?;

    let mut records_read = 0_u64;
    let mut records_used = 0_u64;
    let mut scratch = Vec::new();
    while let Some(record) = bam::Record::read_into(&mut reader, scratch)
        .with_context(|| format!("read BAM record {}", path_in.display()))?
    {
        records_read += 1;
        if should_use_record(&record, opts) {
            add_record_coverage(&mut coverage, &record, opts.bin_size)?;
            records_used += 1;
        }
        scratch = record.data;
    }
    Ok((header, coverage, records_read, records_used))
}

fn estimate_binned_coverage_memory(header: &bam::Header, bin_size: u32) -> Result<ByteSize> {
    let mut bytes = std::mem::size_of::<Vec<Vec<u32>>>() as u128;
    bytes += (header.refs.len() as u128) * (std::mem::size_of::<Vec<u32>>() as u128);

    for reference in &header.refs {
        if reference.length < 0 {
            bail!(
                "negative reference length for {}",
                String::from_utf8_lossy(&reference.name)
            );
        }
        let len = reference.length as u128;
        let bins = len.div_ceil(bin_size as u128);
        bytes += bins * (std::mem::size_of::<u32>() as u128);
    }

    let conservative_bytes = bytes
        .checked_mul(11)
        .and_then(|v| v.checked_div(10))
        .ok_or_else(|| anyhow!("coverage memory estimate overflowed"))?;
    let conservative_bytes =
        u64::try_from(conservative_bytes).context("coverage memory estimate exceeds u64 bytes")?;
    Ok(ByteSize(conservative_bytes))
}

fn init_coverage(header: &bam::Header, bin_size: u32) -> Result<Vec<Vec<u32>>> {
    let mut out = Vec::with_capacity(header.refs.len());
    for reference in &header.refs {
        if reference.length < 0 {
            bail!(
                "negative reference length for {}",
                String::from_utf8_lossy(&reference.name)
            );
        }
        let len = reference.length as u32;
        let bins = len.div_ceil(bin_size) as usize;
        out.push(vec![0; bins]);
    }
    Ok(out)
}

fn should_use_record(record: &bam::Record, opts: ToBigWigOptions) -> bool {
    let flag = record.flag();
    if opts.skip_unmapped && flag & 0x4 != 0 {
        return false;
    }
    if opts.skip_secondary && flag & 0x100 != 0 {
        return false;
    }
    if opts.skip_supplementary && flag & 0x800 != 0 {
        return false;
    }
    record.ref_id() >= 0 && record.pos() >= 0
}

fn add_record_coverage(
    coverage: &mut [Vec<u32>],
    record: &bam::Record,
    bin_size: u32,
) -> Result<()> {
    let ref_id = usize::try_from(record.ref_id()).context("negative ref_id")?;
    let Some(chrom_coverage) = coverage.get_mut(ref_id) else {
        bail!("record ref_id {} exceeds BAM header references", ref_id);
    };

    let mut ref_pos = record.pos() as u32;
    for op in record.cigar_raw().chunks_exact(4) {
        let val = u32::from_le_bytes([op[0], op[1], op[2], op[3]]);
        let op_len = val >> 4;
        let op_type = val & 0x0f;
        match op_type {
            0 | 7 | 8 => {
                add_interval_coverage(
                    chrom_coverage,
                    ref_pos,
                    ref_pos.saturating_add(op_len),
                    bin_size,
                );
                ref_pos = ref_pos.saturating_add(op_len);
            }
            2 | 3 => {
                ref_pos = ref_pos.saturating_add(op_len);
            }
            _ => {}
        }
    }
    Ok(())
}

fn add_interval_coverage(chrom_coverage: &mut [u32], start: u32, end: u32, bin_size: u32) {
    if end <= start || chrom_coverage.is_empty() {
        return;
    }
    let first_bin = (start / bin_size) as usize;
    let last_bin = ((end - 1) / bin_size) as usize;
    let last_bin = last_bin.min(chrom_coverage.len() - 1);
    for bin in first_bin..=last_bin {
        let bin_start = (bin as u32).saturating_mul(bin_size);
        let bin_end = bin_start.saturating_add(bin_size);
        let overlap_start = start.max(bin_start);
        let overlap_end = end.min(bin_end);
        if overlap_end > overlap_start {
            chrom_coverage[bin] = chrom_coverage[bin].saturating_add(overlap_end - overlap_start);
        }
    }
}

fn chrom_sizes(header: &bam::Header) -> Result<HashMap<String, u32>> {
    let mut out = HashMap::with_capacity(header.refs.len());
    for reference in &header.refs {
        if reference.length < 0 {
            bail!(
                "negative reference length for {}",
                String::from_utf8_lossy(&reference.name)
            );
        }
        out.insert(
            String::from_utf8(reference.name.clone()).with_context(|| {
                format!(
                    "reference name is not UTF-8: {}",
                    String::from_utf8_lossy(&reference.name)
                )
            })?,
            reference.length as u32,
        );
    }
    Ok(out)
}

fn coverage_values(
    header: &bam::Header,
    coverage: &mut [Vec<u32>],
    bin_size: u32,
    scale_factor: f32,
) -> Vec<(String, Value)> {
    let mut values = Vec::new();
    for (reference, chrom_coverage) in header.refs.iter().zip(coverage.iter()) {
        let chrom = String::from_utf8_lossy(&reference.name).into_owned();
        let chrom_len = reference.length.max(0) as u32;
        let mut run_start: Option<u32> = None;
        let mut run_value = 0_u32;
        for (bin, covered_bases) in chrom_coverage.iter().copied().enumerate() {
            let start = (bin as u32).saturating_mul(bin_size);
            let end = start.saturating_add(bin_size).min(chrom_len);
            if end <= start {
                continue;
            }
            if covered_bases == 0 {
                if let Some(open_start) = run_start.take() {
                    values.push((
                        chrom.clone(),
                        Value {
                            start: open_start,
                            end: start,
                            value: run_value as f32 / bin_size as f32 * scale_factor,
                        },
                    ));
                }
                continue;
            }
            if run_start.is_some() && covered_bases == run_value {
                continue;
            }
            if let Some(open_start) = run_start.replace(start) {
                values.push((
                    chrom.clone(),
                    Value {
                        start: open_start,
                        end: start,
                        value: run_value as f32 / bin_size as f32 * scale_factor,
                    },
                ));
            }
            run_value = covered_bases;
        }
        if let Some(open_start) = run_start {
            values.push((
                chrom,
                Value {
                    start: open_start,
                    end: chrom_len,
                    value: run_value as f32 / bin_size as f32 * scale_factor,
                },
            ));
        }
    }
    values
}
