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

const DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const MIN_FILTERBAM_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024;
const MIN_FILTERBAM_MEMORY: ByteSize = ByteSize::mib(32);
const FILTERBAM_MEMORY_RESERVE: ByteSize = ByteSize::mib(16);
const FILTERBAM_ESTIMATED_BYTES_PER_BGZF_QUEUE_BLOCK: u64 = (bgzf::BLOCK_SIZE as u64 + 0x10000) * 3;
const BAM_FLAG_SEGMENTED: u16 = 0x1;
const BAM_FLAG_UNMAPPED: u16 = 0x4;
const BAM_FLAG_FIRST_SEGMENT: u16 = 0x40;
const BAM_FLAG_LAST_SEGMENT: u16 = 0x80;
const DEFAULT_MIN_MATCHING: u32 = 0;
const DEFAULT_MIN_MATCHING_PERCENT: u8 = 90;

#[derive(Clone, Copy, Debug)]
pub struct AlignmentFilter {
    pub min_matching: u32,
    pub min_matching_percent: u8,
}

#[derive(Clone, Copy, Debug)]
struct FilterBamMemoryPlan {
    output_buffer_size: usize,
    bgzf_queue_capacity: usize,
}

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

    /// Which alignment class to keep. For adjacent paired-end R1/R2 records, the pair is
    /// retained if either mate matches, and both mates are written so downstream paired readers
    /// still see complete pairs.
    #[arg(long = "keep", value_enum, default_value_t = BamFilterMode::Mapped)]
    pub keep: BamFilterMode,

    /// Minimum CIGAR M-bases for a BAM-mapped read to be considered aligned.
    /// The default of 0 disables this absolute cutoff.
    #[arg(long = "min-matching", value_parser, default_value_t = DEFAULT_MIN_MATCHING)]
    pub min_matching: u32,

    /// Minimum percent of read bases covered by CIGAR M operations for a BAM-mapped read
    /// to be considered aligned. Use 0 to disable this fractional cutoff.
    #[arg(
        long = "min-matching-percent",
        value_parser = clap::value_parser!(u8).range(0..=100),
        default_value_t = DEFAULT_MIN_MATCHING_PERCENT
    )]
    pub min_matching_percent: u8,

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
            AlignmentFilter {
                min_matching: self.min_matching,
                min_matching_percent: self.min_matching_percent,
            },
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
    alignment_filter: AlignmentFilter,
    num_threads: usize,
    total_mem: Option<ByteSize>,
) -> Result<()> {
    if paths_in.is_empty() || paths_in.len() > 2 {
        bail!("filterbam expects one or two input BAM files");
    }
    let memory_plan = filterbam_memory_plan(total_mem, num_threads)?;
    std::fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create temp dir {}", path_temp.display()))?;

    info!(
        inputs = paths_in.len(),
        output = %path_out.display(),
        temp_dir = %path_temp.display(),
        keep = ?keep,
        flag_mask = BAM_FLAG_UNMAPPED,
        min_matching = alignment_filter.min_matching,
        min_matching_percent = alignment_filter.min_matching_percent,
        threads = num_threads,
        memory = ?total_mem,
        output_buffer_size = memory_plan.output_buffer_size,
        bgzf_queue_capacity = memory_plan.bgzf_queue_capacity,
        "FilterBam: starting"
    );

    let path_tmp = atomic_temp_path_in_dir(path_out, path_temp);
    let output = File::create(&path_tmp)
        .with_context(|| format!("create output BAM tmp {}", path_tmp.display()))?;
    let output = BufWriter::with_capacity(memory_plan.output_buffer_size, output);
    let mut writer = bgzf::ParallelWriter::new_with_queue_capacity(
        output,
        6,
        num_threads,
        memory_plan.bgzf_queue_capacity,
    );

    let mut expected_header: Option<bam::Header> = None;
    let mut total_read = 0_u64;
    let mut total_written = 0_u64;

    for path_in in paths_in {
        let (header, read, written) = filter_one_bam(
            path_in,
            &mut writer,
            expected_header.as_ref(),
            keep,
            alignment_filter,
            num_threads,
            memory_plan.bgzf_queue_capacity,
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
    alignment_filter: AlignmentFilter,
    num_threads: usize,
    bgzf_queue_capacity: usize,
) -> Result<(bam::Header, u64, u64)> {
    let input =
        File::open(path_in).with_context(|| format!("open input BAM {}", path_in.display()))?;
    let mut reader =
        bgzf::ParallelReader::new_with_queue_capacity(input, num_threads, bgzf_queue_capacity);
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
    let mut pushback: Option<bam::Record> = None;
    while let Some(record) = match pushback.take() {
        Some(record) => Some(record),
        None => bam::Record::read(&mut reader)
            .with_context(|| format!("read BAM record {}", path_in.display()))?,
    } {
        records_read += 1;

        if is_first_segment(&record) {
            let next = bam::Record::read(&mut reader)
                .with_context(|| format!("read BAM record {}", path_in.display()))?;
            if let Some(next) = next {
                records_read += 1;
                if is_adjacent_second_mate(&record, &next) {
                    if should_keep_record(&record, keep, alignment_filter)
                        || should_keep_record(&next, keep, alignment_filter)
                    {
                        record.write(writer).with_context(|| {
                            format!("write filtered BAM record from {}", path_in.display())
                        })?;
                        next.write(writer).with_context(|| {
                            format!("write filtered BAM record from {}", path_in.display())
                        })?;
                        records_written += 2;
                    }
                    continue;
                }
                pushback = Some(next);
                records_read -= 1;
            }
        }

        if should_keep_record(&record, keep, alignment_filter) {
            record
                .write(writer)
                .with_context(|| format!("write filtered BAM record from {}", path_in.display()))?;
            records_written += 1;
        }
    }

    info!(
        input = %path_in.display(),
        records_read,
        records_written,
        "FilterBam: input complete"
    );
    Ok((header, records_read, records_written))
}

fn filterbam_memory_plan(
    total_mem: Option<ByteSize>,
    num_threads: usize,
) -> Result<FilterBamMemoryPlan> {
    let default_queue_capacity = num_threads.max(1) * 2;
    let Some(total_mem) = total_mem else {
        return Ok(FilterBamMemoryPlan {
            output_buffer_size: DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE,
            bgzf_queue_capacity: default_queue_capacity,
        });
    };

    if total_mem < MIN_FILTERBAM_MEMORY {
        bail!("filterbam --memory must be at least {MIN_FILTERBAM_MEMORY}; got {total_mem}");
    }

    let usable = total_mem
        .as_u64()
        .saturating_sub(FILTERBAM_MEMORY_RESERVE.as_u64());
    let output_buffer_size = (usable / 8).clamp(
        MIN_FILTERBAM_OUTPUT_BUFFER_SIZE as u64,
        DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE as u64,
    ) as usize;
    let queue_budget = usable.saturating_sub(output_buffer_size as u64);
    let queue_capacity = (queue_budget / FILTERBAM_ESTIMATED_BYTES_PER_BGZF_QUEUE_BLOCK)
        .max(1)
        .min(default_queue_capacity as u64) as usize;

    Ok(FilterBamMemoryPlan {
        output_buffer_size,
        bgzf_queue_capacity: queue_capacity,
    })
}

fn should_keep_record(record: &bam::Record, keep: BamFilterMode, filter: AlignmentFilter) -> bool {
    let is_aligned = is_record_aligned(record, filter);
    match keep {
        BamFilterMode::Mapped => is_aligned,
        BamFilterMode::Unmapped => !is_aligned,
    }
}

fn is_record_aligned(record: &bam::Record, filter: AlignmentFilter) -> bool {
    if record.flag() & BAM_FLAG_UNMAPPED != 0 {
        return false;
    }

    let matching_bases = count_matching_bases(record);
    let passes_absolute = filter.min_matching > 0 && matching_bases >= filter.min_matching;
    let passes_percent = if filter.min_matching_percent == 0 {
        true
    } else {
        let read_len = record.l_seq().max(0) as u32;
        let required = (read_len as u64 * filter.min_matching_percent as u64).div_ceil(100) as u32;
        matching_bases >= required
    };

    passes_absolute || passes_percent
}

fn count_matching_bases(record: &bam::Record) -> u32 {
    record
        .cigar_raw()
        .chunks_exact(4)
        .filter_map(|op| {
            let raw = u32::from_le_bytes(op.try_into().unwrap());
            let kind = raw & 0x0f;
            let len = raw >> 4;
            (kind == 0).then_some(len)
        })
        .sum()
}

fn is_first_segment(record: &bam::Record) -> bool {
    let flag = record.flag();
    flag & BAM_FLAG_SEGMENTED != 0 && flag & BAM_FLAG_FIRST_SEGMENT != 0
}

fn is_adjacent_second_mate(first: &bam::Record, second: &bam::Record) -> bool {
    let second_flag = second.flag();
    second_flag & BAM_FLAG_SEGMENTED != 0
        && second_flag & BAM_FLAG_LAST_SEGMENT != 0
        && first.read_name() == second.read_name()
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
    use super::{
        AlignmentFilter, BAM_FLAG_FIRST_SEGMENT, BAM_FLAG_LAST_SEGMENT, BAM_FLAG_SEGMENTED,
        BAM_FLAG_UNMAPPED, BamFilterMode, DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE,
        MIN_FILTERBAM_MEMORY, filterbam_memory_plan, is_adjacent_second_mate, is_record_aligned,
        should_keep_record,
    };
    use crate::command::samtools_rs::bam::Record;
    use bytesize::ByteSize;

    #[test]
    fn keep_mapped_requires_bam_mapped_and_enough_matching_bases() {
        let filter = default_alignment_filter();
        let aligned = test_record_with_cigar(b"cell:umi", 0x0, 10, &[(10, 0)]);
        let weak = test_record_with_cigar(b"cell:umi", 0x0, 10, &[(7, 0), (3, 4)]);
        let unmapped = test_record_with_cigar(b"cell:umi", BAM_FLAG_UNMAPPED, 10, &[]);

        assert!(should_keep_record(&aligned, BamFilterMode::Mapped, filter));
        assert!(!should_keep_record(&weak, BamFilterMode::Mapped, filter));
        assert!(!should_keep_record(
            &unmapped,
            BamFilterMode::Mapped,
            filter
        ));
    }

    #[test]
    fn keep_unmapped_is_complement_of_alignment_filter() {
        let filter = default_alignment_filter();
        let aligned = test_record_with_cigar(b"cell:umi", 0x0, 10, &[(9, 0), (1, 4)]);
        let weak = test_record_with_cigar(b"cell:umi", 0x0, 10, &[(7, 0), (3, 4)]);
        let unmapped = test_record_with_cigar(b"cell:umi", BAM_FLAG_UNMAPPED, 10, &[]);

        assert!(!should_keep_record(
            &aligned,
            BamFilterMode::Unmapped,
            filter
        ));
        assert!(should_keep_record(&weak, BamFilterMode::Unmapped, filter));
        assert!(should_keep_record(
            &unmapped,
            BamFilterMode::Unmapped,
            filter
        ));
    }

    #[test]
    fn absolute_matching_cutoff_can_make_alignment_pass() {
        let filter = AlignmentFilter {
            min_matching: 6,
            min_matching_percent: 80,
        };
        let record = test_record_with_cigar(b"cell:umi", 0x0, 10, &[(6, 0), (4, 4)]);

        assert!(is_record_aligned(&record, filter));
    }

    #[test]
    fn zero_percent_disables_fractional_cutoff() {
        let filter = AlignmentFilter {
            min_matching: 0,
            min_matching_percent: 0,
        };
        let record = test_record_with_cigar(b"cell:umi", 0x0, 10, &[]);

        assert!(is_record_aligned(&record, filter));
    }

    #[test]
    fn adjacent_r1_r2_with_same_name_is_detected_as_pair() {
        let r1 = test_record_with_cigar(
            b"cell:umi",
            BAM_FLAG_SEGMENTED | BAM_FLAG_FIRST_SEGMENT,
            10,
            &[(10, 0)],
        );
        let r2 = test_record_with_cigar(
            b"cell:umi",
            BAM_FLAG_SEGMENTED | BAM_FLAG_LAST_SEGMENT,
            10,
            &[(10, 0)],
        );

        assert!(is_adjacent_second_mate(&r1, &r2));
    }

    #[test]
    fn different_read_name_is_not_detected_as_pair() {
        let r1 = test_record_with_cigar(
            b"cell:umi1",
            BAM_FLAG_SEGMENTED | BAM_FLAG_FIRST_SEGMENT,
            10,
            &[(10, 0)],
        );
        let r2 = test_record_with_cigar(
            b"cell:umi2",
            BAM_FLAG_SEGMENTED | BAM_FLAG_LAST_SEGMENT,
            10,
            &[(10, 0)],
        );

        assert!(!is_adjacent_second_mate(&r1, &r2));
    }

    #[test]
    fn pair_is_retained_when_either_mate_matches_filter() {
        let filter = default_alignment_filter();
        let r1 = test_record_with_cigar(
            b"cell:umi",
            BAM_FLAG_SEGMENTED | BAM_FLAG_FIRST_SEGMENT,
            10,
            &[(10, 0)],
        );
        let r2 = test_record_with_cigar(
            b"cell:umi",
            BAM_FLAG_SEGMENTED | BAM_FLAG_LAST_SEGMENT | BAM_FLAG_UNMAPPED,
            10,
            &[],
        );

        assert!(
            should_keep_record(&r1, BamFilterMode::Mapped, filter)
                || should_keep_record(&r2, BamFilterMode::Mapped, filter)
        );
    }

    #[test]
    fn memory_plan_without_budget_preserves_default_queue_depth() {
        let plan = filterbam_memory_plan(None, 5).unwrap();

        assert_eq!(
            plan.output_buffer_size,
            DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE
        );
        assert_eq!(plan.bgzf_queue_capacity, 10);
    }

    #[test]
    fn memory_plan_with_generous_budget_preserves_default_queue_depth() {
        let plan = filterbam_memory_plan(Some(ByteSize::gib(10)), 5).unwrap();

        assert_eq!(
            plan.output_buffer_size,
            DEFAULT_FILTERBAM_OUTPUT_BUFFER_SIZE
        );
        assert_eq!(plan.bgzf_queue_capacity, 10);
    }

    #[test]
    fn memory_plan_rejects_too_small_budget() {
        assert!(filterbam_memory_plan(Some(MIN_FILTERBAM_MEMORY - ByteSize(1)), 5).is_err());
    }

    fn default_alignment_filter() -> AlignmentFilter {
        AlignmentFilter {
            min_matching: 0,
            min_matching_percent: 90,
        }
    }

    fn test_record_with_cigar(
        read_name: &[u8],
        flag: u16,
        read_len: u32,
        cigar_ops: &[(u32, u8)],
    ) -> Record {
        let l_read_name = read_name.len() + 1;
        let n_cigar = cigar_ops.len();
        let seq_len = read_len as usize;
        let seq_bytes = seq_len.div_ceil(2);
        let mut data = vec![0_u8; 32 + l_read_name + 4 * n_cigar + seq_bytes + seq_len];
        data[8] = l_read_name as u8;
        data[12..14].copy_from_slice(&(n_cigar as u16).to_le_bytes());
        data[14..16].copy_from_slice(&flag.to_le_bytes());
        data[16..20].copy_from_slice(&(read_len as i32).to_le_bytes());
        data[32..32 + read_name.len()].copy_from_slice(read_name);
        let cigar_off = 32 + l_read_name;
        for (idx, (len, kind)) in cigar_ops.iter().enumerate() {
            let raw = (len << 4) | u32::from(*kind);
            let off = cigar_off + 4 * idx;
            data[off..off + 4].copy_from_slice(&raw.to_le_bytes());
        }
        Record { data }
    }
}
