//! `bam-sort` subcommand and reusable `sort_and_index_bam` API. Both delegate to the
//! vendored `samtools_rs` module (pure-Rust port of `samtools sort` + `samtools index`).
//!
//! Publish flow: writes the sorted BAM and `.bai` to hidden files under `path_temp`, then
//! publishes them to their final paths on success. Spill chunks also live under `path_temp` and
//! are deleted after a successful sort (samtools-rs handles spill-file cleanup internally).

use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytesize::ByteSize;
use clap::Args;
use tracing::info;

use super::determine_thread_counts_1;
use super::samtools_rs::sort::{
    IndexFormat, Order, ReferenceOrder, SortOptions, sort_streaming_parallel,
};
use crate::utils::{atomic_temp_path_in_dir, publish_atomic_output};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_MEMORY: &str = "8GB";
const SORT_MEMORY_FRACTION: f64 = 0.60;
const SORT_EXTRA_IN_FLIGHT_BUFFERS: usize = 2;

#[derive(Args)]
pub struct BamSortCMD {
    /// Input BAM file (any sort order; will be coordinate-sorted on output).
    #[arg(short = 'i', long = "in", value_parser)]
    pub path_in: PathBuf,

    /// Output BAM file (coordinate-sorted). A `.bai` index is written alongside.
    #[arg(short = 'o', long = "out", value_parser)]
    pub path_out: PathBuf,

    /// Directory for spill chunks. Cleaned by samtools-rs on success.
    #[arg(short = 't', long = "temp", value_parser, default_value = DEFAULT_PATH_TEMP)]
    pub path_temp: PathBuf,

    /// Total in-memory budget for the sort phase, split across threads.
    #[arg(short = 'm', long = "memory", value_parser, default_value = DEFAULT_MEMORY)]
    pub memory: ByteSize,

    /// Total threads.
    #[arg(short = '@', long = "threads", value_parser = clap::value_parser!(usize))]
    pub num_threads: Option<usize>,

    /// Preserve the input BAM reference order instead of sorting @SQ/reference IDs by name.
    #[arg(long = "preserve-reference-order")]
    pub preserve_reference_order: bool,
}

impl BamSortCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let num_threads = determine_thread_counts_1(self.num_threads)?;
        sort_and_index_bam(
            &self.path_in,
            &self.path_out,
            &self.path_temp,
            self.memory,
            num_threads,
            if self.preserve_reference_order {
                ReferenceOrder::Preserve
            } else {
                ReferenceOrder::Lexicographic
            },
        )
    }
}

/// Sort `path_in` (BAM) into `path_out_sorted` and write a BAI index alongside as
/// `<path_out_sorted>.bai`. Both outputs are atomic-published. Used by the `bam-sort`
/// subcommand and by every aligner's post-align flow.
///
/// Backed by the vendored `samtools_rs` port. Threads + memory map directly to its
/// `SortOptions`; the index is written by samtools-rs's `BaiBuilder` driven from
/// `write_index = Some((bai_path_tmp, IndexFormat::Bai))`.
pub fn sort_and_index_bam(
    path_in: &Path,
    path_out_sorted: &Path,
    path_temp: &Path,
    memory: ByteSize,
    num_threads: usize,
    reference_order: ReferenceOrder,
) -> Result<()> {
    info!(
        input = %path_in.display(),
        output = %path_out_sorted.display(),
        temp_dir = %path_temp.display(),
        memory = %memory,
        threads = num_threads,
        "BamSort: starting (samtools-rs backend)"
    );

    std::fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create temp dir {}", path_temp.display()))?;

    // samtools-rs's `SortOptions::max_mem` is the per-chunk budget. The parallel spill
    // implementation can hold roughly `threads + 1` chunks concurrently, plus BGZF buffers,
    // recycled record data, and allocator-retained pages. Keep a deliberate reserve so
    // `--memory` remains a process-level budget instead of just the sum of worker chunks.
    let total_mem = usize::try_from(memory.as_u64())
        .map_err(|_| anyhow::anyhow!("memory cap exceeds usize"))?;
    let sort_mem = ((total_mem as f64) * SORT_MEMORY_FRACTION) as usize;
    let in_flight_buffers = num_threads
        .max(1)
        .saturating_add(SORT_EXTRA_IN_FLIGHT_BUFFERS);
    let max_mem_per_chunk = (sort_mem / in_flight_buffers.max(1)).max(64 * 1024 * 1024);
    info!(
        sort_memory_fraction = SORT_MEMORY_FRACTION,
        sort_mem_bytes = sort_mem,
        max_mem_per_chunk,
        in_flight_buffers,
        "BamSort: memory budget"
    );

    // Publish paths for both BAM and BAI. Keep these under the job temp dir so concurrent
    // jobs do not leave partial output in the final output directory.
    let path_out_tmp = atomic_temp_path_in_dir(path_out_sorted, path_temp);
    let path_bai = bai_path(path_out_sorted);
    let path_bai_tmp = atomic_temp_path_in_dir(&path_bai, path_temp);

    // samtools-rs uses `<tmp_prefix>.NNNN.bam` for spill chunks; deletes them on success.
    let tmp_prefix = path_temp.join("bascet-bamsort");

    let opts = SortOptions {
        order: Order::Coordinate,
        reference_order,
        level: 6,
        arg_list: None,
        no_pg: true, // we don't have a stable @PG line to add
        max_mem: max_mem_per_chunk,
        tmp_prefix,
        threads: num_threads.max(1),
        write_index: Some((path_bai_tmp.clone(), IndexFormat::Bai)),
    };

    // Open input + output. Output goes to a `.tmp` and is renamed atomically.
    let input_file =
        File::open(path_in).with_context(|| format!("open input BAM {}", path_in.display()))?;
    let output_file = File::create(&path_out_tmp)
        .with_context(|| format!("create output BAM tmp {}", path_out_tmp.display()))?;
    let output_file = BufWriter::with_capacity(1 << 20, output_file);

    sort_streaming_parallel(input_file, output_file, &opts)
        .with_context(|| format!("samtools-rs sort failed for {}", path_in.display()))?;

    // Both files exist at their `.tmp` paths now; publish atomically.
    publish_atomic_output(&path_out_tmp, &path_out_sorted.to_path_buf())?;
    publish_atomic_output(&path_bai_tmp, &path_bai)?;
    info!(
        output = %path_out_sorted.display(),
        index = %path_bai.display(),
        "BamSort: complete"
    );
    Ok(())
}

fn bai_path(bam_path: &Path) -> PathBuf {
    let mut s = bam_path.as_os_str().to_owned();
    s.push(".bai");
    PathBuf::from(s)
}
