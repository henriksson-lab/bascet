use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use bytesize::ByteSize;
use tracing::info;

use super::bwa_stock_driver::{StockDriverState, run_stock_driver_tirp_to_bam};
use crate::command::{bamsort::sort_and_index_bam, samtools_rs::sort::ReferenceOrder};

/// Drive the BWAMEM2 stock driver end-to-end: TIRP → pipelined BAM (reader → aligner →
/// compressor pool → writer with bounded memory + in-flight limiters) → sort → index.
pub fn try_execute_bwa_mem2(
    path_in: &Path,
    path_genome: &Path,
    path_out_unsorted: &PathBuf,
    path_out_sorted: &PathBuf,
    path_temp: &PathBuf,
    align_threads: usize,
    total_memory: ByteSize,
    total_threads: u64,
    worker_pool: Arc<rayon::ThreadPool>,
    mem_overhead_per_input_byte: u64,
) -> Result<()> {
    info!("BWAMEM2 selected");
    let index_disk_size = validate_bwa_mem2_index(path_genome)?;
    super::common::warn_if_index_disk_size_exceeds_memory(
        "BWAMEM2",
        path_genome,
        index_disk_size,
        total_memory,
    );

    let prefix = path_genome
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("BWAMEM2 genome path is not UTF-8: {path_genome:?}"))?;
    info!(index_prefix = prefix, "Loading BWAMEM2 index");
    let mut state = StockDriverState::new(prefix, align_threads)?;
    info!("BWAMEM2 index loaded");

    run_stock_driver_tirp_to_bam(
        &mut state,
        path_in,
        path_out_unsorted,
        path_temp,
        total_memory,
        total_threads,
        worker_pool,
        mem_overhead_per_input_byte,
    )?;
    // Free the FMI index + worker_t before the sort phase so the in-process sort gets the
    // full memory budget.
    drop(state);

    info!("Sorting + indexing BAM file (in-process)");
    sort_and_index_bam(
        path_out_unsorted,
        path_out_sorted,
        path_temp,
        total_memory,
        total_threads as usize,
        ReferenceOrder::Lexicographic,
    )?;

    info!("BWAMEM2 alignment complete");
    Ok(())
}

fn validate_bwa_mem2_index(index_prefix: &Path) -> Result<u64> {
    let required_suffixes = [".0123", ".amb", ".ann", ".bwt.2bit.64", ".pac"];
    let mut total_size = 0_u64;
    for suffix in required_suffixes {
        let path = PathBuf::from(format!("{}{}", index_prefix.display(), suffix));
        if !path.is_file() {
            anyhow::bail!(
                "BWAMEM2 aligner requires an existing bwa-mem2 index prefix; missing required index file {path:?}. Build the index first, then pass the reference prefix with `--genome`."
            );
        }
        total_size = total_size.saturating_add(
            path.metadata()
                .with_context(|| format!("failed to stat BWAMEM2 index file {path:?}"))?
                .len(),
        );
    }
    Ok(total_size)
}
