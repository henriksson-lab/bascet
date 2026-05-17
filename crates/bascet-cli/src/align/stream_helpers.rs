//! Memory-budgeting helpers shared by the Stream-based aligners (STAR, minimap2). These were
//! historically used by the BWA mainline path too, but BWA now drives its own pipeline-internal
//! `ReadMemoryLimiter` inside `align_bwa_stock_driver` and no longer needs these.
//!
//! The whole module is feature-gated to `any(star-rs-align, minimap2-rs-align)` since neither
//! function has a caller without those features.

use bytesize::ByteSize;
use tracing::{info, warn};

/// Pick a stream buffer size that fits within `total_memory` after the aligner index has been
/// loaded. The decode stream is bounded; multi-GB buffering wastes RAM without helping
/// throughput. Aligner thread-local workspace and bgzf write buffers are not separately
/// budgeted, so the hard cap leaves headroom for them. The arena is mmap-backed and pages get
/// RSS-counted as they're touched, so we want this small.
pub(super) fn stream_buffer_after_index_load(
    aligner_name: &str,
    total_memory: ByteSize,
    requested_stream_buffer: ByteSize,
    sizeof_stream_arena: ByteSize,
    max_stream_buffer: ByteSize,
) -> ByteSize {
    let minimum_stream_buffer = ByteSize(
        (sizeof_stream_arena.as_u64() * 2)
            .max(ByteSize::mib(64).as_u64())
            .min(requested_stream_buffer.as_u64()),
    );
    let memory_headroom = ByteSize(
        ByteSize::mib(512)
            .as_u64()
            .max((total_memory.as_u64() as f64 * 0.05) as u64),
    );
    // Decoded reads in flight cap at ~5 × per-batch cap (≤128 MiB) ≈ 640 MiB; BAM writer adds a
    // few hundred MiB. 1 GiB is a generous reserve; the per-thread × 256 MiB scaling here
    // pre-dated the outer-batch cap and over-reserved by 10× on high-thread runs.
    let future_reading_reserve = ByteSize::gib(1);

    let Some(memory) = memory_stats::memory_stats() else {
        warn!(
            aligner = aligner_name,
            total_memory = %total_memory,
            requested_stream_buffer = %requested_stream_buffer,
            "Could not read current RSS after aligner index load; using requested stream buffer"
        );
        return ByteSize(
            requested_stream_buffer
                .as_u64()
                .min(max_stream_buffer.as_u64()),
        );
    };

    let index_loaded_rss = ByteSize(memory.physical_mem as u64);
    let available_for_stream = total_memory
        .as_u64()
        .saturating_sub(index_loaded_rss.as_u64())
        .saturating_sub(memory_headroom.as_u64())
        .saturating_sub(future_reading_reserve.as_u64());
    let available_after_index = ByteSize(
        total_memory
            .as_u64()
            .saturating_sub(index_loaded_rss.as_u64()),
    );
    let adjusted = ByteSize(
        available_for_stream
            .max(minimum_stream_buffer.as_u64())
            .min(requested_stream_buffer.as_u64())
            .min(max_stream_buffer.as_u64()),
    );

    if adjusted == minimum_stream_buffer && adjusted < requested_stream_buffer {
        warn!(
            aligner = aligner_name,
            index_loaded_rss = %index_loaded_rss,
            total_memory = %total_memory,
            available_after_index = %available_after_index,
            requested_stream_buffer = %requested_stream_buffer,
            memory_headroom = %memory_headroom,
            future_reading_reserve = %future_reading_reserve,
            max_stream_buffer = %max_stream_buffer,
            adjusted_stream_buffer = %adjusted,
            "Aligner index leaves little budget for stream buffers; using minimum stream buffer"
        );
    } else {
        info!(
            aligner = aligner_name,
            index_loaded_rss = %index_loaded_rss,
            total_memory = %total_memory,
            available_after_index = %available_after_index,
            available_for_stream = %ByteSize(available_for_stream),
            requested_stream_buffer = %requested_stream_buffer,
            memory_headroom = %memory_headroom,
            future_reading_reserve = %future_reading_reserve,
            max_stream_buffer = %max_stream_buffer,
            adjusted_stream_buffer = %adjusted,
            "Adjusted aligner stream buffer after index load"
        );
    }

    adjusted
}

/// Derive a per-batch input-bytes cap so that index_rss + cap × bytes_per_base + reserves stays
/// within `total_memory`. Used by the minimap2 path; bwa-mem2 has its own pipelined memory
/// limiter inside the stock driver.
#[cfg(feature = "minimap2-rs-align")]
pub(super) fn aligner_batch_bases_cap(
    aligner_name: &str,
    total_memory: ByteSize,
    bytes_per_base_estimate: u64,
    absolute_cap: usize,
) -> usize {
    let Some(memory) = memory_stats::memory_stats() else {
        warn!(
            aligner = aligner_name,
            total_memory = %total_memory,
            absolute_cap,
            "Could not read RSS to derive adaptive batch cap; using absolute cap"
        );
        return absolute_cap;
    };
    let index_rss = ByteSize(memory.physical_mem as u64);
    // Reserve: matches the 2 GiB cap used by `stream_buffer_after_index_load` for the decode
    // buffer, plus 1 GiB for batches in flight (5 × cap-sized arenas + writer Vec<RecordBuf>),
    // plus 4 GiB safety so transient peaks don't blow the budget.
    let stream_buffer_reserve = ByteSize::gib(2);
    let pipeline_reserve = ByteSize::gib(1);
    let safety_margin = ByteSize::gib(4);
    let available = total_memory
        .as_u64()
        .saturating_sub(index_rss.as_u64())
        .saturating_sub(stream_buffer_reserve.as_u64())
        .saturating_sub(pipeline_reserve.as_u64())
        .saturating_sub(safety_margin.as_u64());
    let from_memory = available / bytes_per_base_estimate.max(1);
    let from_memory = usize::try_from(from_memory).unwrap_or(usize::MAX);
    let floor = ByteSize::mib(1).as_u64() as usize;
    let cap = from_memory.min(absolute_cap).max(floor);
    info!(
        aligner = aligner_name,
        total_memory = %total_memory,
        index_rss = %index_rss,
        bytes_per_base_estimate,
        from_memory_bytes = from_memory,
        absolute_cap_bytes = absolute_cap,
        chosen_cap_bytes = cap,
        "Adaptive batch-bases cap from memory budget"
    );
    cap
}
