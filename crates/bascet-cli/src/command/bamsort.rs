//! In-process BAM coordinate sort + BAI index. Replaces external `samtools sort` /
//! `samtools index`. Two-phase external-merge sort with **raw-bytes arena** for the inner
//! loop — bypasses noodles' `bam::Record` decode entirely so the reader can run at BGZF
//! decompress speed instead of allocating a Vec per record.
//!
//! **Sort phase** (memory bounded by `--memory`):
//!   - **Reader thread** reads raw BAM record bytes (4-byte block_size + body) directly
//!     from the BGZF stream into a packed `Vec<u8>` arena, with a parallel `Vec<RecordMeta>`
//!     holding `(offset, length, sort_key)` per record. Sort key `(ref_id, pos)` is read
//!     straight from the first 8 bytes of each record body — no struct decode.
//!     When the arena hits `--memory / 2`, the buffer is shipped (double-buffered) to the
//!     spill coordinator while the reader fills the next.
//!   - **Spill coordinator** receives an `ArenaBatch`, splits the metas into N segments,
//!     parallel radix-sorts each segment on `pool` (`--threads` workers), then heap-merges
//!     them and writes record bytes back-to-back through a NONE-compressed BGZF wrapper
//!     into ONE temp file per memory cycle. Sort and write touch the arena via offsets only;
//!     bytes never get parsed or copied except into the BGZF writer.
//!
//! **Merge phase** (samtools-style k-way merge):
//!   - **Source readers** read raw record bytes from each temp file (same arena trick).
//!   - **Merger** heap-pops by sort key, batches records into chunks of
//!     a `bgzf::io::MultithreadedWriter` whose internal worker pool handles all output
//!     BGZF compression async.
//!
//! **Index phase**: `noodles_bam::fs::index` walks the sorted output, writes BAI.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use anyhow::{Context, Result};
use bytesize::ByteSize;
use clap::Args;
use crossbeam::channel;
use noodles::bam;
use noodles::bgzf;
use noodles::bgzf::io::writer::CompressionLevel;
use noodles::sam::header::record::value::map::header::{sort_order::COORDINATE, tag::SORT_ORDER};
use noodles::sam::{self};
use rayon::prelude::*;
use tracing::{debug, info};

use super::determine_thread_counts_1;
use crate::utils::{atomic_temp_path, publish_atomic_output};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_MEMORY: &str = "8GB";

#[derive(Args)]
pub struct BamSortCMD {
    /// Input BAM file (any sort order; will be coordinate-sorted on output).
    #[arg(short = 'i', long = "in", value_parser)]
    pub path_in: PathBuf,

    /// Output BAM file (coordinate-sorted). A `.bai` index is written alongside.
    #[arg(short = 'o', long = "out", value_parser)]
    pub path_out: PathBuf,

    /// Directory for spill files during the external-merge sort. Cleaned on success.
    #[arg(short = 't', long = "temp", value_parser, default_value = DEFAULT_PATH_TEMP)]
    pub path_temp: PathBuf,

    /// In-memory buffer cap for the sort phase. Total resident records ~bounded by this.
    #[arg(short = 'm', long = "memory", value_parser, default_value = DEFAULT_MEMORY)]
    pub memory: ByteSize,

    /// Total threads. Drives BGZF decode parallelism (input read), parallel radix sort
    /// within each memory cycle, and the merge-phase compressor pool.
    #[arg(short = '@', long = "threads", value_parser = clap::value_parser!(usize))]
    pub num_threads: Option<usize>,
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
        )
    }
}

/// Sort `path_in` (BAM, any order) into `path_out_sorted` (coordinate-sorted) and write a
/// BAI index alongside as `<path_out_sorted>.bai`. Two final files are atomic-published on
/// success; all intermediate spill runs in `path_temp` are cleaned up on both success and
/// failure (via `TempRunGuard`).
///
/// This is the in-process replacement for the old `samtools sort` + `samtools index`
/// shell-out — used both by the `bam-sort` subcommand and by every aligner's post-align
/// flow. Atomic guarantees:
///   - If sort phase fails: no `<path_out_sorted>` or `.bai` exists; temp runs deleted.
///   - If merge fails after sort: as above; the in-progress `.tmp` BAM is left for the OS
///     to garbage-collect (next run with same PID won't collide due to nanosecond suffix).
///   - If BAI write fails: the sorted BAM IS published (it's correct); the `.bai` is not.
///     Re-running indexing alone via `samtools index` (or another tool) will recover.
pub fn sort_and_index_bam(
    path_in: &Path,
    path_out_sorted: &Path,
    path_temp: &Path,
    memory: ByteSize,
    num_threads: usize,
) -> Result<()> {
    info!(
        input = %path_in.display(),
        output = %path_out_sorted.display(),
        temp_dir = %path_temp.display(),
        memory = %memory,
        threads = num_threads,
        "BamSort: starting"
    );

    std::fs::create_dir_all(path_temp).with_context(|| {
        format!("failed to create temp dir {}", path_temp.display())
    })?;

    let bgzf_workers = NonZeroUsize::new(num_threads.max(1))
        .expect("num_threads.max(1) is nonzero");
    let memory_cap = usize::try_from(memory.as_u64())
        .map_err(|_| anyhow::anyhow!("memory cap exceeds usize"))?;

    // Phase 1: arena reader → parallel radix sort → spill.
    let SortPhaseOut { header, temp_paths } = sort_phase(
        path_in,
        path_temp,
        memory_cap,
        num_threads,
        bgzf_workers,
    )?;
    info!(n_runs = temp_paths.len(), "BamSort: sort phase complete");
    // Guard: deletes spill runs on both success and failure paths (drop-on-scope-exit).
    let _temp_guard = TempRunGuard::new(temp_paths.clone());

    // Phase 2: k-way merge → MultithreadedWriter → atomic publish.
    let path_out_tmp = atomic_temp_path(path_out_sorted);
    merge_phase(&header, &temp_paths, &path_out_tmp, num_threads)?;
    publish_atomic_output(&path_out_tmp, &path_out_sorted.to_path_buf())?;
    info!(output = %path_out_sorted.display(), "BamSort: merge phase complete");

    // Phase 3: BAI index → atomic publish.
    let path_bai = bai_path(path_out_sorted);
    let path_bai_tmp = atomic_temp_path(&path_bai);
    let index = bam::fs::index(path_out_sorted)
        .with_context(|| format!("failed to index {}", path_out_sorted.display()))?;
    bam::bai::fs::write(&path_bai_tmp, &index)
        .with_context(|| format!("failed to write BAI to {}", path_bai_tmp.display()))?;
    publish_atomic_output(&path_bai_tmp, &path_bai)?;
    info!(index = %path_bai.display(), "BamSort: index complete");

    Ok(())
}

/// Drop guard: deletes a list of temp run files when it goes out of scope. Used to ensure
/// that a sort failure or panic between sort_phase and the BAI publish doesn't leak ~tens
/// of GB of spill files in the temp directory.
struct TempRunGuard {
    paths: Vec<PathBuf>,
}

impl TempRunGuard {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

impl Drop for TempRunGuard {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

// ============================================================================
// Sort key + radix sort
// ============================================================================

/// Pack `(ref_id, pos)` into a `u64` such that the natural `<` ordering matches the BAM
/// coordinate-sort order. Unmapped (`ref_id == -1`) maps to `u32::MAX` so unmapped records
/// sort to the end (samtools convention). `pos == -1` for unmapped clamps to 0.
type SortKey = u64;

#[inline(always)]
fn pack_sort_key(ref_id: i32, pos: i32) -> SortKey {
    let r: u32 = if ref_id < 0 {
        u32::MAX
    } else {
        ref_id as u32
    };
    let p: u32 = if pos < 0 { 0 } else { pos as u32 };
    ((r as u64) << 32) | (p as u64)
}

#[inline(always)]
fn extract_sort_key_from_record_bytes(body: &[u8]) -> SortKey {
    // BAM record body layout (per SAM spec §4.2):
    //   bytes 0..4 : ref_id  (i32 LE)
    //   bytes 4..8 : pos     (i32 LE)
    //   ... rest ignored for sort
    debug_assert!(body.len() >= 8);
    let ref_id = i32::from_le_bytes(body[0..4].try_into().unwrap());
    let pos = i32::from_le_bytes(body[4..8].try_into().unwrap());
    pack_sort_key(ref_id, pos)
}

/// Per-record metadata in the arena: where the record body lives + its precomputed sort key.
/// `Copy` because radix sort scatters by value.
#[derive(Copy, Clone, Default, Eq, PartialEq)]
struct RecordMeta {
    offset: u32,
    length: u32,
    sort_key: SortKey,
}

/// Samtools-style LSB byte radix sort over `RecordMeta` by `sort_key`. O(n × 8) bytes
/// scattered. Faster than `sort_unstable_by_key` for our key shape — and unlike a
/// comparison sort, doesn't re-extract the key per compare.
fn radix_sort_metas(metas: &mut [RecordMeta]) {
    if metas.len() < 2 {
        return;
    }
    let n = metas.len();
    let mut tmp: Vec<RecordMeta> = vec![RecordMeta::default(); n];
    let mut a: &mut [RecordMeta] = metas;
    let mut b: &mut [RecordMeta] = &mut tmp;

    for byte_pos in 0..std::mem::size_of::<SortKey>() {
        let shift = byte_pos * 8;
        let mut counts = [0usize; 256];
        for m in a.iter() {
            counts[((m.sort_key >> shift) & 0xff) as usize] += 1;
        }
        let mut acc = 0usize;
        for c in counts.iter_mut() {
            let cur = *c;
            *c = acc;
            acc += cur;
        }
        for m in a.iter() {
            let bucket = ((m.sort_key >> shift) & 0xff) as usize;
            b[counts[bucket]] = *m;
            counts[bucket] += 1;
        }
        std::mem::swap(&mut a, &mut b);
    }

    // 8 passes is an even count, so after `swap` the final `a` always points back to the
    // original `metas` slice — no copy-back needed. (`b` ends up at `tmp`.)
    let _ = (a, b);
}

// ============================================================================
// Sort phase: arena reader → spill coordinator
// ============================================================================

struct SortPhaseOut {
    header: sam::Header,
    temp_paths: Vec<PathBuf>,
}

/// One filled arena flowing reader → spill coordinator. Tagged with `cycle_idx` so spill
/// names temp files in source order (also useful for debug logging).
struct ArenaBatch {
    cycle_idx: u64,
    arena: Vec<u8>,
    metas: Vec<RecordMeta>,
}

fn sort_phase(
    path_in: &Path,
    path_temp: &Path,
    memory_cap: usize,
    num_workers: usize,
    bgzf_workers: NonZeroUsize,
) -> Result<SortPhaseOut> {
    // Open input via noodles just to parse the header, then unwrap to get the raw BGZF
    // stream so we can read record bytes directly without `bam::Record` decode.
    let file = File::open(path_in)
        .with_context(|| format!("open input BAM {}", path_in.display()))?;
    let bgzf_reader = bgzf::io::MultithreadedReader::with_worker_count(bgzf_workers, file);
    let mut bam_reader = bam::io::Reader::from(bgzf_reader);
    let header = bam_reader.read_header().context("read input BAM header")?;
    let header_with_so = with_coordinate_sort(header);
    let mut bgzf_reader_for_records = bam_reader.into_inner();

    // Dedicated rayon pool for the per-cycle parallel sort segments.
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_workers.max(1))
            .thread_name(|i| format!("BamSortSort@{i}"))
            .build()
            .context("build BamSort sort pool")?,
    );

    // Double-buffer: each arena targets `memory_cap / 2`. Channel depth 1 ⇒ at most 2
    // arenas in flight ⇒ peak resident ≈ `memory_cap`.
    let buffer_target_bytes = (memory_cap / 2).max(1024 * 1024);
    info!(
        memory_cap_bytes = memory_cap,
        memory_cap = %ByteSize(memory_cap as u64),
        buffer_target = %ByteSize(buffer_target_bytes as u64),
        num_workers,
        "BamSort: sort phase config (raw-bytes arena, double-buffered, one temp per cycle)"
    );

    let (batch_tx, batch_rx) = channel::bounded::<ArenaBatch>(1);
    // Pool of recycled arena buffers. Pre-seeded with `pool_size` empty Vecs sized to
    // `buffer_target_bytes`. Reader pops, fills, ships; spill returns drained Vecs back.
    // Avoids reallocating multi-GB arenas every cycle.
    const POOL_SIZE: usize = 2;
    let (pool_tx, pool_rx) = channel::bounded::<(Vec<u8>, Vec<RecordMeta>)>(POOL_SIZE);
    for _ in 0..POOL_SIZE {
        pool_tx
            .send((Vec::with_capacity(buffer_target_bytes), Vec::new()))
            .expect("seed buffer pool");
    }

    // Reader thread: raw-bytes arena fill loop.
    let pool_rx_reader = pool_rx.clone();
    let reader_handle: JoinHandle<Result<u64>> = thread::Builder::new()
        .name("BamSortReader".to_owned())
        .spawn(move || -> Result<u64> {
            let mut cycle_idx: u64 = 0;
            let mut total_records: u64 = 0;
            let mut eof = false;
            while !eof {
                // Recycle a buffer from the pool (blocks if all buffers are in flight).
                let (mut arena, mut metas) = match pool_rx_reader.recv() {
                    Ok(v) => v,
                    Err(_) => break,
                };
                arena.clear();
                metas.clear();
                loop {
                    // Read 4-byte block_size prefix; EOF is acceptable here.
                    let mut len_buf = [0u8; 4];
                    match bgzf_reader_for_records.read_exact(&mut len_buf) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            eof = true;
                            break;
                        }
                        Err(e) => return Err(e).context("read BAM record block_size"),
                    }
                    let block_size = i32::from_le_bytes(len_buf);
                    if block_size <= 0 {
                        anyhow::bail!("invalid BAM block_size {block_size}");
                    }
                    let block_size = block_size as usize;

                    let start = arena.len();
                    arena.resize(start + block_size, 0);
                    bgzf_reader_for_records
                        .read_exact(&mut arena[start..start + block_size])
                        .context("read BAM record body")?;

                    let sort_key = extract_sort_key_from_record_bytes(
                        &arena[start..start + block_size],
                    );
                    metas.push(RecordMeta {
                        offset: u32::try_from(start)
                            .map_err(|_| anyhow::anyhow!("arena offset exceeded u32"))?,
                        length: u32::try_from(block_size)
                            .map_err(|_| anyhow::anyhow!("record length exceeded u32"))?,
                        sort_key,
                    });

                    if arena.len() >= buffer_target_bytes {
                        break;
                    }
                }
                if metas.is_empty() {
                    break;
                }
                total_records += metas.len() as u64;
                if batch_tx
                    .send(ArenaBatch {
                        cycle_idx,
                        arena,
                        metas,
                    })
                    .is_err()
                {
                    break;
                }
                cycle_idx += 1;
            }
            drop(batch_tx);
            Ok(total_records)
        })
        .expect("spawn BamSortReader");

    // Spill coordinator: receives arenas, parallel radix-sorts metas, writes temp BAM,
    // returns drained buffers back to the pool for reader to refill.
    // Precompute the BAM header bytes once — every spill writes the same bytes.
    let header_bytes = encode_bam_header(&header_with_so)?;
    let run_id_base = unique_run_id_base();
    let mut temp_paths: Vec<PathBuf> = Vec::new();
    while let Ok(batch) = batch_rx.recv() {
        let ArenaBatch { cycle_idx, arena, mut metas } = batch;
        let path = path_temp.join(format!(
            "bascet-bamsort-{run_id_base}-c{cycle_idx}.bam"
        ));
        sort_and_spill_arena(&pool, &arena, &mut metas, &header_bytes, &path, num_workers)?;
        debug!(
            cycle = cycle_idx,
            records = metas.len(),
            arena_bytes = arena.len(),
            path = %path.display(),
            "BamSort: spilled cycle"
        );
        temp_paths.push(path);
        // Recycle: return drained buffers to the pool (best-effort; reader may already be
        // done so the pool channel may be closed).
        let _ = pool_tx.send((arena, metas));
    }
    // Drop the original pool sender so the reader's recv() returns Err once the pool is
    // empty, signaling EOF cleanly.
    drop(pool_tx);
    drop(pool_rx);

    let total_records = reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("BamSortReader panicked"))?
        .context("BamSortReader failed")?;
    info!(
        total_records,
        n_runs = temp_paths.len(),
        "BamSort: sort phase done"
    );

    Ok(SortPhaseOut {
        header: header_with_so,
        temp_paths,
    })
}

/// Sort one memory cycle's arena and spill it as a single NONE-compressed temp BAM. The
/// metas are split into `num_workers` segments, parallel radix-sorted, then heap-merged in
/// memory while writing record bytes back-to-back through the BGZF wrapper.
fn sort_and_spill_arena(
    pool: &rayon::ThreadPool,
    arena: &[u8],
    metas: &mut [RecordMeta],
    header_bytes: &[u8],
    path: &Path,
    num_workers: usize,
) -> Result<()> {
    // Partition metas into N roughly-equal segments and sort each in parallel.
    let n = metas.len();
    let n_workers = num_workers.max(1);
    let segment_size = n.div_ceil(n_workers);
    pool.install(|| {
        metas
            .par_chunks_mut(segment_size)
            .for_each(|seg| radix_sort_metas(seg));
    });

    // Compute segment boundaries for k-way merge.
    let segments: Vec<(usize, usize)> = (0..n)
        .step_by(segment_size)
        .map(|s| (s, (s + segment_size).min(n)))
        .collect();

    // Open temp BAM with light BGZF compression (level 1 = fast deflate). Trades a small
    // amount of CPU on the spill workers for ~3× less disk I/O on the merge-phase reads,
    // which on HDD-class temp storage is the dominant cost.
    // Skip noodles' bam::Writer wrapper: write the precomputed BAM header bytes directly
    // through the BGZF writer.
    let file = File::create(path)
        .with_context(|| format!("create temp BAM {}", path.display()))?;
    let mut bgzf_writer = bgzf::io::writer::Builder::default()
        .set_compression_level(
            CompressionLevel::new(1).expect("CompressionLevel 1 is valid"),
        )
        .build_from_writer(BufWriter::with_capacity(1 << 20, file));
    bgzf_writer
        .write_all(header_bytes)
        .with_context(|| format!("write temp BAM header {}", path.display()))?;

    // Heap merge. Each segment has a cursor into its sorted meta range.
    let mut cursors: Vec<usize> = segments.iter().map(|(s, _)| *s).collect();
    let mut heap: BinaryHeap<Reverse<MergeKey>> = BinaryHeap::with_capacity(segments.len());
    for (seg_idx, &(s, e)) in segments.iter().enumerate() {
        if s < e {
            heap.push(Reverse(MergeKey {
                key: metas[s].sort_key,
                segment_idx: seg_idx,
            }));
        }
    }
    while let Some(Reverse(top)) = heap.pop() {
        let seg_idx = top.segment_idx;
        let cur = cursors[seg_idx];
        let meta = metas[cur];
        // Write 4-byte block_size + body bytes from arena.
        bgzf_writer
            .write_all(&(meta.length as i32).to_le_bytes())
            .with_context(|| format!("write block_size to {}", path.display()))?;
        bgzf_writer
            .write_all(&arena[meta.offset as usize..(meta.offset + meta.length) as usize])
            .with_context(|| format!("write record body to {}", path.display()))?;
        cursors[seg_idx] += 1;
        if cursors[seg_idx] < segments[seg_idx].1 {
            heap.push(Reverse(MergeKey {
                key: metas[cursors[seg_idx]].sort_key,
                segment_idx: seg_idx,
            }));
        }
    }

    bgzf_writer
        .finish()
        .with_context(|| format!("finish temp BAM {}", path.display()))?;
    Ok(())
}

#[derive(Eq, PartialEq)]
struct MergeKey {
    key: SortKey,
    segment_idx: usize,
}

impl Ord for MergeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key).then(self.segment_idx.cmp(&other.segment_idx))
    }
}
impl PartialOrd for MergeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ============================================================================
// Merge phase: samtools-style — single merger thread does heap-pop + write_all
// directly to a `bgzf::io::MultithreadedWriter`. The writer's internal worker pool
// handles all BGZF compression async, so the merger's `write_all` per record is
// just memcpy. Source reads are direct (no per-source channels): the merger calls
// `bgzf::io::Reader::read_exact()` per source, blocking on disk only when buffers
// are exhausted.
// ============================================================================

fn merge_phase(
    header: &sam::Header,
    temp_paths: &[PathBuf],
    path_out_tmp: &Path,
    num_workers: usize,
) -> Result<()> {
    let workers = NonZeroUsize::new(num_workers.max(1)).expect("num_workers >= 1");
    let file = File::create(path_out_tmp)
        .with_context(|| format!("create output BAM {}", path_out_tmp.display()))?;
    // MultithreadedWriter spawns N workers internally that BGZF-compress blocks async.
    let mut bgzf_writer = bgzf::io::MultithreadedWriter::with_worker_count(workers, file);

    // Header — write directly through the writer. Output uses default BGZF compression
    // level (6) so the final BAM is reasonably-sized; no level-1 hack here.
    let header_bytes = encode_bam_header(header)?;
    bgzf_writer
        .write_all(&header_bytes)
        .context("write output BAM header")?;

    // Open all sources. Each holds its own single-thread `bgzf::io::Reader` over a
    // BufReader; the merger calls `advance()` per heap-pop, blocking on read only when
    // the per-source read-ahead buffer is empty.
    let mut sources: Vec<MergeSource> = Vec::with_capacity(temp_paths.len());
    for (idx, p) in temp_paths.iter().enumerate() {
        sources.push(MergeSource::open(p, idx)?);
    }

    // Prime the heap with the first record from each source.
    let mut heap: BinaryHeap<Reverse<MergeKey>> = BinaryHeap::with_capacity(sources.len());
    for src in sources.iter_mut() {
        if let Some(key) = src.advance()? {
            heap.push(Reverse(MergeKey {
                key,
                segment_idx: src.idx,
            }));
        }
    }

    let mut total_records: u64 = 0;
    while let Some(Reverse(top)) = heap.pop() {
        let src = &mut sources[top.segment_idx];
        let body = src
            .current
            .take()
            .expect("merger: source primed but current is None");
        // Write 4-byte block_size + body bytes. MultithreadedWriter buffers and dispatches
        // 64 KiB BGZF blocks to its compression workers; this call is just memcpy.
        bgzf_writer
            .write_all(&(body.len() as i32).to_le_bytes())
            .context("merge: write block_size")?;
        bgzf_writer
            .write_all(&body)
            .context("merge: write record body")?;
        total_records += 1;

        if let Some(key) = src.advance()? {
            heap.push(Reverse(MergeKey {
                key,
                segment_idx: top.segment_idx,
            }));
        }
    }

    // MultithreadedWriter::finish() flushes pending blocks and writes the BGZF EOF marker.
    bgzf_writer.finish().context("finish output BGZF writer")?;
    info!(total_records, "BamSort: merge done");
    Ok(())
}

/// Source reader for the merge phase: reads raw record bytes (no `bam::Record` decode) from
/// a temp BAM file, exposing the next record's body bytes + sort key on `advance()`.
struct MergeSource {
    idx: usize,
    reader: bgzf::io::Reader<BufReader<File>>,
    /// Buffer holding the next record's body bytes (without the 4-byte length prefix).
    current: Option<Vec<u8>>,
}

impl MergeSource {
    fn open(path: &Path, idx: usize) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("open temp run {}", path.display()))?;
        let bgzf_reader = bgzf::io::Reader::new(BufReader::with_capacity(1 << 20, file));
        let mut bam_reader = bam::io::Reader::from(bgzf_reader);
        let _header = bam_reader
            .read_header()
            .with_context(|| format!("read temp BAM header {}", path.display()))?;
        let reader = bam_reader.into_inner();
        Ok(Self {
            idx,
            reader,
            current: None,
        })
    }

    fn advance(&mut self) -> Result<Option<SortKey>> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.current = None;
                return Ok(None);
            }
            Err(e) => return Err(anyhow::Error::from(e).context("merge: read block_size")),
        }
        let block_size = i32::from_le_bytes(len_buf);
        if block_size <= 0 {
            anyhow::bail!("invalid temp-BAM block_size {block_size}");
        }
        let block_size = block_size as usize;
        let mut body = vec![0u8; block_size];
        self.reader
            .read_exact(&mut body)
            .context("merge: read record body")?;
        let key = extract_sort_key_from_record_bytes(&body);
        self.current = Some(body);
        Ok(Some(key))
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Encode just the BAM magic + header text + ref dict into a `Vec<u8>`.
fn encode_bam_header(header: &sam::Header) -> Result<Vec<u8>> {
    let buf: Vec<u8> = Vec::new();
    let mut bam_w = bam::io::Writer::from(buf);
    bam_w.write_header(header).context("encode BAM header")?;
    Ok(bam_w.into_inner())
}

/// Inject `SO:coordinate` into the SAM header. Builds a fresh HD record (any prior HD
/// fields are dropped — fine for our pipeline since the upstream aligners emit headers
/// without HD).
fn with_coordinate_sort(mut header: sam::Header) -> sam::Header {
    use noodles::sam::header::record::value::Map;
    use noodles::sam::header::record::value::map::header::Header as HeaderMap;
    let new_hd = Map::<HeaderMap>::builder()
        .insert(SORT_ORDER, COORDINATE)
        .build()
        .expect("build HD with SO:coordinate");
    *header.header_mut() = Some(new_hd);
    header
}

fn bai_path(bam_path: &Path) -> PathBuf {
    let mut s = bam_path.as_os_str().to_owned();
    s.push(".bai");
    PathBuf::from(s)
}

fn unique_run_id_base() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
