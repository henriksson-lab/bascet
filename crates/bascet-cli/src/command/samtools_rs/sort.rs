// Sort orchestration: comparators, in-memory chunks, external sort, k-way merge.
// Translated from samtools/bam_sort.c.

use super::bam::{Header, Record};
use super::bgzf;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender, bounded, unbounded};
use tracing::{debug, info};

const SPILL_PROGRESS_STEP_RECORDS: u64 = 10_000_000;

fn fmt_count(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

#[derive(Clone, Copy)]
pub enum Order {
    Coordinate,
}

#[derive(Clone, Copy)]
pub enum ReferenceOrder {
    Lexicographic,
    Preserve,
}

/// Coordinate comparator matching `bam1_cmp_core` for `Coordinate` in
/// samtools/bam_sort.c:2023. Key is (tid sign-extended to u64, pos+1, is_rev).
/// Sign-extending tid causes unmapped reads (tid = -1) to sort last.
pub fn coord_cmp(a: &Record, b: &Record) -> Ordering {
    let a_tid = (a.ref_id() as i64) as u64;
    let b_tid = (b.ref_id() as i64) as u64;
    if a_tid != b_tid {
        return a_tid.cmp(&b_tid);
    }
    let a_pos = a.pos() as i64 + 1;
    let b_pos = b.pos() as i64 + 1;
    if a_pos != b_pos {
        return a_pos.cmp(&b_pos);
    }
    let a_rev = (a.flag() & 0x10) != 0;
    let b_rev = (b.flag() & 0x10) != 0;
    a_rev.cmp(&b_rev)
}

pub struct SortOptions<'a> {
    pub order: Order,
    pub reference_order: ReferenceOrder,
    pub level: u8,
    /// Command line text written into the @PG CL field. None = omit CL.
    pub arg_list: Option<&'a str>,
    pub no_pg: bool,
    /// Memory budget per sort batch in bytes. Once exceeded, the in-memory
    /// chunk is written to a temp BAM and a new chunk is started. The CLI
    /// computes this as `total_mem / threads` so the user-facing `-m` flag
    /// is a *total* memory budget across all in-flight chunks.
    pub max_mem: usize,
    /// Path prefix for temporary chunk files: `<prefix>.NNNN.bam`.
    pub tmp_prefix: PathBuf,
    /// Number of compression worker threads for the **final** output BGZF
    /// stream. 1 = single-threaded (matches samtools' `-@ 0`/`-@ 1`).
    /// Temp file writes stay single-threaded; they use level 1 and are
    /// almost never the bottleneck.
    pub threads: usize,
    /// If `Some((path, format))`, write a BAI/CSI index for the sorted
    /// output to `path`. Requires the output to be a real file (we need
    /// block compressed offsets, which is incompatible with truly
    /// streaming stdout indexing in samtools-style "concurrent" mode).
    /// Set by CLI `--write-index` (BAI) or `--write-index-csi` (CSI).
    pub write_index: Option<(PathBuf, IndexFormat)>,
}

#[derive(Clone, Copy)]
pub enum IndexFormat {
    Bai,
    Csi,
}

const TMP_LEVEL: u8 = 1;
const PER_RECORD_OVERHEAD: usize = std::mem::size_of::<Record>();

fn record_mem(r: &Record) -> usize {
    PER_RECORD_OVERHEAD + r.data.capacity()
}

/// Read a whole BAM, sort in memory, write a BAM. Used when no spilling is
/// expected (small input or generous `max_mem`).
pub fn sort_in_memory<R: Read, W: Write>(
    input: R,
    output: W,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    let staged = collect_in_memory(bgzf::Reader::new(input), opts)?;
    write_with_single_threaded_writer(output, staged, opts)
}

/// Same as `sort_in_memory` but uses `ParallelReader` on input and
/// `ParallelWriter` on output, both with `opts.threads` workers.
pub fn sort_in_memory_parallel<R: Read + Send + 'static, W: Write + Send + 'static>(
    input: R,
    output: W,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    let staged = collect_in_memory(bgzf::ParallelReader::new(input, opts.threads), opts)?;
    write_with_parallel_writer(output, staged, opts)
}

fn collect_in_memory<R: Read>(mut bgz_in: R, opts: &SortOptions<'_>) -> io::Result<StagedSort> {
    let original_header = Header::read(&mut bgz_in)?;
    let (mut header, ref_id_map) = prepare_output_header(&original_header, opts.reference_order)?;
    let mut records = Vec::new();
    while let Some(mut r) = Record::read(&mut bgz_in)? {
        remap_record_refs(&mut r, ref_id_map.as_deref())?;
        records.push(r);
    }
    sort_records(&mut records, opts.order);
    update_header(&mut header, opts);
    Ok(StagedSort {
        out_header: header,
        chunks: ChunkSource::InMemory(records),
    })
}

fn sort_records(records: &mut [Record], order: Order) {
    // sort_by is stable. samtools' ksort is unstable introsort, so true
    // ties (same tid, pos, strand) may order differently from samtools.
    // For typical inputs ksort happens to preserve input order on small
    // tied groups, so this is usually byte-equivalent. Track divergence
    // via the diff harness; replace with a ksort port if it bites.
    match order {
        Order::Coordinate => records.sort_by(coord_cmp),
    }
}

/// Streaming sort: read records, spill sorted chunks to temp BAMs whenever
/// the in-memory buffer exceeds `opts.max_mem`, then k-way merge to output.
/// Falls back to a direct in-memory write when nothing spilled.
pub fn sort_streaming<R: Read, W: Write>(
    input: R,
    output: W,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    let staged = collect_and_spill(input, opts)?;
    write_with_single_threaded_writer(output, staged, opts)
}

/// Same as `sort_streaming` but uses `ParallelReader` on input and
/// `ParallelWriter` on output, both with `opts.threads` workers.
pub fn sort_streaming_parallel<R: Read + Send + 'static, W: Write + Send + 'static>(
    input: R,
    output: W,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    info!("BamSort: phase 1/2 — reading input and spilling sorted chunks");
    let phase1_start = Instant::now();
    let staged = collect_and_spill_parallel(input, opts)?;
    let phase1_elapsed = phase1_start.elapsed();
    match &staged.chunks {
        ChunkSource::InMemory(records) => info!(
            records = records.len(),
            elapsed_secs = phase1_elapsed.as_secs(),
            "BamSort: phase 1 done — sort fits in memory, no spill"
        ),
        ChunkSource::Files(paths) => info!(
            chunks = paths.len(),
            elapsed_secs = phase1_elapsed.as_secs(),
            "BamSort: phase 1 done — spilled chunks; phase 2/2 starting (k-way merge + write)"
        ),
    }
    let phase2_start = Instant::now();
    write_with_parallel_writer(output, staged, opts)?;
    info!(
        elapsed_secs = phase2_start.elapsed().as_secs(),
        "BamSort: phase 2 done — merge + write complete"
    );
    Ok(())
}

/// Source of sorted records ready for the final write. Either everything
/// fit in memory, or it spilled to one-or-more sorted temp BAMs that need
/// k-way merging on the way out.
struct StagedSort {
    out_header: Header,
    chunks: ChunkSource,
}

enum ChunkSource {
    InMemory(Vec<Record>),
    Files(Vec<PathBuf>),
}

fn collect_and_spill<R: Read>(input: R, opts: &SortOptions<'_>) -> io::Result<StagedSort> {
    collect_and_spill_with_reader(bgzf::Reader::new(input), opts)
}

/// Parallel collect+spill: main thread reads records into the next buffer;
/// when the buffer hits `max_mem` it's handed to a worker pool that
/// sorts + writes to a temp BAM. The job channel is rendezvous (bound 0)
/// so peak memory ≈ (threads + 1) × max_mem rather than unbounded.
///
/// Mirrors samtools' design where each chunk is sorted on its own thread
/// while the reader keeps streaming.
fn collect_and_spill_parallel<R: Read + Send + 'static>(
    input: R,
    opts: &SortOptions<'_>,
) -> io::Result<StagedSort> {
    let mut bgz_in = bgzf::ParallelReader::new(input, opts.threads);
    let original_header = Header::read(&mut bgz_in)?;
    let (mut out_header, ref_id_map) =
        prepare_output_header(&original_header, opts.reference_order)?;

    let header_arc = Arc::new(out_header.clone());
    let order = opts.order;
    let prefix = opts.tmp_prefix.clone();

    // Rendezvous job channel (bound 0): main blocks until a worker is ready,
    // capping in-flight buffers to `threads` (one per worker).
    // crossbeam-channel is MPMC and lock-free — no Mutex<Receiver> contention.
    let (job_tx, job_rx) = bounded::<SpillJob>(0);
    let (result_tx, result_rx) = unbounded::<SpillResult>();
    // Recycle channel: spill workers ship their freed `Vec<u8>`s back as a
    // batch after writing each chunk, so the reader can reuse them instead
    // of allocating fresh per record. Mirrors samtools' `bam1_t.data` reuse
    // semantics, but at the chunk-batch level. Bounded(1) so peak extra
    // memory ≈ one chunk worth (~max_mem); workers `try_send` and drop the
    // batch on full to avoid any deadlock.
    let (recycle_tx, recycle_rx) = bounded::<Vec<Vec<u8>>>(1);

    let n_workers = opts.threads.max(1);
    let mut workers = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = job_rx.clone();
        let tx = result_tx.clone();
        let recycle = recycle_tx.clone();
        let h = Arc::clone(&header_arc);
        let pfx = prefix.clone();
        workers.push(std::thread::spawn(move || {
            spill_worker(rx, tx, recycle, h, order, pfx);
        }));
    }
    drop(result_tx);
    drop(job_rx);
    drop(recycle_tx);

    let mut buf: Vec<Record> = Vec::new();
    let mut buf_bytes: usize = 0;
    let mut next_chunk_idx: usize = 0;
    let mut spill_started = false;
    let mut buf_pool: Vec<Vec<u8>> = Vec::new();
    let mut total_records: u64 = 0;
    let mut next_spill_progress_at = SPILL_PROGRESS_STEP_RECORDS;

    let read_result: io::Result<()> = (|| {
        loop {
            // Drain any returned buffers into the local pool. Non-blocking,
            // so the reader is never paced by recycle traffic.
            while let Ok(batch) = recycle_rx.try_recv() {
                buf_pool.extend(batch);
            }
            let scratch = buf_pool.pop().unwrap_or_default();
            let Some(mut r) = Record::read_into(&mut bgz_in, scratch)? else {
                break;
            };
            remap_record_refs(&mut r, ref_id_map.as_deref())?;
            buf_bytes += record_mem(&r);
            buf.push(r);
            total_records += 1;
            if buf_bytes >= opts.max_mem {
                spill_started = true;
                let chunk_records = buf.len();
                let chunk_bytes = buf_bytes;
                let job = SpillJob {
                    records: std::mem::take(&mut buf),
                    chunk_idx: next_chunk_idx,
                };
                debug!(
                    chunk_idx = next_chunk_idx,
                    records = %fmt_count(chunk_records as u64),
                    bytes = %fmt_count(chunk_bytes as u64),
                    total = %fmt_count(total_records),
                    "BamSort: dispatched spill chunk"
                );
                if total_records >= next_spill_progress_at {
                    info!(
                        reads = %fmt_count(total_records),
                        chunks = %fmt_count((next_chunk_idx + 1) as u64),
                        "BamSort: spilled"
                    );
                    while next_spill_progress_at <= total_records {
                        next_spill_progress_at =
                            next_spill_progress_at.saturating_add(SPILL_PROGRESS_STEP_RECORDS);
                    }
                }
                next_chunk_idx += 1;
                buf_bytes = 0;
                if job_tx.send(job).is_err() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "spill worker channel closed unexpectedly",
                    ));
                }
            }
        }
        Ok(())
    })();
    info!(
        reads = %fmt_count(total_records),
        chunks = %fmt_count(next_chunk_idx as u64),
        "BamSort: read input"
    );

    // If we ever spilled, also spill the final partial buffer so the merge
    // path is uniform.
    if read_result.is_ok() && spill_started && !buf.is_empty() {
        let job = SpillJob {
            records: std::mem::take(&mut buf),
            chunk_idx: next_chunk_idx,
        };
        let _ = job_tx.send(job);
    }
    drop(job_tx);

    // Workers will drain remaining jobs, then exit when the channel closes.
    for w in workers {
        let _ = w.join();
    }

    let mut chunk_results: Vec<SpillResult> = result_rx.iter().collect();
    let mut first_err: Option<io::Error> = read_result.err();
    for r in &mut chunk_results {
        if first_err.is_none() {
            if let Some(e) = r.error.take() {
                first_err = Some(e);
            }
        }
    }
    if let Some(e) = first_err {
        // Best-effort cleanup of any temp files that did get written.
        for r in &chunk_results {
            let _ = std::fs::remove_file(&r.path);
        }
        return Err(e);
    }

    update_header(&mut out_header, opts);

    if !spill_started {
        // Nothing spilled — single in-memory chunk.
        sort_records(&mut buf, opts.order);
        return Ok(StagedSort {
            out_header,
            chunks: ChunkSource::InMemory(buf),
        });
    }

    // Order chunk paths by chunk_idx so file_idx in the merge heap matches
    // the order chunks were created — this preserves samtools' tie-break.
    chunk_results.sort_by_key(|r| r.chunk_idx);
    let chunk_paths: Vec<PathBuf> = chunk_results.into_iter().map(|r| r.path).collect();

    Ok(StagedSort {
        out_header,
        chunks: ChunkSource::Files(chunk_paths),
    })
}

struct SpillJob {
    records: Vec<Record>,
    chunk_idx: usize,
}

struct SpillResult {
    chunk_idx: usize,
    path: PathBuf,
    error: Option<io::Error>,
}

fn spill_worker(
    rx: Receiver<SpillJob>,
    tx: Sender<SpillResult>,
    recycle: Sender<Vec<Vec<u8>>>,
    header: Arc<Header>,
    order: Order,
    tmp_prefix: PathBuf,
) {
    loop {
        let Ok(SpillJob {
            mut records,
            chunk_idx,
        }) = rx.recv()
        else {
            return;
        };
        let n_records = records.len();
        let spill_start = Instant::now();
        sort_records(&mut records, order);
        let path = chunk_path(&tmp_prefix, chunk_idx);
        let error = match write_chunk_file(&path, &header, &records) {
            Ok(()) => {
                debug!(
                    chunk_idx,
                    records = %fmt_count(n_records as u64),
                    elapsed_secs = spill_start.elapsed().as_secs(),
                    "BamSort: spill chunk written"
                );
                None
            }
            Err(e) => Some(e),
        };
        // Ship the freed `Vec<u8>`s back to the reader for reuse. Best-effort:
        // try_send drops the batch if the channel is full, in which case the
        // allocator simply frees them on Drop. No deadlock risk.
        let recycled: Vec<Vec<u8>> = records
            .into_iter()
            .map(|r| {
                let mut d = r.data;
                d.clear();
                d
            })
            .collect();
        let _ = recycle.try_send(recycled);
        if tx
            .send(SpillResult {
                chunk_idx,
                path,
                error,
            })
            .is_err()
        {
            return;
        }
    }
}

fn chunk_path(tmp_prefix: &Path, idx: usize) -> PathBuf {
    tmp_prefix.with_file_name(format!(
        "{}.{:04}.bam",
        tmp_prefix
            .file_name()
            .map(|s| s.to_string_lossy())
            .unwrap_or_else(|| "samtools-rs.tmp".into()),
        idx
    ))
}

fn write_chunk_file(path: &Path, header: &Header, records: &[Record]) -> io::Result<()> {
    let f = BufWriter::new(File::create(path)?);
    let mut bgz = bgzf::Writer::new(f, TMP_LEVEL);
    header.write(&mut bgz)?;
    for r in records {
        r.write(&mut bgz)?;
    }
    bgz.finish()?;
    Ok(())
}

fn collect_and_spill_with_reader<R: Read>(
    mut bgz_in: R,
    opts: &SortOptions<'_>,
) -> io::Result<StagedSort> {
    let original_header = Header::read(&mut bgz_in)?;
    let (mut out_header, ref_id_map) =
        prepare_output_header(&original_header, opts.reference_order)?;

    let mut buf: Vec<Record> = Vec::new();
    let mut buf_bytes: usize = 0;
    let mut chunk_paths: Vec<PathBuf> = Vec::new();

    while let Some(mut r) = Record::read(&mut bgz_in)? {
        remap_record_refs(&mut r, ref_id_map.as_deref())?;
        buf_bytes += record_mem(&r);
        buf.push(r);
        if buf_bytes >= opts.max_mem {
            spill_chunk(
                &mut buf,
                &out_header,
                opts.order,
                &opts.tmp_prefix,
                &mut chunk_paths,
            )?;
            buf_bytes = 0;
        }
    }

    update_header(&mut out_header, opts);

    let chunks = if chunk_paths.is_empty() {
        sort_records(&mut buf, opts.order);
        ChunkSource::InMemory(buf)
    } else {
        if !buf.is_empty() {
            spill_chunk(
                &mut buf,
                &out_header,
                opts.order,
                &opts.tmp_prefix,
                &mut chunk_paths,
            )?;
        }
        drop(buf);
        ChunkSource::Files(chunk_paths)
    };

    Ok(StagedSort { out_header, chunks })
}

fn write_with_single_threaded_writer<W: Write>(
    output: W,
    staged: StagedSort,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    let StagedSort { out_header, chunks } = staged;
    let mut bgz = bgzf::Writer::new(output, opts.level);
    out_header.write(&mut bgz)?;
    let mut builder = opts
        .write_index
        .as_ref()
        .map(|_| super::index::BaiBuilder::new(&out_header));
    let header_uoffset = out_header.serialized_len() as u64;
    drain_chunks(&mut bgz, chunks, builder.as_mut(), header_uoffset)?;
    let (_inner, block_offsets) = bgz.finish_with_offsets()?;
    if let (Some(b), Some((path, fmt))) = (builder, opts.write_index.as_ref()) {
        write_index_file(path, *fmt, &b, &block_offsets)?;
    }
    Ok(())
}

fn write_with_parallel_writer<W: Write + Send + 'static>(
    output: W,
    staged: StagedSort,
    opts: &SortOptions<'_>,
) -> io::Result<()> {
    let StagedSort { out_header, chunks } = staged;
    let mut bgz = bgzf::ParallelWriter::new(output, opts.level, opts.threads);
    out_header.write(&mut bgz)?;
    let mut builder = opts
        .write_index
        .as_ref()
        .map(|_| super::index::BaiBuilder::new(&out_header));
    let header_uoffset = out_header.serialized_len() as u64;
    drain_chunks(&mut bgz, chunks, builder.as_mut(), header_uoffset)?;
    let block_offsets = bgz.finish_with_offsets()?;
    if let (Some(b), Some((path, fmt))) = (builder, opts.write_index.as_ref()) {
        write_index_file(path, *fmt, &b, &block_offsets)?;
    }
    Ok(())
}

fn write_index_file(
    path: &Path,
    fmt: IndexFormat,
    builder: &super::index::BaiBuilder,
    block_offsets: &[u64],
) -> io::Result<()> {
    let mut f = BufWriter::new(File::create(path)?);
    match fmt {
        IndexFormat::Bai => builder.write(&mut f, block_offsets)?,
        IndexFormat::Csi => builder.write_csi(&mut f, block_offsets)?,
    }
    f.flush()
}

fn drain_chunks<W: Write>(
    out: &mut W,
    chunks: ChunkSource,
    mut builder: Option<&mut super::index::BaiBuilder>,
    starting_uoffset: u64,
) -> io::Result<u64> {
    let mut uoffset = starting_uoffset;
    match chunks {
        ChunkSource::InMemory(records) => {
            for r in &records {
                let start = uoffset;
                r.write(out)?;
                uoffset += 4 + r.data.len() as u64;
                if let Some(b) = builder.as_deref_mut() {
                    b.add_record(r, start, uoffset);
                }
            }
            Ok(uoffset)
        }
        ChunkSource::Files(paths) => {
            let result = merge_chunks_into(out, &paths, builder, uoffset);
            for p in &paths {
                let _ = std::fs::remove_file(p);
            }
            result
        }
    }
}

fn spill_chunk(
    buf: &mut Vec<Record>,
    header: &Header,
    order: Order,
    tmp_prefix: &std::path::Path,
    chunk_paths: &mut Vec<PathBuf>,
) -> io::Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    sort_records(buf, order);
    let path = chunk_path(tmp_prefix, chunk_paths.len());
    write_chunk_file(&path, header, buf)?;
    chunk_paths.push(path);
    buf.clear();
    Ok(())
}

/// A chunk reader used during k-way merge. Wraps a `ParallelReader` with a
/// single inflate worker so each open chunk has a background inflate
/// pipeline — analogous to htslib's `bgzf_mt` read-ahead. The merge thread
/// pops the heap and pulls the next record without blocking on inline
/// decompression: the next block is usually already inflated and waiting.
///
/// `scratch` is the recycled `Vec<u8>` that mirrors htslib's `bam1_t.data`
/// realloc-and-keep pattern. After the merge writes a record, its Vec is
/// returned here via `return_buf` and reused for the next read — so this
/// chunk reader allocates one `Vec<u8>` for its lifetime, not one per
/// record. Across all chunks during merge that's ~100M allocations
/// eliminated for a 9 GB BAM workload.
struct ChunkReader {
    bgz: bgzf::ParallelReader,
    file_idx: usize,
    next_record_idx: u64,
    scratch: Vec<u8>,
}

impl ChunkReader {
    fn open(path: &std::path::Path, file_idx: usize) -> io::Result<Self> {
        let f = BufReader::new(File::open(path)?);
        // 1 inflate worker per chunk: with N chunks open during merge, we
        // get N decompression streams running concurrently up to the
        // physical core count. Total threads = 2 * N (1 reader + 1 worker
        // per chunk); for typical chunk counts (10s–100s) this is fine.
        let mut bgz = bgzf::ParallelReader::new(f, 1);
        // Skip the header — we already have the merged output header.
        let _ = Header::read(&mut bgz)?;
        Ok(Self {
            bgz,
            file_idx,
            next_record_idx: 0,
            scratch: Vec::new(),
        })
    }

    fn read(&mut self) -> io::Result<Option<HeapEntry>> {
        let scratch = std::mem::take(&mut self.scratch);
        match Record::read_into(&mut self.bgz, scratch)? {
            None => Ok(None),
            Some(record) => {
                let idx = self.next_record_idx;
                self.next_record_idx += 1;
                Ok(Some(HeapEntry {
                    record,
                    file_idx: self.file_idx,
                    record_idx: idx,
                }))
            }
        }
    }

    /// Return a previously-handed-out `Vec<u8>` for reuse on the next read.
    /// Called by the merge loop after writing a record.
    fn return_buf(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.scratch = buf;
    }
}

struct HeapEntry {
    record: Record,
    /// Matches samtools' `heap1_t.i` — input file index, used as primary
    /// tie-break after the sort key.
    file_idx: usize,
    /// Matches samtools' `heap1_t.idx` — the record's position within its
    /// source file, used as the secondary tie-break.
    record_idx: u64,
}

impl HeapEntry {
    fn key_cmp(&self, other: &Self) -> Ordering {
        match coord_cmp(&self.record, &other.record) {
            Ordering::Equal => match self.file_idx.cmp(&other.file_idx) {
                Ordering::Equal => self.record_idx.cmp(&other.record_idx),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl Eq for HeapEntry {}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key_cmp(other) == Ordering::Equal
    }
}
impl Ord for HeapEntry {
    // BinaryHeap is a max-heap; reverse so the smallest record pops first.
    fn cmp(&self, other: &Self) -> Ordering {
        self.key_cmp(other).reverse()
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Drain a set of pre-sorted chunk BAMs into `out` via k-way merge.
/// `out` is assumed to already have the header written to it.
/// Returns the post-write uncompressed offset (used by BAI).
fn merge_chunks_into<W: Write>(
    out: &mut W,
    chunk_paths: &[PathBuf],
    mut builder: Option<&mut super::index::BaiBuilder>,
    starting_uoffset: u64,
) -> io::Result<u64> {
    info!(
        n_chunks = chunk_paths.len(),
        "BamSort: opening chunks for k-way merge"
    );
    let mut readers: Vec<ChunkReader> = chunk_paths
        .iter()
        .enumerate()
        .map(|(i, p)| ChunkReader::open(p, i))
        .collect::<io::Result<_>>()?;

    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(readers.len());
    for r in readers.iter_mut() {
        if let Some(e) = r.read()? {
            heap.push(e);
        }
    }

    // Log every ~10M merged records — gives a heartbeat every few seconds at typical merge
    // throughput. Cheap branch on the hot path.
    const MERGE_LOG_EVERY: u64 = 10_000_000;
    let mut merged_records: u64 = 0;
    let mut next_log_at: u64 = MERGE_LOG_EVERY;
    let merge_start = Instant::now();

    let mut uoffset = starting_uoffset;
    while let Some(top) = heap.pop() {
        let HeapEntry {
            record, file_idx, ..
        } = top;
        let start = uoffset;
        record.write(out)?;
        uoffset += 4 + record.data.len() as u64;
        if let Some(b) = builder.as_deref_mut() {
            b.add_record(&record, start, uoffset);
        }
        // Hand the Vec back to the chunk reader for the next read — mirrors
        // htslib's bam1_t.data realloc-and-keep pattern.
        readers[file_idx].return_buf(record.data);
        if let Some(e) = readers[file_idx].read()? {
            heap.push(e);
        }
        merged_records += 1;
        if merged_records >= next_log_at {
            info!(
                reads = %fmt_count(merged_records),
                elapsed_secs = merge_start.elapsed().as_secs(),
                "BamSort: merged"
            );
            next_log_at += MERGE_LOG_EVERY;
        }
    }
    info!(
        reads = %fmt_count(merged_records),
        elapsed_secs = merge_start.elapsed().as_secs(),
        "BamSort: merge done"
    );
    Ok(uoffset)
}

fn update_header(h: &mut Header, opts: &SortOptions<'_>) {
    let so = match opts.order {
        Order::Coordinate => "coordinate",
    };
    h.text = update_or_add_hd_so(&h.text, so);
    if !opts.no_pg {
        h.text = append_pg_line(&h.text, opts.arg_list);
    }
}

fn prepare_output_header(
    header: &Header,
    reference_order: ReferenceOrder,
) -> io::Result<(Header, Option<Vec<i32>>)> {
    match reference_order {
        ReferenceOrder::Preserve => Ok((header.clone(), None)),
        ReferenceOrder::Lexicographic => {
            let mut order: Vec<usize> = (0..header.refs.len()).collect();
            order.sort_by(|&a, &b| header.refs[a].name.cmp(&header.refs[b].name));
            if order.iter().copied().eq(0..header.refs.len()) {
                return Ok((header.clone(), None));
            }

            let mut old_to_new = vec![0i32; header.refs.len()];
            let mut refs = Vec::with_capacity(header.refs.len());
            for (new_idx, old_idx) in order.iter().copied().enumerate() {
                old_to_new[old_idx] = i32::try_from(new_idx).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "too many BAM references")
                })?;
                refs.push(header.refs[old_idx].clone());
            }

            let text = reorder_sq_lines(&header.text, &refs);
            Ok((Header { text, refs }, Some(old_to_new)))
        }
    }
}

fn remap_record_refs(record: &mut Record, ref_id_map: Option<&[i32]>) -> io::Result<()> {
    let Some(map) = ref_id_map else {
        return Ok(());
    };
    let ref_id = record.ref_id();
    if ref_id >= 0 {
        record.set_ref_id(remap_ref_id(ref_id, map)?);
    }
    let next_ref_id = record.next_ref_id();
    if next_ref_id >= 0 {
        record.set_next_ref_id(remap_ref_id(next_ref_id, map)?);
    }
    Ok(())
}

fn remap_ref_id(ref_id: i32, map: &[i32]) -> io::Result<i32> {
    let idx = usize::try_from(ref_id)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative reference id"))?;
    map.get(idx).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("reference id {ref_id} exceeds BAM header references"),
        )
    })
}

fn reorder_sq_lines(text: &[u8], refs: &[super::bam::RefInfo]) -> Vec<u8> {
    let lines = split_lines_keep_terminator(text);
    let mut sq_by_name: Vec<(&[u8], &[u8])> = Vec::new();
    for line in &lines {
        if line.starts_with(b"@SQ") {
            if let Some(name) = field_value(line, b"SN") {
                sq_by_name.push((name, *line));
            }
        }
    }

    let mut sorted_sq = Vec::new();
    for r in refs {
        match sq_by_name
            .iter()
            .find_map(|(name, line)| (*name == r.name.as_slice()).then_some(*line))
        {
            Some(line) => sorted_sq.extend_from_slice(line),
            None => {
                sorted_sq.extend_from_slice(b"@SQ\tSN:");
                sorted_sq.extend_from_slice(&r.name);
                sorted_sq.extend_from_slice(b"\tLN:");
                sorted_sq.extend_from_slice(r.length.to_string().as_bytes());
                sorted_sq.push(b'\n');
            }
        }
    }

    let mut out = Vec::with_capacity(text.len());
    let mut emitted_sq = false;
    for line in &lines {
        if line.starts_with(b"@SQ") {
            if !emitted_sq {
                out.extend_from_slice(&sorted_sq);
                emitted_sq = true;
            }
            continue;
        }
        out.extend_from_slice(line);
        if !emitted_sq && line.starts_with(b"@HD") {
            out.extend_from_slice(&sorted_sq);
            emitted_sq = true;
        }
    }
    if !emitted_sq {
        let mut with_sq = sorted_sq;
        with_sq.extend_from_slice(&out);
        return with_sq;
    }
    out
}

/// Update the SO: field on the @HD line, or insert an @HD line if absent.
/// SAM header text is a sequence of lines, each starting with `@<TAG>` and
/// ending with `\n`. Fields within a line are tab-separated.
fn update_or_add_hd_so(text: &[u8], so: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() + 32);
    let mut hd_seen = false;
    for line in split_lines_keep_terminator(text) {
        if !hd_seen && line.starts_with(b"@HD") {
            hd_seen = true;
            out.extend_from_slice(&rewrite_hd_so(line, so));
        } else {
            out.extend_from_slice(line);
        }
    }
    if !hd_seen {
        // Prepend @HD with VN:1.6 and the chosen SO.
        let mut prefix = Vec::with_capacity(out.len() + 32);
        prefix.extend_from_slice(b"@HD\tVN:1.6\tSO:");
        prefix.extend_from_slice(so.as_bytes());
        prefix.push(b'\n');
        prefix.extend_from_slice(&out);
        out = prefix;
    }
    out
}

fn rewrite_hd_so(line: &[u8], so: &str) -> Vec<u8> {
    // Strip terminator, split on tab, find/replace SO:, re-emit.
    let (body, term) = split_terminator(line);
    let mut fields: Vec<&[u8]> = body.split(|&b| b == b'\t').collect();
    let mut updated = false;
    for f in fields.iter_mut() {
        if f.starts_with(b"SO:") {
            // Replace this field — keep the &[u8] borrow alive until we re-emit.
            // Easier: build the output directly.
            updated = true;
            break;
        }
    }
    let mut out = Vec::with_capacity(line.len() + so.len());
    let mut first = true;
    for f in body.split(|&b| b == b'\t') {
        if !first {
            out.push(b'\t');
        }
        first = false;
        if f.starts_with(b"SO:") {
            out.extend_from_slice(b"SO:");
            out.extend_from_slice(so.as_bytes());
        } else {
            out.extend_from_slice(f);
        }
    }
    if !updated {
        out.push(b'\t');
        out.extend_from_slice(b"SO:");
        out.extend_from_slice(so.as_bytes());
    }
    out.extend_from_slice(term);
    out
}

/// Append a `@PG` line for samtools-rs. If the existing @PG chain has any
/// IDs starting with our base ID, suffix with `.1`, `.2`, ... to keep it
/// unique. PP points to the last @PG with no descendant (the chain leaf).
fn append_pg_line(text: &[u8], arg_list: Option<&str>) -> Vec<u8> {
    let base_id = "samtools-rs";
    let pg_lines: Vec<&[u8]> = split_lines_keep_terminator(text)
        .into_iter()
        .filter(|l| l.starts_with(b"@PG"))
        .collect();

    let existing_ids: Vec<Vec<u8>> = pg_lines
        .iter()
        .filter_map(|l| field_value(l, b"ID"))
        .map(|s| s.to_vec())
        .collect();

    let id = unique_id(base_id, &existing_ids);
    let pp = leaf_pg_id(&pg_lines);

    let mut line = Vec::with_capacity(128);
    line.extend_from_slice(b"@PG\tID:");
    line.extend_from_slice(id.as_bytes());
    line.extend_from_slice(b"\tPN:samtools-rs\tVN:");
    line.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
    if let Some(pp_id) = pp {
        line.extend_from_slice(b"\tPP:");
        line.extend_from_slice(&pp_id);
    }
    if let Some(cl) = arg_list {
        line.extend_from_slice(b"\tCL:");
        line.extend_from_slice(cl.as_bytes());
    }
    line.push(b'\n');

    let mut out = Vec::with_capacity(text.len() + line.len());
    // SAM convention is to ensure header ends with \n before appending.
    if !text.is_empty() && !text.ends_with(b"\n") {
        out.extend_from_slice(text);
        out.push(b'\n');
    } else {
        out.extend_from_slice(text);
    }
    out.extend_from_slice(&line);
    out
}

fn unique_id(base: &str, existing: &[Vec<u8>]) -> String {
    if !existing.iter().any(|e| e == base.as_bytes()) {
        return base.to_string();
    }
    let mut n = 1u32;
    loop {
        let cand = format!("{base}.{n}");
        if !existing.iter().any(|e| e == cand.as_bytes()) {
            return cand;
        }
        n += 1;
    }
}

/// Find the leaf of the @PG chain — the last @PG with no other @PG's PP
/// pointing past it. samtools' rule: the PG that nothing else has as PP.
fn leaf_pg_id(pg_lines: &[&[u8]]) -> Option<Vec<u8>> {
    let ids: Vec<&[u8]> = pg_lines
        .iter()
        .filter_map(|l| field_value(l, b"ID"))
        .collect();
    let pps: Vec<&[u8]> = pg_lines
        .iter()
        .filter_map(|l| field_value(l, b"PP"))
        .collect();
    for &id in ids.iter().rev() {
        if !pps.iter().any(|&pp| pp == id) {
            return Some(id.to_vec());
        }
    }
    None
}

fn field_value<'a>(line: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let (body, _) = split_terminator(line);
    for f in body.split(|&b| b == b'\t') {
        if f.len() >= 3 && f.starts_with(tag) && f[2] == b':' {
            return Some(&f[3..]);
        }
    }
    None
}

fn split_terminator(line: &[u8]) -> (&[u8], &[u8]) {
    if let Some(stripped) = line.strip_suffix(b"\r\n") {
        (stripped, b"\r\n")
    } else if let Some(stripped) = line.strip_suffix(b"\n") {
        (stripped, b"\n")
    } else {
        (line, b"")
    }
}

fn split_lines_keep_terminator(text: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < text.len() {
        if text[i] == b'\n' {
            out.push(&text[start..=i]);
            start = i + 1;
        }
        i += 1;
    }
    if start < text.len() {
        out.push(&text[start..]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn coord_cmp_sorts_by_tid_then_pos_then_strand() {
        // Mock records via direct data construction. Build a minimal record
        // with just the 32-byte core + a 1-byte read name + nothing else.
        fn rec(tid: i32, pos: i32, flag: u16) -> Record {
            let mut data = vec![0u8; 32 + 1];
            data[0..4].copy_from_slice(&tid.to_le_bytes());
            data[4..8].copy_from_slice(&pos.to_le_bytes());
            data[8] = 1; // l_read_name = 1 (just the NUL)
            data[9] = 0; // mapq
            data[10..12].copy_from_slice(&0u16.to_le_bytes());
            data[12..14].copy_from_slice(&0u16.to_le_bytes()); // n_cigar_op
            data[14..16].copy_from_slice(&flag.to_le_bytes());
            data[16..20].copy_from_slice(&0i32.to_le_bytes()); // l_seq
            data[20..24].copy_from_slice(&(-1i32).to_le_bytes());
            data[24..28].copy_from_slice(&(-1i32).to_le_bytes());
            data[28..32].copy_from_slice(&0i32.to_le_bytes());
            data[32] = 0; // NUL-terminated empty read name
            Record { data }
        }

        let unmapped = rec(-1, -1, 4);
        let chr1_50_fwd = rec(0, 49, 0);
        let chr1_50_rev = rec(0, 49, 16);
        let chr1_100 = rec(0, 99, 0);
        let chr2_0 = rec(1, 0, 0);

        let mut v = vec![unmapped, chr2_0, chr1_100, chr1_50_rev, chr1_50_fwd];
        v.sort_by(coord_cmp);
        let order: Vec<(i32, i32, u16)> =
            v.iter().map(|r| (r.ref_id(), r.pos(), r.flag())).collect();
        assert_eq!(
            order,
            vec![(0, 49, 0), (0, 49, 16), (0, 99, 0), (1, 0, 0), (-1, -1, 4)]
        );
    }

    #[test]
    fn end_to_end_sort_test_bam() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/small_unsorted.bam");
        let bytes = std::fs::read(path).unwrap();
        let mut out = Vec::new();
        sort_in_memory(
            Cursor::new(&bytes),
            &mut out,
            &SortOptions {
                order: Order::Coordinate,
                reference_order: ReferenceOrder::Lexicographic,
                level: 6,
                arg_list: Some("sort tests/data/small_unsorted.bam"),
                no_pg: false,
                max_mem: usize::MAX,
                tmp_prefix: PathBuf::from("/tmp/unused"),
                threads: 1,
                write_index: None,
            },
        )
        .unwrap();

        // Decompress output and check record order.
        let mut dec = Vec::new();
        bgzf::Reader::new(Cursor::new(&out))
            .read_to_end(&mut dec)
            .unwrap();
        let mut cur = Cursor::new(&dec);
        let h = Header::read(&mut cur).unwrap();
        let text = std::str::from_utf8(&h.text).unwrap();
        assert!(
            text.contains("SO:coordinate"),
            "header SO not updated: {text:?}"
        );
        assert!(
            text.contains("@PG\tID:samtools-rs"),
            "PG line not added: {text:?}"
        );
        assert!(
            !text.contains("SO:unsorted"),
            "SO:unsorted not replaced: {text:?}"
        );

        let mut recs = Vec::new();
        while let Some(r) = Record::read(&mut cur).unwrap() {
            recs.push(r);
        }
        let names: Vec<_> = recs.iter().map(|r| r.read_name().to_vec()).collect();
        // Expected: by tid then pos then strand. read001 (chr1:50 rev) first
        // because it's the only chr1:50 read, then read002+read003 at chr1:100
        // (input order preserved by stable sort), then chr2 reads, chr3 reads,
        // then unmapped read007 last.
        let expect: &[&[u8]] = &[
            b"read001", b"read002", b"read003", b"read005", b"read006", b"read004", b"read008",
            b"read007",
        ];
        assert_eq!(names, expect);
    }

    #[test]
    fn update_or_add_hd_so_replaces_existing() {
        let h = b"@HD\tVN:1.6\tSO:unsorted\n@SQ\tSN:x\tLN:1\n";
        let out = update_or_add_hd_so(h, "coordinate");
        assert_eq!(
            out,
            b"@HD\tVN:1.6\tSO:coordinate\n@SQ\tSN:x\tLN:1\n".to_vec()
        );
    }

    #[test]
    fn update_or_add_hd_so_inserts_when_missing() {
        let h = b"@SQ\tSN:x\tLN:1\n";
        let out = update_or_add_hd_so(h, "coordinate");
        assert_eq!(
            out,
            b"@HD\tVN:1.6\tSO:coordinate\n@SQ\tSN:x\tLN:1\n".to_vec()
        );
    }

    #[test]
    fn update_or_add_hd_so_appends_when_field_missing() {
        let h = b"@HD\tVN:1.6\n";
        let out = update_or_add_hd_so(h, "coordinate");
        assert_eq!(out, b"@HD\tVN:1.6\tSO:coordinate\n".to_vec());
    }

    #[test]
    fn append_pg_uses_unique_id_and_pp_chain() {
        let h = b"@HD\tVN:1.6\n@PG\tID:bwa\tPN:bwa\tVN:0.7\n";
        let out = append_pg_line(h, Some("sort foo.bam"));
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("@PG\tID:samtools-rs\tPN:samtools-rs\tVN:"));
        assert!(s.contains("\tPP:bwa"));
        assert!(s.ends_with("\tCL:sort foo.bam\n"));
    }

    #[test]
    fn lexicographic_reference_order_rewrites_header_and_mapping() {
        let header = Header {
            text: b"@HD\tVN:1.6\n@SQ\tSN:OPD_9\tLN:9\tM5:keep9\n@SQ\tSN:OPD_10\tLN:10\tM5:keep10\n@PG\tID:x\n".to_vec(),
            refs: vec![
                super::super::bam::RefInfo {
                    name: b"OPD_9".to_vec(),
                    length: 9,
                },
                super::super::bam::RefInfo {
                    name: b"OPD_10".to_vec(),
                    length: 10,
                },
            ],
        };

        let (out, map) = prepare_output_header(&header, ReferenceOrder::Lexicographic).unwrap();
        assert_eq!(
            out.refs
                .iter()
                .map(|r| r.name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"OPD_10".as_slice(), b"OPD_9".as_slice()]
        );
        assert_eq!(map.unwrap(), vec![1, 0]);
        assert_eq!(
            std::str::from_utf8(&out.text).unwrap(),
            "@HD\tVN:1.6\n@SQ\tSN:OPD_10\tLN:10\tM5:keep10\n@SQ\tSN:OPD_9\tLN:9\tM5:keep9\n@PG\tID:x\n"
        );
    }

    #[test]
    fn streaming_sort_with_forced_spill_matches_in_memory() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/small_unsorted.bam");
        let bytes = std::fs::read(path).unwrap();

        // In-memory reference output.
        let mut ref_out = Vec::new();
        sort_in_memory(
            Cursor::new(&bytes),
            &mut ref_out,
            &SortOptions {
                order: Order::Coordinate,
                reference_order: ReferenceOrder::Lexicographic,
                level: 6,
                arg_list: Some("sort small.bam"),
                no_pg: false,
                max_mem: usize::MAX,
                tmp_prefix: PathBuf::from("/tmp/unused"),
                threads: 1,
                write_index: None,
            },
        )
        .unwrap();

        // Streaming with a tiny -m to force per-record spill (8 chunks).
        let tmp_dir = std::env::temp_dir();
        let tmp_prefix = tmp_dir.join(format!(
            "samtools-rs-test.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut spill_out = Vec::new();
        sort_streaming(
            Cursor::new(&bytes),
            &mut spill_out,
            &SortOptions {
                order: Order::Coordinate,
                reference_order: ReferenceOrder::Lexicographic,
                level: 6,
                arg_list: Some("sort small.bam"),
                no_pg: false,
                max_mem: 1, // forces spill on every record
                tmp_prefix: tmp_prefix.clone(),
                threads: 1,
                write_index: None,
            },
        )
        .unwrap();

        // Decompressed bytes should match.
        let mut ref_dec = Vec::new();
        bgzf::Reader::new(Cursor::new(&ref_out))
            .read_to_end(&mut ref_dec)
            .unwrap();
        let mut spill_dec = Vec::new();
        bgzf::Reader::new(Cursor::new(&spill_out))
            .read_to_end(&mut spill_dec)
            .unwrap();
        assert_eq!(ref_dec, spill_dec, "external sort diverged from in-memory");
    }

    #[test]
    fn append_pg_avoids_duplicate_id() {
        let h = b"@PG\tID:samtools-rs\tPN:samtools-rs\tVN:0.0.0\n";
        let out = append_pg_line(h, None);
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("@PG\tID:samtools-rs.1\t"));
        assert!(s.contains("\tPP:samtools-rs"));
    }
}
