//! BWAMEM2 driver. Reads TIRP records and emits BAM with `CB:Z` / `UB:Z` cell-barcode tags
//! injected from the embedded read names.
//!
//! Faithful to stock bwa-mem2's streaming shape: read one fixed-size batch (stock
//! `chunk_size × threads` bases), align it (bwa's internal Rayon parallelism), then stream the
//! SAM lines straight into a `bam::io::Writer<bgzf::MultithreadedWriter>` (parallel deflate,
//! in-order output). Only one batch is resident at a time, so memory stays flat at
//! `index + O(1 batch)` instead of accumulating whole-batch SAM across a compressor pool.

use std::{
    io::{BufRead, BufReader, Read},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bascet_core::{Decode, DecodeResult};
use bascet_io::codec;
use bwa_mem2_rs::mem_api::{MemAligner, MemReadPair};
use bytesize::ByteSize;
use noodles::sam;
use tracing::{debug, info};

use super::output::{
    SamRecordSink, TaggedBamSamSink, TaggedBamWriter, create_tagged_bam_writer,
    finish_tagged_bam_writer,
};
use crate::utils::{atomic_temp_path_in_dir, publish_atomic_output};

const SOURCE_NAME: &str = "bwa-mem2";

struct DecodeRead<D> {
    decoder: D,
    eof: bool,
}

impl<D> DecodeRead<D> {
    fn new(decoder: D) -> Self {
        Self {
            decoder,
            eof: false,
        }
    }
}

impl<D: Decode> Read for DecodeRead<D> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.eof || buf.is_empty() {
            return Ok(0);
        }

        match self.decoder.decode_into(buf) {
            DecodeResult::Decoded(n) => Ok(n),
            DecodeResult::Eof => {
                self.eof = true;
                Ok(0)
            }
            DecodeResult::Error(err) => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
            }
        }
    }
}

/// Owns the BWAMEM2 aligner across batches.
pub struct StockDriverState {
    aligner: MemAligner,
    align_threads: usize,
    pub n_processed: i64,
}

impl StockDriverState {
    pub fn new(
        prefix: &Path,
        n_threads: usize,
        worker_pool: Arc<rayon::ThreadPool>,
    ) -> Result<Self> {
        let aligner = MemAligner::new_with_thread_pool(prefix, n_threads.max(1), worker_pool)
            .map_err(|err| anyhow::anyhow!(err))?;

        Ok(Self {
            aligner,
            align_threads: n_threads.max(1),
            n_processed: 0,
        })
    }

    pub fn sam_header(&self) -> Result<String> {
        self.aligner
            .sam_header()
            .map_err(|err| anyhow::anyhow!(err))
    }

    /// Stock bwa-mem2 batch size: `opt.chunk_size × n_threads` total sequence bases. Matching
    /// this keeps per-batch insert-size estimation (`mem_pestat`) identical to a stock
    /// `bwa-mem2 mem` run for the same thread count.
    fn stock_chunk_size(&self) -> usize {
        let opt = self.aligner.opt();
        let chunk_size = usize::try_from(opt.chunk_size.max(1)).unwrap_or(usize::MAX);
        chunk_size.saturating_mul(self.align_threads.max(1))
    }
}

/// Borrowed view of a parsed TIRP line. Mirrors the field layout used by
/// `bascet_io::parse::tirp::tirp_as_record::Tirp::parse_aligned`.
struct TirpFields<'a> {
    id: &'a [u8],
    r1: &'a [u8],
    r2: &'a [u8],
    q1: &'a [u8],
    q2: &'a [u8],
    umi: &'a [u8],
}

/// Parse a single newline-stripped TIRP record. Matches the upstream parser's slicing — 7 tabs
/// dividing the record into 8 fields: `id, _, _, r1, r2, q1, q2, umi`.
fn parse_tirp_line(line: &[u8]) -> Result<TirpFields<'_>> {
    let mut iter = memchr::memchr_iter(b'\t', line);
    let pos = [
        iter.next().context("tab 0")?,
        iter.next().context("tab 1")?,
        iter.next().context("tab 2")?,
        iter.next().context("tab 3")?,
        iter.next().context("tab 4")?,
        iter.next().context("tab 5")?,
        iter.next().context("tab 6")?,
    ];
    Ok(TirpFields {
        id: &line[..pos[0]],
        r1: &line[pos[2] + 1..pos[3]],
        r2: &line[pos[3] + 1..pos[4]],
        q1: &line[pos[4] + 1..pos[5]],
        q2: &line[pos[5] + 1..pos[6]],
        umi: &line[pos[6] + 1..],
    })
}

/// Compose a bascet-style read name: `{cell_id}:{umi}:{num_read}`.
fn write_bascet_read_name(dst: &mut String, record_id: &[u8], record_umi: &[u8], num_read: u64) {
    use std::fmt::Write;
    dst.reserve(record_id.len() + record_umi.len() + 24);
    dst.push_str(&String::from_utf8_lossy(record_id));
    dst.push(':');
    dst.push_str(&String::from_utf8_lossy(record_umi));
    dst.push(':');
    let _ = write!(dst, "{num_read}");
}

/// Offsets/lengths of one read pair's fields inside `BatchArena::bytes`.
#[derive(Clone, Copy)]
struct PairSlices {
    name_off: u32,
    name_len: u32,
    r1_off: u32,
    r1_len: u32,
    q1_off: u32,
    q1_len: u32,
    r2_off: u32,
    r2_len: u32,
    q2_off: u32,
    q2_len: u32,
}

/// Arena-backed batch of read pairs. All field bytes live in one contiguous `Vec<u8>` (one
/// allocation per batch instead of five per pair), and `MemReadPair` views borrow into it. The
/// arena is reused across batches via `clear()`, so steady-state allocation is bounded.
#[derive(Default)]
struct BatchArena {
    bytes: Vec<u8>,
    pairs: Vec<PairSlices>,
    /// Total sequence bases (r1 + r2 over all pairs); compared against the stock batch target.
    bases: usize,
}

impl BatchArena {
    fn push(
        &mut self,
        name: &[u8],
        r1: &[u8],
        q1: &[u8],
        r2: &[u8],
        q2: &[u8],
    ) -> Result<()> {
        if r1.len() != q1.len() {
            anyhow::bail!(
                "R1 sequence/quality length mismatch: {} != {}",
                r1.len(),
                q1.len()
            );
        }
        if r2.len() != q2.len() {
            anyhow::bail!(
                "R2 sequence/quality length mismatch: {} != {}",
                r2.len(),
                q2.len()
            );
        }

        let (name_off, name_len) = self.push_bytes(name)?;
        let (r1_off, r1_len) = self.push_bytes(r1)?;
        let (q1_off, q1_len) = self.push_bytes(q1)?;
        let (r2_off, r2_len) = self.push_bytes(r2)?;
        let (q2_off, q2_len) = self.push_bytes(q2)?;

        self.pairs.push(PairSlices {
            name_off,
            name_len,
            r1_off,
            r1_len,
            q1_off,
            q1_len,
            r2_off,
            r2_len,
            q2_off,
            q2_len,
        });
        self.bases += r1.len() + r2.len();
        Ok(())
    }

    fn push_bytes(&mut self, src: &[u8]) -> Result<(u32, u32)> {
        let off = u32::try_from(self.bytes.len())
            .map_err(|_| anyhow::anyhow!("BWAMEM2 read batch arena exceeded 4 GiB"))?;
        let len = u32::try_from(src.len())
            .map_err(|_| anyhow::anyhow!("BWAMEM2 read batch field exceeded 4 GiB"))?;
        self.bytes.extend_from_slice(src);
        Ok((off, len))
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.pairs.clear();
        self.bases = 0;
    }

    fn len(&self) -> usize {
        self.pairs.len()
    }

    fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    fn view(&self, i: usize) -> MemReadPair<'_> {
        let p = &self.pairs[i];
        let bytes = &self.bytes;
        // SAFETY: `name` was written from a `String` (via `write_bascet_read_name`), so it is
        // valid UTF-8; the arena is append-only within a batch.
        let name = unsafe { std::str::from_utf8_unchecked(slice(bytes, p.name_off, p.name_len)) };
        MemReadPair {
            name,
            r1: slice(bytes, p.r1_off, p.r1_len),
            q1: slice(bytes, p.q1_off, p.q1_len),
            r2: slice(bytes, p.r2_off, p.r2_len),
            q2: slice(bytes, p.q2_off, p.q2_len),
        }
    }
}

#[inline]
fn slice(bytes: &[u8], off: u32, len: u32) -> &[u8] {
    &bytes[off as usize..off as usize + len as usize]
}

/// Align one batch and stream its SAM records straight to the BAM writer. Returns the number of
/// read pairs aligned. Nothing beyond the (crate-internal) per-batch SAM buffer is retained.
fn align_and_write_batch(
    state: &mut StockDriverState,
    batch: &BatchArena,
    writer: &mut TaggedBamWriter,
    header: &sam::Header,
) -> Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }

    let pairs: Vec<MemReadPair<'_>> = (0..batch.len()).map(|i| batch.view(i)).collect();
    let mut sink = TaggedBamSamSink::new(writer, header, SOURCE_NAME);

    // `align_pairs_into` aligns the whole batch (bwa's internal Rayon parallelism) and then
    // invokes the callback once per emitted SAM line, in read order. We convert each line to a
    // tagged BAM record and hand it to the MultithreadedWriter immediately — no whole-batch
    // materialization on our side.
    let mut sink_err: Option<anyhow::Error> = None;
    state
        .aligner
        .align_pairs_into(&pairs, |line| {
            match sink.record(line) {
                Ok(()) => Ok(()),
                Err(err) => {
                    let msg = err.to_string();
                    sink_err = Some(err);
                    Err(msg)
                }
            }
        })
        .map_err(|err| match sink_err.take() {
            Some(err) => err,
            None => anyhow::anyhow!(err),
        })?;

    let n_pairs = pairs.len();
    state.n_processed += (n_pairs * 2) as i64;
    Ok(n_pairs)
}

/// TIRP → BAM driver. Streams read pairs through stock-sized batches into a parallel BGZF BAM
/// writer, keeping memory flat.
pub fn run_stock_driver_tirp_to_bam(
    state: &mut StockDriverState,
    path_in: &Path,
    out_path_unsorted: &Path,
    path_temp: &Path,
    total_memory: ByteSize,
    total_threads: u64,
    _worker_pool: Arc<rayon::ThreadPool>,
    max_batch_pairs: usize,
) -> Result<()> {
    info!(
        input = %path_in.display(),
        output = %out_path_unsorted.display(),
        "Starting BWAMEM2 alignment"
    );

    let header_text = state.sam_header()?;
    let header: sam::Header = header_text.parse().context("parse generated SAM header")?;

    std::fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create temp dir {}", path_temp.display()))?;

    // Pre-flight: the aligner index is already resident. If it alone does not fit in the budget,
    // fail fast instead of thrashing. The streaming pipeline itself needs only ~one batch on top.
    if let Some(mem) = memory_stats::memory_stats() {
        let rss = ByteSize(mem.physical_mem as u64);
        if rss.as_u64() >= total_memory.as_u64() {
            anyhow::bail!(
                "BWAMEM2 index/runtime RSS ({rss}) already meets or exceeds --memory {total_memory}; increase --memory"
            );
        }
        debug!(index_loaded_rss = %rss, "BWAMEM2 streaming driver: memory pre-flight");
    }

    // Output BAM (published atomically on success). Staged under --temp until complete.
    let out_tmp = atomic_temp_path_in_dir(out_path_unsorted, path_temp);
    let writer_threads = (total_threads.max(1) as usize).clamp(1, 8);
    let mut writer = create_tagged_bam_writer(&out_tmp, &header, writer_threads)?;

    let batch_target_bases = state.stock_chunk_size();
    // bwa-mem2 (and the `bwa-mem2-pure-rs` port) materialize an *entire* batch's alignment
    // scratch and SAM before returning, so per-batch peak memory scales with the batch's read
    // count. On low-complexity / over-amplified (e.g. MDA) repeat regions each read produces far
    // more candidate alignments and SAM text, so a batch sized purely by bases (~390k pairs for
    // 256-base reads) can transiently balloon by tens of GB. Capping pairs bounds that transient,
    // keeping peak RSS flat and predictable regardless of the input's local repeat content. The
    // base-count target still applies as an upper bound for unusually long reads.
    let max_batch_pairs = max_batch_pairs.max(1);
    debug!(
        align_threads = state.align_threads,
        writer_threads,
        batch_target_bases,
        max_batch_pairs,
        "BWAMEM2 streaming driver: config"
    );

    let decoder = codec::BBGZDecoder::builder().with_path(path_in).build();
    let mut tirp_lines = BufReader::with_capacity(1 << 20, DecodeRead::new(decoder));
    let mut line_buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut name_buf = String::new();
    let mut batch = BatchArena::default();
    let mut num_read: u64 = 0;
    let mut n_batches: u64 = 0;

    loop {
        line_buf.clear();
        let n = tirp_lines
            .read_until(b'\n', &mut line_buf)
            .context("read TIRP line")?;
        if n == 0 {
            break;
        }
        while matches!(line_buf.last(), Some(b'\n') | Some(b'\r')) {
            line_buf.pop();
        }
        if line_buf.is_empty() {
            continue;
        }

        let fields = parse_tirp_line(&line_buf)
            .with_context(|| format!("malformed TIRP line at record {num_read}"))?;
        name_buf.clear();
        write_bascet_read_name(&mut name_buf, fields.id, fields.umi, num_read);
        batch.push(name_buf.as_bytes(), fields.r1, fields.q1, fields.r2, fields.q2)?;
        num_read += 1;

        if batch.bases >= batch_target_bases || batch.len() >= max_batch_pairs {
            align_and_write_batch(state, &batch, &mut writer, &header)?;
            batch.clear();
            n_batches += 1;
            info!(reads_m = state.n_processed / 1_000_000, "BWAMEM2 aligned");
        }
    }

    if !batch.is_empty() {
        align_and_write_batch(state, &batch, &mut writer, &header)?;
        n_batches += 1;
    }

    finish_tagged_bam_writer(writer)?;
    publish_atomic_output(&out_tmp, &out_path_unsorted.to_path_buf())?;

    info!(
        n_batches,
        n_reads_processed = state.n_processed,
        "BWAMEM2 stock-driver BAM: done"
    );
    Ok(())
}
