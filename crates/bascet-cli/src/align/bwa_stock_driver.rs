//! BWAMEM2 driver. Replicates the inner loop of
//! `bwa_mem2_rs::generated::fastmap_cpp::process` (the `_pipe_threads <= 1` branch) but
//! reads TIRP records (instead of kseq FASTQ) and emits BAM (with `CB:Z` / `UB:Z` cell-barcode
//! tags injected from the embedded read-names).
//!
//! Pipeline: reader → aligner → compressor pool → writer, connected by bounded queues with
//! `ReadMemoryLimiter` (input-byte × multiplier) and `InFlightLimiter` (chunk count). BGZF
//! compression happens on the compressor pool workers; the writer thread serially concatenates
//! pre-compressed chunks (in source order) into a plain `BufWriter<File>`. No
//! `MultithreadedWriter`; deflate parallelism comes from the compressor pool itself.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

use anyhow::{Context, Result};
use bascet_core::{Decode, DecodeResult};
use bascet_io::codec;
use bwa_mem2_rs::generated::{
    bwa_h::bseq1_t,
    bwamem_cpp::{mem_opt_init, mem_process_seqs, with_current_rayon_pool},
    bwamem_h::{mem_opt_t, mem_pestat_t, worker_t},
    fmi_search_cpp::FMI_search,
};
use bytesize::ByteSize;
use crossbeam::channel;
use noodles::{bam, bgzf, sam};
use tracing::{debug, info};

use crate::fileformat::bam::readname_to_cell_umi;
use sam::alignment::record_buf::data::field::Value;
use sam::alignment::{RecordBuf, io::Write as _, record::data::field::Tag};

use crate::utils::{atomic_temp_path, publish_atomic_output};

const MEM_F_PE: i32 = 0x2;

/// Mirror of the private `BATCH_SIZE` const in bwa-mem2-rs (= 512). Used by the per-tid `lim`
/// vec inside `mem_cache`, which is sized at `BATCH_SIZE + 32` per thread.
const BATCH_SIZE: usize = 512;

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

/// Build the forward-plus-reverse-complement reference layout used by `mem_kernel2_core`.
/// Mirrors private `pac_to_reference_layout` in bwa-mem2-rs.
fn build_ref_string(l_pac: i64, pac: &[u8]) -> Vec<u8> {
    let l_pac_usize = usize::try_from(l_pac).expect("l_pac fits in usize");
    let mut forward = vec![0_u8; l_pac_usize];
    for (i, base) in forward.iter_mut().enumerate() {
        let shift = (((!(i as i64)) & 3) << 1) as u8;
        *base = (pac[i >> 2] >> shift) & 3;
    }
    let mut ref_string = forward.clone();
    ref_string.extend(forward.iter().rev().map(|&b| if b < 4 { 3 - b } else { b }));
    ref_string
}

/// Mirror of private `ensure_mem_cache_thread_slots` in bwa-mem2-rs, exposed so we can
/// initialize the worker mmc without going through `memoryAlloc` (which wants a full
/// `ktp_aux_t` and is more than we need).
fn ensure_mem_cache_thread_slots(w: &mut worker_t, nthreads: usize, nreads: usize) {
    use bwa_mem2_rs::generated::bwamem_h::{mem_alnreg_v, mem_chain_v};

    w.regs = vec![mem_alnreg_v::default(); nreads];
    w.chain_ar = vec![mem_chain_v::default(); nreads];
    w.seedBufSize = 0;
    w.seedBuf.clear();

    w.mmc.seqBufLeftRef.resize_with(nthreads, Vec::new);
    w.mmc.seqBufLeftQer.resize_with(nthreads, Vec::new);
    w.mmc.seqBufRightRef.resize_with(nthreads, Vec::new);
    w.mmc.seqBufRightQer.resize_with(nthreads, Vec::new);
    w.mmc.wsize_buf_ref.resize(nthreads, 0);
    w.mmc.wsize_buf_qer.resize(nthreads, 0);

    w.mmc.seqPairArrayAux.resize_with(nthreads, Vec::new);
    w.mmc.seqPairArrayLeft128.resize_with(nthreads, Vec::new);
    w.mmc.seqPairArrayRight128.resize_with(nthreads, Vec::new);
    w.mmc.wsize.resize(nthreads, 0);

    w.mmc.wsize_mem.resize(nthreads, 0);
    w.mmc.wsize_mem_s.resize(nthreads, 0);
    w.mmc.wsize_mem_r.resize(nthreads, 0);
    w.mmc.matchArray.resize_with(nthreads, Vec::new);
    w.mmc.min_intv_ar.resize_with(nthreads, Vec::new);
    w.mmc.query_pos_ar.resize_with(nthreads, Vec::new);
    w.mmc.enc_qdb.resize_with(nthreads, Vec::new);
    w.mmc.rid.resize_with(nthreads, Vec::new);
    w.mmc.lim.resize_with(nthreads, || vec![0; BATCH_SIZE + 32]);
}

/// Owns everything that lives across batches in stock's `process()`.
pub struct StockDriverState {
    pub opt: mem_opt_t,
    /// Worker holds the persistent mmc + n_processed counter + fmi reference. Same lifetime
    /// pattern as stock's `worker_t`.
    pub worker: worker_t,
    pub n_processed: i64,
}

impl StockDriverState {
    pub fn new(prefix: &str, n_threads: usize) -> Result<Self> {
        // Load index.
        let mut fmi = FMI_search::ctor(prefix);
        fmi.load_index();
        if fmi.base.idx.bns.is_none() {
            anyhow::bail!("failed to load bwa-mem2 index from {prefix}");
        }

        // Build opt with PE + thread count.
        let mut opt = *mem_opt_init();
        opt.n_threads = i32::try_from(n_threads.max(1))
            .map_err(|_| anyhow::anyhow!("thread count too large: {n_threads}"))?;
        opt.flag |= MEM_F_PE;

        // Build ref_string once, attach to worker.
        let bns = fmi.base.idx.bns.as_ref().expect("bns loaded");
        let pac = fmi.base.idx.pac.as_slice();
        let ref_string = build_ref_string(bns.l_pac, pac);

        // Worker: fmi by value (matches stock's worker_t.fmi: Option<FMI_search>).
        let mut worker = worker_t {
            fmi: Some(fmi),
            nthreads: i16::try_from(opt.n_threads).expect("nthreads"),
            ref_string,
            ..Default::default()
        };

        // Pre-init per-tid slots so SAM-PE batch kernels don't panic on first call.
        // nreads is just an initial guess for regs/chain_ar capacity; mem_process_seqs
        // resizes on demand.
        ensure_mem_cache_thread_slots(&mut worker, n_threads.max(1), 0);

        Ok(Self {
            opt,
            worker,
            n_processed: 0,
        })
    }

    pub fn sam_header(&self) -> String {
        let bns = self
            .worker
            .fmi
            .as_ref()
            .expect("fmi loaded")
            .base
            .idx
            .bns
            .as_ref()
            .expect("bns loaded");
        let mut out = String::new();
        for ann in &bns.anns {
            out.push_str("@SQ\tSN:");
            out.push_str(&ann.name);
            out.push_str("\tLN:");
            out.push_str(&ann.len.to_string());
            if ann.is_alt != 0 {
                out.push_str("\tAH:*");
            }
            out.push('\n');
        }
        out.push_str("@PG\tID:bwa-mem2-rs\tPN:bwa-mem2-rs\n");
        out
    }
}

fn make_bseq(id: i32, name: &str, seq: &[u8], qual: &[u8]) -> Result<bseq1_t> {
    if seq.len() != qual.len() {
        anyhow::bail!(
            "sequence/quality length mismatch for {name}: {} != {}",
            seq.len(),
            qual.len()
        );
    }
    let seq = std::str::from_utf8(seq)
        .with_context(|| format!("read sequence for {name} is not valid UTF-8"))?;
    let qual = std::str::from_utf8(qual)
        .with_context(|| format!("read qualities for {name} are not valid UTF-8"))?;
    let l_seq =
        i32::try_from(seq.len()).map_err(|_| anyhow::anyhow!("read too long: {}", seq.len()))?;
    Ok(bseq1_t {
        l_seq,
        id,
        name: Some(name.to_string()),
        comment: None,
        seq: Some(seq.to_string()),
        qual: Some(qual.to_string()),
        sam: None,
        seq_nt4: Vec::new(),
    })
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

/// Format an integer with comma thousands separators for human-readable log output.
fn comma(n: i64) -> String {
    let abs = n.unsigned_abs().to_string();
    let bytes = abs.as_bytes();
    let mut out = String::with_capacity(abs.len() + abs.len() / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
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

/// Local copy of stock's private `process_batch` body (PE branch only — no MEM_F_SMARTPE).
/// After alignment, drain SAM strings out of `seqs[i].sam` into a `Vec<Box<str>>`. Each entry
/// may contain multiple newline-separated SAM records (primary + secondaries).
fn process_batch_into_sam_lines(
    seqs: &mut Vec<bseq1_t>,
    opt: &mut mem_opt_t,
    n_processed: i64,
    pes0: Option<&[mem_pestat_t; 4]>,
    worker: &mut worker_t,
    worker_pool: &rayon::ThreadPool,
) -> Vec<Box<str>> {
    let n_seqs = i32::try_from(seqs.len()).expect("n_seqs fits in i32");
    worker_pool.install(|| {
        with_current_rayon_pool(|| {
            mem_process_seqs(opt, n_processed, n_seqs, seqs, pes0, worker);
        });
    });

    let mut sam_lines: Vec<Box<str>> = Vec::with_capacity(seqs.len());
    for seq in seqs.iter_mut() {
        if let Some(sam) = seq.sam.take() {
            sam_lines.push(sam.into_boxed_str());
        }
        // Match stock's cleanup — frees per-read owned strings before next batch starts.
        seq.name = None;
        seq.comment = None;
        seq.seq = None;
        seq.qual = None;
        seq.seq_nt4.clear();
    }
    seqs.clear();
    seqs.shrink_to(0);
    sam_lines
}

/// How many records to encode+compress per parallel chunk. Each chunk produces one Vec<u8>
/// of *compressed* BGZF bytes (encode → uncompressed BAM record bytes → BGZF deflate, all on
/// one rayon worker), which then get concatenated into the output file serially. 4096 keeps
/// per-chunk uncompressed byte size near 1 MiB (~16 BGZF blocks of 65280 B each) and gives
/// rayon enough granularity to load-balance across `align_threads` workers.
const ENCODE_CHUNK_RECORDS: usize = 4096;

/// The fixed-content empty BGZF block used as the BAM EOF marker. Per the BGZF spec, any
/// concatenation of valid BGZF blocks is itself valid BGZF, so we can compress chunks
/// independently in parallel, strip each chunk's trailing EOF marker, concatenate, and
/// append exactly one EOF marker at the very end. See SAM/BAM spec §4.1.2.
const BGZF_EOF_BLOCK: [u8; 28] = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// Encode + compress one helper-input bytes buffer into a self-contained BGZF blob and strip
/// the trailing EOF marker so the result can be safely concatenated with another such blob.
/// Used by both the header path and the per-chunk record path.
fn bgzf_compress_chunk_no_eof(uncompressed: &[u8]) -> Result<Vec<u8>> {
    let buf: Vec<u8> = Vec::with_capacity(uncompressed.len() / 3 + 64);
    let mut bgzf = bgzf::io::Writer::new(buf);
    bgzf.write_all(uncompressed).context("compress chunk")?;
    let mut compressed = bgzf.finish().context("finish bgzf chunk")?;
    if compressed.ends_with(&BGZF_EOF_BLOCK) {
        compressed.truncate(compressed.len() - BGZF_EOF_BLOCK.len());
    }
    Ok(compressed)
}

/// Parse one SAM text line into a `RecordBuf` with `CB:Z` (cell barcode) and optional `UB:Z`
/// (UMI) tags attached from the embedded read-name. Returns `Ok(None)` for empty input lines.
/// Stateless and thread-safe — runs on rayon worker threads.
fn parse_sam_line_to_record(line: &str, header: &sam::Header) -> Result<Option<RecordBuf>> {
    use std::io::Cursor;
    let line = line.trim_end_matches('\n');
    if line.is_empty() {
        return Ok(None);
    }

    let qname = line
        .split('\t')
        .next()
        .filter(|f| !f.is_empty())
        .context("SAM record missing QNAME")?;
    let (cell_id_bytes, umi_bytes) = readname_to_cell_umi(qname.as_bytes());
    let cell_id = std::str::from_utf8(cell_id_bytes)
        .with_context(|| format!("cell id in read name not UTF-8: {qname:?}"))?;
    let umi_str = if umi_bytes.is_empty() {
        None
    } else {
        Some(
            std::str::from_utf8(umi_bytes)
                .with_context(|| format!("UMI in read name not UTF-8: {qname:?}"))?,
        )
    };

    // SAM records emitted by `mem_aln2sam` may have empty SEQ/QUAL fields for unmapped reads;
    // noodles expects those as `*`. Normalize via simple string substitution.
    let normalized = normalize_empty_sam_seq_qual(line);
    let mut sam_reader = sam::io::Reader::new(Cursor::new(normalized.as_bytes()));
    let mut record = RecordBuf::default();
    sam_reader
        .read_record_buf(header, &mut record)
        .with_context(|| format!("failed to parse SAM record: {normalized}"))?;

    record
        .data_mut()
        .insert(Tag::CELL_BARCODE_ID, Value::from(cell_id.to_owned()));
    if let Some(umi) = umi_str {
        record
            .data_mut()
            .insert(Tag::new(b'U', b'B'), Value::from(umi.to_owned()));
    }

    Ok(Some(record))
}

/// Replace empty SEQ (field 9) or QUAL (field 10) with `*`. Matches the same normalization used
/// by `align_output::parse_sam_line_with_cell_umi`.
fn normalize_empty_sam_seq_qual(line: &str) -> std::borrow::Cow<'_, str> {
    let mut fields = line.split('\t');
    let mut normalized = String::new();
    let mut changed = false;
    for field_index in 0..11 {
        let Some(field) = fields.next() else {
            return std::borrow::Cow::Borrowed(line);
        };
        let field = if (field_index == 9 || field_index == 10) && field.is_empty() {
            changed = true;
            "*"
        } else {
            field
        };
        if changed && normalized.is_empty() {
            let mut prefix_end = 0;
            for (seen, (offset, _)) in line.match_indices('\t').enumerate() {
                if seen == field_index {
                    break;
                }
                prefix_end = offset + 1;
            }
            normalized.push_str(&line[..prefix_end]);
        }
        if changed {
            if field_index > 0 && !normalized.ends_with('\t') {
                normalized.push('\t');
            }
            normalized.push_str(field);
        }
    }
    if !changed {
        return std::borrow::Cow::Borrowed(line);
    }
    for field in fields {
        normalized.push('\t');
        normalized.push_str(field);
    }
    std::borrow::Cow::Owned(normalized)
}

// ============================================================================
// BAM driver — getraw-style pipeline. Reader → aligner → compressor pool →
// writer with bounded memory + in-flight limiters and full thread-load
// autobalancing. This is the default and only BWAMEM2 BAM output path.
// ============================================================================

/// Fraction of the user's `--memory` budget the pipeline is allowed to use for in-flight
/// batches (the rest is for the FMI index, sort buffers, OS, etc.).
const MEMORY_BUDGET_FRACTION: f64 = 1.0;
const MIN_PIPELINE_MEM_CAP: usize = 256 * 1024 * 1024;

use crate::command::limiters::{
    InFlightLimiter, InFlightPermit, ReadMemoryLimiter, ReadMemoryPermit,
};

/// One alignment-input batch flowing reader → aligner.
struct AlignBatch {
    batch_idx: u64,
    seqs: Vec<bseq1_t>,
    /// Charge for the input bytes; held for the entire pipeline lifetime of this batch (it
    /// is moved into an `Arc` after alignment so all chunks of this batch share it, and the
    /// memory is released only when the last chunk has been written to disk).
    permit: ReadMemoryPermit,
}

/// One chunk of SAM blocks flowing aligner → compressor pool. We hand a shared `Arc` of the
/// full batch's blocks plus a `[start, end)` range, instead of cloning sub-slices, to avoid
/// per-line allocations on the aligner's hot path (each batch has ~1.7M records).
struct SamBatch {
    batch_idx: u64,
    blocks: Vec<Box<str>>,
    /// Held until the chunker has moved the permit into all chunk work items.
    permit: ReadMemoryPermit,
}

/// One chunk of SAM blocks flowing chunker → compressor pool. We hand a shared `Arc` of the
/// full batch's blocks plus a `[start, end)` range, instead of cloning sub-slices, to avoid
/// per-line allocations on the aligner's hot path (each batch has ~1.7M records).
struct ChunkWork {
    batch_idx: u64,
    chunk_idx: u32,
    total_chunks: u32,
    blocks: Arc<Vec<Box<str>>>,
    block_start: usize,
    block_end: usize,
    /// Shared with all chunks of the same batch. Drops when the last chunk is dropped.
    _batch_permit: Arc<ReadMemoryPermit>,
    /// Caps the number of compressor work items in flight.
    _inflight: InFlightPermit,
}

/// Approximate target number of blocks (= input read pairs from `mem_process_seqs`) per
/// compressor work item. ~2048 blocks ≈ 4-6K records ≈ ~1 MiB compressed output, which
/// matches `ENCODE_CHUNK_RECORDS` from the sync path's chunk sizing.
const BLOCKS_PER_CHUNK: usize = 2048;

/// One compressed BGZF chunk flowing compressor pool → writer.
struct CompressedChunk {
    batch_idx: u64,
    chunk_idx: u32,
    total_chunks: u32,
    bytes: Vec<u8>,
    _batch_permit: Arc<ReadMemoryPermit>,
    _inflight: InFlightPermit,
}

/// TIRP → BAM driver. Reader / aligner / compressor pool / writer stages connected by bounded
/// queues; `ReadMemoryLimiter` caps in-flight bytes (charged per batch by sequence length ×
/// multiplier), `InFlightLimiter` caps the compressor work-item queue depth.
pub fn run_stock_driver_tirp_to_bam(
    state: &mut StockDriverState,
    path_in: &Path,
    out_path_unsorted: &Path,
    total_memory: ByteSize,
    total_threads: u64,
    worker_pool: Arc<rayon::ThreadPool>,
    mem_overhead_per_input_byte: u64,
) -> Result<()> {
    info!(
        input = %path_in.display(),
        output = %out_path_unsorted.display(),
        "Starting BWAMEM2 alignment"
    );

    // Build the SAM header up front (used both by the writer thread and by every compressor
    // worker for tag injection / record encoding).
    let header_text = state.sam_header();
    let header: Arc<sam::Header> =
        Arc::new(header_text.parse().context("parse generated SAM header")?);

    // Output file (atomic publish on success).
    let out_tmp = atomic_temp_path(out_path_unsorted);
    let file = std::fs::File::create(&out_tmp)
        .with_context(|| format!("failed to create BAM output {out_tmp:?}"))?;
    let mut out_file = std::io::BufWriter::with_capacity(1 << 20, file);

    // Header → BGZF-compressed bytes (no EOF) → write up front.
    let header_bytes = {
        let buf: Vec<u8> = Vec::new();
        let mut bam_w = bam::io::Writer::from(buf);
        bam_w.write_header(&header).context("encode BAM header")?;
        bam_w.into_inner()
    };
    let header_compressed = bgzf_compress_chunk_no_eof(&header_bytes)?;
    out_file
        .write_all(&header_compressed)
        .context("write BAM header to output file")?;
    drop(header_bytes);
    drop(header_compressed);

    // ---------- Budgets ----------
    let align_threads = state.opt.n_threads.max(1) as usize;
    let total_threads_usize = total_threads.max(1) as usize;
    let requested_mem_cap = ((total_memory.as_u64() as f64) * MEMORY_BUDGET_FRACTION) as usize;
    let memory_headroom = ByteSize(
        ByteSize::gib(2)
            .as_u64()
            .max((total_memory.as_u64() as f64 * 0.10) as u64),
    );
    let (mem_cap, index_loaded_rss) = match memory_stats::memory_stats() {
        Some(memory) => {
            let rss = ByteSize(memory.physical_mem as u64);
            let available = total_memory
                .as_u64()
                .saturating_sub(rss.as_u64())
                .saturating_sub(memory_headroom.as_u64());
            if available < MIN_PIPELINE_MEM_CAP as u64 {
                anyhow::bail!(
                    "BWAMEM2 index/runtime RSS ({rss}) leaves only {} after reserving {memory_headroom}; refusing to start pipeline under --memory {total_memory}",
                    ByteSize(available)
                );
            }
            (requested_mem_cap.min(available as usize), Some(rss))
        }
        None => (requested_mem_cap, None),
    };
    let stock_chunk_size = i64::from(state.opt.chunk_size) * i64::from(state.opt.n_threads);
    let mem_overhead_per_input_byte = mem_overhead_per_input_byte.max(1);
    let memory_capped_chunk_size = (mem_cap / mem_overhead_per_input_byte as usize)
        .max(1 << 20)
        .min(stock_chunk_size as usize);
    let chunk_size = i64::try_from(memory_capped_chunk_size)
        .map_err(|_| anyhow::anyhow!("BWAMEM2 chunk size too large: {memory_capped_chunk_size}"))?;
    let max_batch_charge =
        memory_capped_chunk_size.saturating_mul(mem_overhead_per_input_byte as usize);
    let read_queue_cap = mem_cap
        .checked_div(max_batch_charge.max(1))
        .unwrap_or(1)
        .max(1);
    // Cap chunks in flight at `total_threads * 2` (same heuristic getraw uses) so the
    // compressor never starves and the writer's reorder buffer stays bounded.
    let inflight_cap = total_threads_usize.saturating_mul(2).max(2);
    debug!(
        align_threads,
        worker_pool_threads = worker_pool.current_num_threads(),
        compression_task_cap = inflight_cap,
        index_loaded_rss = ?index_loaded_rss,
        memory_headroom = %memory_headroom,
        requested_mem_cap_bytes = requested_mem_cap,
        mem_cap_bytes = mem_cap,
        mem_cap = %ByteSize(mem_cap as u64),
        stock_chunk_size,
        chunk_size,
        max_batch_charge,
        inflight_cap,
        read_queue_cap,
        sam_batch_queue_cap = 1,
        encode_chunk_records = ENCODE_CHUNK_RECORDS,
        mem_overhead_per_input_byte,
        "BWAMEM2 stock-driver BAM: budgets"
    );

    let mem_limiter = Arc::new(ReadMemoryLimiter::new(mem_cap));
    let inflight_limiter = Arc::new(InFlightLimiter::new(inflight_cap));

    // ---------- Channels ----------
    // q1: reader → aligner. Capacity is memory-driven: the channel can hold as many full
    // batches as `mem_cap` can permit, so read-ahead stops on the memory quota instead of an
    // arbitrary queue depth.
    let (q1_tx, q1_rx) = channel::bounded::<AlignBatch>(read_queue_cap);
    // q2: aligner → chunker. Keep this small; memory permits bound total data and this lets
    // the BWA thread start a following batch while the previous batch is split/encoded, without
    // allowing multiple completed full-SAM batches to pile up.
    let (q2_tx, q2_rx) = channel::bounded::<SamBatch>(1);
    // q3: chunker → compression dispatcher. Bounded by inflight_limiter; channel cap matches.
    let (q3_tx, q3_rx) = channel::bounded::<ChunkWork>(inflight_cap);
    // q4: compression tasks → writer. This must not block compressor workers:
    // the writer emits chunks in source order, so a bounded queue can fill with
    // later chunks while the missing prefix chunk is still waiting to run. The
    // in-flight permits carried by CompressedChunk already cap count and memory.
    let (q4_tx, q4_rx) = channel::unbounded::<CompressedChunk>();

    // ---------- Reader thread ----------
    let path_in_buf = path_in.to_path_buf();
    let mem_limiter_reader = Arc::clone(&mem_limiter);
    let reader_handle: JoinHandle<Result<u64>> = thread::Builder::new()
        .name("BWAMEM2StockReader".to_owned())
        .spawn(move || {
            bam_reader_loop(
                path_in_buf,
                chunk_size,
                mem_overhead_per_input_byte,
                mem_limiter_reader,
                q1_tx,
            )
        })
        .expect("spawn reader");

    // ---------- Aligner thread (owns state.worker exclusively) ----------
    // We move state into the closure as a raw pointer so the worker (which is `!Send` due to
    // its FFI fields) can be borrowed mutably across the pipeline. Safe because the aligner
    // thread is joined before this function returns, so the borrow lifetime is bounded.
    let header_aligner = Arc::clone(&header);
    // SAFETY: state is borrowed exclusively by the aligner thread which is joined before
    // returning. We use a raw pointer to dodge the !Send bound on worker_t (which contains
    // FFI types). No other thread touches state during the pipeline.
    let state_ptr = state as *mut StockDriverState as usize;
    let aligner_pool = Arc::clone(&worker_pool);
    let aligner_handle: JoinHandle<Result<u64>> = thread::Builder::new()
        .name("BWAMEM2StockAligner".to_owned())
        .spawn(move || -> Result<u64> {
            // SAFETY: see comment above where state_ptr is captured.
            let state = unsafe { &mut *(state_ptr as *mut StockDriverState) };
            bam_aligner_loop(state, q1_rx, q2_tx, header_aligner, aligner_pool)
        })
        .expect("spawn aligner");

    // ---------- Chunker thread ----------
    // Keep compressor backpressure out of the BWA aligner. The upstream pipeline also lets the
    // compute stage hand a completed batch off and start the next batch before output work is
    // fully drained.
    let chunker_handle: JoinHandle<Result<u64>> = thread::Builder::new()
        .name("BWAMEM2StockChunker".to_owned())
        .spawn(move || bam_chunker_loop(q2_rx, q3_tx, inflight_limiter))
        .expect("spawn chunker");

    // ---------- Compressor dispatcher ----------
    // Encode/compress tasks run on the same fixed-size Rayon pool as BWA's internal parallel
    // regions. This caps runnable CPU workers at `--threads` and lets compression fill BWA's
    // serial/barrier gaps without reserving dedicated compressor cores.
    let compressor_pool = Arc::clone(&worker_pool);
    let compressor_handle: JoinHandle<Result<()>> = thread::Builder::new()
        .name("BWAMEM2StockCompressor".to_owned())
        .spawn(move || bam_compressor_dispatch_loop(q3_rx, q4_tx, header, compressor_pool))
        .expect("spawn compressor dispatcher");
    // ---------- Writer thread (reorders + writes BGZF bytes serially) ----------
    let writer_handle: JoinHandle<Result<u64>> = thread::Builder::new()
        .name("BWAMEM2StockWriter".to_owned())
        .spawn(move || bam_writer_loop(q4_rx, out_file))
        .expect("spawn writer");

    // ---------- Wait for completion (in pipeline order so panics propagate cleanly) ----------
    let n_input_batches = reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("reader panicked"))?
        .context("reader failed")?;
    let n_aligned_batches = aligner_handle
        .join()
        .map_err(|_| anyhow::anyhow!("aligner panicked"))?
        .context("aligner failed")?;
    let n_chunked_batches = chunker_handle
        .join()
        .map_err(|_| anyhow::anyhow!("chunker panicked"))?
        .context("chunker failed")?;
    let compressor_result = compressor_handle
        .join()
        .map_err(|_| anyhow::anyhow!("compressor dispatcher panicked"))?
        .context("compressor dispatcher failed");
    let n_written_bytes = writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("writer panicked"))?
        .context("writer failed")?;
    compressor_result?;

    publish_atomic_output(&out_tmp, &out_path_unsorted.to_path_buf())?;
    info!(
        n_input_batches,
        n_aligned_batches,
        n_chunked_batches,
        n_written_bytes,
        n_reads_processed = state.n_processed,
        "BWAMEM2 stock-driver BAM: done"
    );
    Ok(())
}

fn bam_reader_loop(
    path_in: PathBuf,
    chunk_size: i64,
    mem_overhead_per_input_byte: u64,
    mem_limiter: Arc<ReadMemoryLimiter>,
    tx: channel::Sender<AlignBatch>,
) -> Result<u64> {
    let decoder = codec::BBGZDecoder::builder().with_path(&path_in).build();
    let mut tirp_lines = BufReader::with_capacity(1 << 20, DecodeRead::new(decoder));
    let mut line_buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut name_buf = String::new();
    let mut num_read_counter: u64 = 0;
    let mut batch_idx: u64 = 0;
    let mut eof = false;

    loop {
        // Acquire before constructing the batch. Otherwise a full next batch can be allocated
        // outside the limiter while previous batches are still in flight.
        let max_batch_charge =
            (chunk_size as usize).saturating_mul(mem_overhead_per_input_byte as usize);
        let permit = mem_limiter.acquire(max_batch_charge);
        let mut seqs: Vec<bseq1_t> = Vec::new();
        let mut size = 0_i64;
        while !eof && size < chunk_size {
            line_buf.clear();
            let n = tirp_lines
                .read_until(b'\n', &mut line_buf)
                .context("read TIRP line")?;
            if n == 0 {
                eof = true;
                break;
            }
            while matches!(line_buf.last(), Some(b'\n') | Some(b'\r')) {
                line_buf.pop();
            }
            if line_buf.is_empty() {
                continue;
            }
            let fields = parse_tirp_line(&line_buf)
                .with_context(|| format!("malformed TIRP line at record {num_read_counter}"))?;

            name_buf.clear();
            write_bascet_read_name(&mut name_buf, fields.id, fields.umi, num_read_counter);

            let id_r1 = i32::try_from(seqs.len()).expect("id fits in i32");
            let r1 = make_bseq(id_r1, &name_buf, fields.r1, fields.q1)?;
            size += i64::from(r1.l_seq);
            seqs.push(r1);

            let id_r2 = i32::try_from(seqs.len()).expect("id fits in i32");
            let r2 = make_bseq(id_r2, &name_buf, fields.r2, fields.q2)?;
            size += i64::from(r2.l_seq);
            seqs.push(r2);

            num_read_counter += 1;
        }
        if seqs.is_empty() {
            break;
        }

        if tx
            .send(AlignBatch {
                batch_idx,
                seqs,
                permit,
            })
            .is_err()
        {
            // Aligner closed the channel — pipeline is shutting down (probably due to error).
            break;
        }
        batch_idx += 1;
    }

    Ok(batch_idx)
}

fn bam_aligner_loop(
    state: &mut StockDriverState,
    rx: channel::Receiver<AlignBatch>,
    tx: channel::Sender<SamBatch>,
    _header: Arc<sam::Header>,
    worker_pool: Arc<rayon::ThreadPool>,
) -> Result<u64> {
    let mut n_aligned: u64 = 0;
    while let Ok(batch) = rx.recv() {
        let AlignBatch {
            batch_idx,
            mut seqs,
            permit,
        } = batch;

        let n_seqs = seqs.len();
        debug!(
            batch_idx,
            n_seqs,
            n_processed_so_far = %comma(state.n_processed),
            "BWAMEM2 stock-driver BAM: aligning batch"
        );

        let mut opt_clone = state.opt.clone();
        let pes0 = None;
        let sam_lines = process_batch_into_sam_lines(
            &mut seqs,
            &mut opt_clone,
            state.n_processed,
            pes0,
            &mut state.worker,
            &worker_pool,
        );
        state.n_processed += n_seqs as i64;

        if tx
            .send(SamBatch {
                batch_idx,
                blocks: sam_lines,
                permit,
            })
            .is_err()
        {
            return Ok(n_aligned);
        }

        n_aligned += 1;
        if batch_idx % 8 == 0 {
            info!(reads_m = state.n_processed / 1_000_000, "BWAMEM2 aligned");
        }
    }
    Ok(n_aligned)
}

fn bam_chunker_loop(
    rx: channel::Receiver<SamBatch>,
    tx: channel::Sender<ChunkWork>,
    inflight: Arc<InFlightLimiter>,
) -> Result<u64> {
    let mut n_chunked: u64 = 0;
    while let Ok(batch) = rx.recv() {
        let SamBatch {
            batch_idx,
            blocks,
            permit,
        } = batch;

        // Move the per-batch memory permit into an Arc so all chunks share it. Memory will
        // only be released when the writer drops the last chunk of this batch.
        let batch_permit = Arc::new(permit);

        // Share the entire batch's blocks across all compressor work items via Arc + range,
        // avoiding any per-line allocation on the aligner's hot path.
        let blocks = Arc::new(blocks);
        let total_blocks = blocks.len();
        let chunk_count = if total_blocks == 0 {
            1
        } else {
            (total_blocks + BLOCKS_PER_CHUNK - 1) / BLOCKS_PER_CHUNK
        };
        let total_chunks =
            u32::try_from(chunk_count).map_err(|_| anyhow::anyhow!("too many chunks for u32"))?;

        if total_blocks == 0 {
            // Empty batch (rare). Send a zero-range chunk so writer's batch counter advances.
            if tx
                .send(ChunkWork {
                    batch_idx,
                    chunk_idx: 0,
                    total_chunks: 1,
                    blocks: Arc::clone(&blocks),
                    block_start: 0,
                    block_end: 0,
                    _batch_permit: Arc::clone(&batch_permit),
                    _inflight: inflight.acquire(),
                })
                .is_err()
            {
                return Ok(n_chunked);
            }
        } else {
            for chunk_idx in 0..chunk_count {
                let block_start = chunk_idx * BLOCKS_PER_CHUNK;
                let block_end = (block_start + BLOCKS_PER_CHUNK).min(total_blocks);
                let permit_slot = inflight.acquire();
                if tx
                    .send(ChunkWork {
                        batch_idx,
                        chunk_idx: u32::try_from(chunk_idx).expect("chunk_idx fits in u32"),
                        total_chunks,
                        blocks: Arc::clone(&blocks),
                        block_start,
                        block_end,
                        _batch_permit: Arc::clone(&batch_permit),
                        _inflight: permit_slot,
                    })
                    .is_err()
                {
                    return Ok(n_chunked);
                }
            }
        }
        n_chunked += 1;
    }
    Ok(n_chunked)
}

fn bam_compressor_dispatch_loop(
    rx: channel::Receiver<ChunkWork>,
    tx: channel::Sender<CompressedChunk>,
    header: Arc<sam::Header>,
    worker_pool: Arc<rayon::ThreadPool>,
) -> Result<()> {
    let (err_tx, err_rx) = channel::unbounded::<anyhow::Error>();
    let pending = Arc::new((Mutex::new(0_usize), Condvar::new()));
    while let Ok(work) = rx.recv() {
        {
            let mut n = pending.0.lock().expect("compressor pending lock");
            *n += 1;
        }
        let tx = tx.clone();
        let header = Arc::clone(&header);
        let err_tx = err_tx.clone();
        let pending_task = Arc::clone(&pending);
        worker_pool.spawn(move || {
            if let Err(err) = bam_compress_work(work, tx, header) {
                let _ = err_tx.send(err);
            }
            let mut n = pending_task.0.lock().expect("compressor pending lock");
            *n = n.saturating_sub(1);
            if *n == 0 {
                pending_task.1.notify_one();
            }
        });
    }
    let mut n = pending.0.lock().expect("compressor pending lock");
    while *n > 0 {
        n = pending.1.wait(n).expect("compressor pending wait");
    }
    drop(err_tx);
    if let Ok(err) = err_rx.try_recv() {
        return Err(err);
    }
    Ok(())
}

fn bam_compress_work(
    work: ChunkWork,
    tx: channel::Sender<CompressedChunk>,
    header: Arc<sam::Header>,
) -> Result<()> {
    let ChunkWork {
        batch_idx,
        chunk_idx,
        total_chunks,
        blocks,
        block_start,
        block_end,
        _batch_permit,
        _inflight,
    } = work;

    // Parse SAM lines → RecordBuf with CB:Z / UB:Z tags. We split each block on '\n' here
    // (cheap — the original Box<str> stays put; we just take string slices) instead of
    // pre-flattening on the aligner thread.
    let mut records: Vec<RecordBuf> = Vec::with_capacity((block_end - block_start) * 2);
    for block_idx in block_start..block_end {
        for line in blocks[block_idx].split('\n') {
            if line.is_empty() {
                continue;
            }
            if let Some(rec) = parse_sam_line_to_record(line, &header)? {
                records.push(rec);
            }
        }
    }

    // Encode → uncompressed BAM bytes.
    let encoded: Vec<u8> = {
        let buf: Vec<u8> = Vec::with_capacity(records.len().saturating_mul(256));
        let mut enc = bam::io::Writer::from(buf);
        for r in &records {
            enc.write_alignment_record(&header, r)
                .context("encode BAM record")?;
        }
        enc.into_inner()
    };
    drop(records);

    // Compress → BGZF bytes (no EOF marker).
    let compressed = bgzf_compress_chunk_no_eof(&encoded)?;
    drop(encoded);

    if tx
        .send(CompressedChunk {
            batch_idx,
            chunk_idx,
            total_chunks,
            bytes: compressed,
            _batch_permit,
            _inflight,
        })
        .is_err()
    {
        return Ok(());
    }
    Ok(())
}

fn bam_writer_loop(
    rx: channel::Receiver<CompressedChunk>,
    mut out_file: BufWriter<File>,
) -> Result<u64> {
    // Reorder buffer keyed by (batch_idx, chunk_idx). We emit chunks in source order. To know
    // when a batch is fully consumed (and its memory permit can drop), each chunk carries
    // total_chunks; when next_chunk_idx == total_chunks we advance to (batch_idx + 1, 0).
    let mut next_batch: u64 = 0;
    let mut next_chunk: u32 = 0;
    let mut current_total_chunks: Option<u32> = None;
    let mut buf: BTreeMap<(u64, u32), CompressedChunk> = BTreeMap::new();
    let mut bytes_written: u64 = 0;

    while let Ok(chunk) = rx.recv() {
        buf.insert((chunk.batch_idx, chunk.chunk_idx), chunk);
        // Drain any prefix that's now contiguous in (batch, chunk) order.
        loop {
            let key = (next_batch, next_chunk);
            let Some(c) = buf.remove(&key) else {
                break;
            };
            // Track total_chunks for the current batch (set on first chunk of the batch).
            if current_total_chunks.is_none() {
                current_total_chunks = Some(c.total_chunks);
            }
            out_file
                .write_all(&c.bytes)
                .context("append compressed chunk to output BAM")?;
            bytes_written = bytes_written.saturating_add(c.bytes.len() as u64);
            // Drop chunk → drops its Arc<batch_permit> + InFlightPermit clones.
            drop(c);
            next_chunk += 1;
            if let Some(total) = current_total_chunks {
                if next_chunk >= total {
                    next_batch += 1;
                    next_chunk = 0;
                    current_total_chunks = None;
                }
            }
        }
    }

    // Append the BGZF EOF marker exactly once.
    out_file
        .write_all(&BGZF_EOF_BLOCK)
        .context("write BGZF EOF marker")?;
    out_file.flush().context("flush output BAM file")?;
    Ok(bytes_written)
}
