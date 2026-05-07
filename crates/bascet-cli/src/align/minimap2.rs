use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use minimap2::{aligner::Aligner, flags::MapFlags, format::sam as minimap_sam, index, map};
use noodles::sam;
use rayon::prelude::*;
use tracing::info;

use super::output::{
    SamRecordSink, TaggedBamSamSink, TaggedBamWriter, create_tagged_bam_writer,
    finish_tagged_bam_writer,
};
use crate::command::{bamsort::sort_and_index_bam, samtools_rs::sort::ReferenceOrder};
use crate::utils::{atomic_temp_path, publish_atomic_output};

// Outer batch scales with thread count so each parallel mapping scope amortizes spawn/join +
// serial SAM/BAM write between scopes — same rationale as the bwa-mem2 path.
const MINIMAP2_BASES_PER_THREAD: usize = 10_000_000;
// Absolute hard cap on per-batch base count, regardless of memory budget. The adaptive cap
// (see `aligner_batch_bases_cap`) usually picks a smaller value on memory-constrained runs.
const MINIMAP2_BATCH_BASES_MAX: usize = 128 * 1024 * 1024;
// Empirical resident-bytes-per-input-base for minimap2. Less per-thread state than bwa-mem2 (no
// equivalent of `worker_t.mmc`), but per-read mapping intermediates still scale with batch size.
// Conservative pick; halve if profiling shows headroom.
const MINIMAP2_RSS_BYTES_PER_INPUT_BYTE: u64 = 200;

pub fn try_execute_minimap2(
    path_in: &Path,
    path_genome: &Path,
    path_out_unsorted: &PathBuf,
    path_out_sorted: &PathBuf,
    path_temp: &PathBuf,
    numof_threads_writebam: usize,
    align_threads: usize,
    numof_threads_read: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    preset: &str,
    total_memory: ByteSize,
    total_threads: u64,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> Result<()> {
    info!(
        preset,
        total_threads,
        align_threads,
        read_threads = numof_threads_read.get(),
        write_bam_threads = numof_threads_writebam,
        "Using direct minimap2-rs aligner"
    );

    let genome_path = path_genome
        .to_str()
        .with_context(|| format!("minimap2 genome path is not UTF-8: {path_genome:?}"))?;
    anyhow::ensure!(
        index::io::is_idx_file(genome_path)
            .with_context(|| format!("failed to read minimap2 index path: {path_genome:?}"))?,
        "minimap2 aligner requires an existing .mmi index; got {path_genome:?}. Build one first, for example with `bascet exttool minimap2 -d <ref.mmi> <ref.fa>`, then pass `--genome <ref.mmi>`."
    );
    let index_disk_size = path_genome
        .metadata()
        .with_context(|| format!("failed to stat minimap2 index path: {path_genome:?}"))?
        .len();
    super::common::warn_if_index_disk_size_exceeds_memory(
        "minimap2",
        path_genome,
        index_disk_size,
        total_memory,
    );
    let mut aligner = Aligner::builder()
        .preset(preset)
        .index(genome_path)
        .with_cigar()
        .build()
        .map_err(anyhow::Error::msg)?;
    aligner.map_opt.flag |= MapFlags::OUT_SAM | MapFlags::CIGAR;
    info!("minimap2 index loaded");
    let sizeof_stream_buffer = super::stream_helpers::stream_buffer_after_index_load(
        "minimap2",
        total_memory,
        sizeof_stream_buffer,
        sizeof_stream_arena,
    );

    let sam_header = minimap_sam::write_sam_hdr(&aligner.idx, None, &[]);
    let bam_header = sam_header.parse::<sam::Header>()?;
    let path_out_unsorted_tmp = atomic_temp_path(path_out_unsorted);
    let mut writer_bam =
        create_tagged_bam_writer(&path_out_unsorted_tmp, &bam_header, numof_threads_writebam)?;

    let pool = rayon_pool;

    align_tirp_with_minimap2(
        path_in,
        &aligner,
        &pool,
        &bam_header,
        &mut writer_bam,
        numof_threads_read,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        align_threads,
        total_memory,
        Arc::clone(&pool),
    )?;
    finish_tagged_bam_writer(writer_bam)?;
    publish_atomic_output(&path_out_unsorted_tmp, path_out_unsorted)?;

    info!("Sorting + indexing BAM file (in-process)");
    sort_and_index_bam(
        path_out_unsorted,
        path_out_sorted,
        path_temp,
        total_memory,
        total_threads as usize,
        ReferenceOrder::Lexicographic,
    )?;

    info!("All alignment steps complete");
    Ok(())
}

fn align_tirp_with_minimap2<P>(
    path_in: P,
    aligner: &Aligner,
    pool: &rayon::ThreadPool,
    header: &sam::Header,
    writer_bam: &mut TaggedBamWriter,
    num_threads: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    align_threads: usize,
    total_memory: ByteSize,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> Result<()>
where
    P: AsRef<Path>,
{
    // Don't share the alignment rayon pool with BBGZDecoder; see align_bwa.rs comment.
    let _ = rayon_pool;
    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .countof_threads(num_threads)
        .build();
    let parser = parse::Tirp::builder().build();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();

    let mut query = stream.query::<tirp::Record>();
    let mut batch = MinimapReadBatch::default();
    let memory_cap = super::stream_helpers::aligner_batch_bases_cap(
        "minimap2",
        total_memory,
        MINIMAP2_RSS_BYTES_PER_INPUT_BYTE,
        MINIMAP2_BATCH_BASES_MAX,
    );
    let uncapped_batch_target_bases =
        MINIMAP2_BASES_PER_THREAD.saturating_mul(align_threads.max(1));
    let batch_target_bases = uncapped_batch_target_bases.min(memory_cap);
    if batch_target_bases < uncapped_batch_target_bases {
        info!(
            align_threads,
            per_thread_bases = MINIMAP2_BASES_PER_THREAD,
            uncapped_batch_target_bases,
            batch_target_bases,
            memory_cap,
            absolute_cap = MINIMAP2_BATCH_BASES_MAX,
            "Capping minimap2 outer batch to limit per-batch RAM"
        );
    }
    let mut num_read = 0_u64;
    let mut name_buf = String::new();

    loop {
        match query.next_into::<tirp::Record>() {
            Ok(Some(record)) => {
                let record_id = *record.get_ref::<Id>();
                let record_r1 = *record.get_ref::<R1>();
                let record_q1 = *record.get_ref::<Q1>();
                let record_umi = *record.get_ref::<Umi>();

                name_buf.clear();
                write_bascet_read_name(&mut name_buf, record_id, record_umi, num_read);
                batch.push(
                    name_buf.as_bytes(),
                    record_id,
                    record_umi,
                    record_r1,
                    record_q1,
                )?;

                num_read += 1;
                if num_read % 1_000_000 == 0 {
                    info!("{}M reads aligned", num_read / 1_000_000);
                }
                if batch.bases >= batch_target_bases {
                    flush_minimap2_batch(aligner, pool, header, writer_bam, &batch)?;
                    batch.clear();
                }
            }
            Ok(None) => break,
            Err(e) => panic!("{:?}", e),
        };
    }

    flush_minimap2_batch(aligner, pool, header, writer_bam, &batch)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct MinimapReadSlices {
    name_off: u32,
    name_len: u32,
    cell_off: u32,
    cell_len: u32,
    umi_off: u32,
    umi_len: u32,
    seq_off: u32,
    seq_len: u32,
    qual_off: u32,
    qual_len: u32,
}

struct MinimapReadView<'a> {
    name: &'a str,
    cell_id: &'a str,
    umi: Option<&'a str>,
    seq: &'a [u8],
    qual: &'a [u8],
}

#[derive(Default)]
struct MinimapReadBatch {
    arena: Vec<u8>,
    reads: Vec<MinimapReadSlices>,
    bases: usize,
}

impl MinimapReadBatch {
    fn push(
        &mut self,
        name: &[u8],
        cell_id: &[u8],
        umi: &[u8],
        seq: &[u8],
        qual: &[u8],
    ) -> Result<()> {
        std::str::from_utf8(name).context("minimap2 read name is not UTF-8")?;
        std::str::from_utf8(cell_id)
            .with_context(|| format!("cell id is not UTF-8: {cell_id:?}"))?;
        if !umi.is_empty() {
            std::str::from_utf8(umi).with_context(|| format!("UMI is not UTF-8: {umi:?}"))?;
        }

        let (name_off, name_len) = self.push_bytes(name)?;
        let (cell_off, cell_len) = self.push_bytes(cell_id)?;
        let (umi_off, umi_len) = self.push_bytes(umi)?;
        let (seq_off, seq_len) = self.push_bytes(seq)?;
        let (qual_off, qual_len) = self.push_bytes(qual)?;

        self.reads.push(MinimapReadSlices {
            name_off,
            name_len,
            cell_off,
            cell_len,
            umi_off,
            umi_len,
            seq_off,
            seq_len,
            qual_off,
            qual_len,
        });
        self.bases += seq.len();
        Ok(())
    }

    fn push_bytes(&mut self, src: &[u8]) -> Result<(u32, u32)> {
        let off = u32::try_from(self.arena.len())
            .map_err(|_| anyhow::anyhow!("minimap2 read batch arena exceeded 4 GiB"))?;
        let len = u32::try_from(src.len())
            .map_err(|_| anyhow::anyhow!("minimap2 read batch field exceeded 4 GiB"))?;
        self.arena.extend_from_slice(src);
        Ok((off, len))
    }

    fn clear(&mut self) {
        self.arena.clear();
        self.reads.clear();
        self.bases = 0;
    }

    fn view(&self, i: usize) -> MinimapReadView<'_> {
        let r = &self.reads[i];
        let bytes = &self.arena;
        // SAFETY: name/cell_id/umi were validated as UTF-8 in `push`; arena is append-only.
        unsafe {
            let name = std::str::from_utf8_unchecked(slice(bytes, r.name_off, r.name_len));
            let cell_id = std::str::from_utf8_unchecked(slice(bytes, r.cell_off, r.cell_len));
            let umi = if r.umi_len == 0 {
                None
            } else {
                Some(std::str::from_utf8_unchecked(slice(
                    bytes, r.umi_off, r.umi_len,
                )))
            };
            MinimapReadView {
                name,
                cell_id,
                umi,
                seq: slice(bytes, r.seq_off, r.seq_len),
                qual: slice(bytes, r.qual_off, r.qual_len),
            }
        }
    }
}

#[inline]
fn slice(bytes: &[u8], off: u32, len: u32) -> &[u8] {
    &bytes[off as usize..(off as usize + len as usize)]
}

fn write_bascet_read_name(dst: &mut String, record_id: &[u8], record_umi: &[u8], num_read: u64) {
    use std::fmt::Write;
    dst.reserve(record_id.len() + record_umi.len() + 24);
    dst.push_str(&String::from_utf8_lossy(record_id));
    dst.push(':');
    dst.push_str(&String::from_utf8_lossy(record_umi));
    dst.push(':');
    let _ = write!(dst, "{num_read}");
}

fn flush_minimap2_batch(
    aligner: &Aligner,
    pool: &rayon::ThreadPool,
    header: &sam::Header,
    writer: &mut TaggedBamWriter,
    batch: &MinimapReadBatch,
) -> Result<()> {
    if batch.reads.is_empty() {
        return Ok(());
    }

    let sam_lines: Vec<Result<Vec<String>>> = pool.install(|| {
        (0..batch.reads.len())
            .into_par_iter()
            .map(|i| {
                let v = batch.view(i);
                let mut sink = VecSamSink::default();
                emit_minimap2_sam_records(aligner, &v, &mut sink)?;
                Ok(sink.lines)
            })
            .collect()
    });

    let mut sink = TaggedBamSamSink::new(writer, header, "minimap2");
    for (i, lines) in sam_lines.into_iter().enumerate() {
        let v = batch.view(i);
        for line in lines? {
            sink.record_with_cell_umi(&line, v.cell_id, v.umi)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct VecSamSink {
    lines: Vec<String>,
}

impl SamRecordSink for VecSamSink {
    fn record(&mut self, line: &str) -> Result<()> {
        self.lines.push(line.to_owned());
        Ok(())
    }
}

fn emit_minimap2_sam_records(
    aligner: &Aligner,
    read: &MinimapReadView<'_>,
    sink: &mut impl SamRecordSink,
) -> Result<()> {
    let result = map::map_query(&aligner.idx, &aligner.map_opt, read.name, read.seq);

    if result.regs.is_empty() {
        if !aligner.map_opt.flag.contains(MapFlags::SAM_HIT_ONLY) {
            let line = minimap_sam::write_sam_record(
                &aligner.idx,
                read.name,
                read.seq,
                read.qual,
                None,
                0,
                &[],
                aligner.map_opt.flag,
                result.rep_len,
            );
            sink.record(&line)?;
        }
    } else {
        for region in result.regs.iter().filter(|region| {
            !(aligner.map_opt.flag.contains(MapFlags::NO_PRINT_2ND) && region.id != region.parent)
        }) {
            let line = minimap_sam::write_sam_record(
                &aligner.idx,
                read.name,
                read.seq,
                read.qual,
                Some(region),
                result.regs.len(),
                &result.regs,
                aligner.map_opt.flag,
                result.rep_len,
            );
            sink.record(&line)?;
        }
    }
    Ok(())
}
