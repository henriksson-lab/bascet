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

use super::align::{index_bam, sort_bam};
use super::align_output::{
    SamRecordSink, TaggedBamSamSink, TaggedBamWriter, create_tagged_bam_writer,
    finish_tagged_bam_writer, make_bascet_read_name,
};
use crate::utils::{atomic_temp_path, publish_atomic_output};

const MINIMAP2_BATCH_BASES: usize = 25_000_000;

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
    super::align::warn_if_index_disk_size_exceeds_memory(
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
    let sizeof_stream_buffer = super::align::stream_buffer_after_index_load(
        "minimap2",
        total_memory,
        sizeof_stream_buffer,
        sizeof_stream_arena,
        total_threads,
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
        Arc::clone(&pool),
    )?;
    finish_tagged_bam_writer(writer_bam)?;
    publish_atomic_output(&path_out_unsorted_tmp, path_out_unsorted)?;

    info!("Sorting BAM file");
    sort_bam(path_out_unsorted, path_out_sorted, path_temp, total_threads)
        .expect("Failed to sort output");

    info!("Indexing BAM file");
    index_bam(path_out_sorted.to_str().expect("error getting sorted path"))
        .expect("Failed to index output");

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
    rayon_pool: Arc<rayon::ThreadPool>,
) -> Result<()>
where
    P: AsRef<Path>,
{
    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .countof_threads(num_threads)
        .with_opt_rayon_pool(rayon_pool)
        .build();
    let parser = parse::Tirp::builder().build();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();

    let mut query = stream.query::<tirp::Record>();
    let mut batch = Vec::new();
    let mut batch_bases = 0_usize;
    let mut num_read = 0_u64;

    loop {
        match query.next_into::<tirp::Record>() {
            Ok(Some(record)) => {
                let record_id = *record.get_ref::<Id>();
                let record_r1 = *record.get_ref::<R1>();
                let record_q1 = *record.get_ref::<Q1>();
                let record_umi = *record.get_ref::<Umi>();
                let read_name = make_bascet_read_name(record_id, record_umi, num_read);
                let cell_id = std::str::from_utf8(record_id)
                    .with_context(|| format!("cell id is not UTF-8: {record_id:?}"))?
                    .to_owned();
                let umi = if record_umi.is_empty() {
                    None
                } else {
                    Some(
                        std::str::from_utf8(record_umi)
                            .with_context(|| format!("UMI is not UTF-8: {record_umi:?}"))?
                            .to_owned(),
                    )
                };

                batch_bases += record_r1.len();
                batch.push(OwnedMinimapRead {
                    name: read_name,
                    cell_id,
                    umi,
                    seq: record_r1.to_vec(),
                    qual: record_q1.to_vec(),
                });

                num_read += 1;
                if num_read % 1_000_000 == 0 {
                    info!("{}M reads aligned", num_read / 1_000_000);
                }
                if batch_bases >= MINIMAP2_BATCH_BASES {
                    flush_minimap2_batch(aligner, pool, header, writer_bam, &batch)?;
                    batch.clear();
                    batch_bases = 0;
                }
            }
            Ok(None) => break,
            Err(e) => panic!("{:?}", e),
        };
    }

    flush_minimap2_batch(aligner, pool, header, writer_bam, &batch)?;
    Ok(())
}

struct OwnedMinimapRead {
    name: String,
    cell_id: String,
    umi: Option<String>,
    seq: Vec<u8>,
    qual: Vec<u8>,
}

fn flush_minimap2_batch(
    aligner: &Aligner,
    pool: &rayon::ThreadPool,
    header: &sam::Header,
    writer: &mut TaggedBamWriter,
    batch: &[OwnedMinimapRead],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let sam_lines: Vec<Result<Vec<String>>> = pool.install(|| {
        batch
            .par_iter()
            .map(|read| {
                let mut sink = VecSamSink::default();
                emit_minimap2_sam_records(aligner, read, &mut sink)?;
                Ok(sink.lines)
            })
            .collect()
    });

    let mut sink = TaggedBamSamSink::new(writer, header, "minimap2");
    for (read, lines) in batch.iter().zip(sam_lines) {
        for line in lines? {
            sink.record_with_cell_umi(&line, &read.cell_id, read.umi.as_deref())?;
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
    read: &OwnedMinimapRead,
    sink: &mut impl SamRecordSink,
) -> Result<()> {
    let result = map::map_query(&aligner.idx, &aligner.map_opt, &read.name, &read.seq);

    if result.regs.is_empty() {
        if !aligner.map_opt.flag.contains(MapFlags::SAM_HIT_ONLY) {
            let line = minimap_sam::write_sam_record(
                &aligner.idx,
                &read.name,
                &read.seq,
                &read.qual,
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
                &read.name,
                &read.seq,
                &read.qual,
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
