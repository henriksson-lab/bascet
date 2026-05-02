use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Context, Result};
use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use noodles::{sam, sam::alignment::io::Write as _};
use rayon::prelude::*;
use tracing::info;

use super::align::{index_bam, sort_bam};
use super::align_output::{
    TaggedBamWriter, create_tagged_bam_writer, finish_tagged_bam_writer, make_bascet_read_name,
    parse_tagged_record_with_cell_umi,
};
use crate::utils::{atomic_temp_path, publish_atomic_output};

const BWA_MEM2_BATCH_BASES: usize = 10_000_000;
const BWA_MEM2_BATCH_QUEUE: usize = 2;

pub fn try_execute_bwa_mem2(
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
    total_memory: ByteSize,
    total_threads: u64,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> Result<()> {
    info!("Using direct bwa-mem2-rs aligner");
    let index_disk_size = validate_bwa_mem2_index(path_genome)?;
    super::align::warn_if_index_disk_size_exceeds_memory(
        "BWA",
        path_genome,
        index_disk_size,
        total_memory,
    );

    let mut aligner = bwa_mem2_rs::mem_api::MemAligner::builder(path_genome)
        .threads(align_threads)
        .thread_pool(Arc::clone(&rayon_pool))
        .build()
        .map_err(|e| anyhow::anyhow!(e))?;
    info!("BWA index loaded");
    let sizeof_stream_buffer = super::align::stream_buffer_after_index_load(
        "BWA",
        total_memory,
        sizeof_stream_buffer,
        sizeof_stream_arena,
        total_threads,
    );

    let batch_target_bases = BWA_MEM2_BATCH_BASES;
    info!(
        total_threads,
        align_threads,
        read_threads = numof_threads_read.get(),
        write_bam_threads = numof_threads_writebam,
        batch_target_bases,
        "Configured BWA alignment threading"
    );

    info!("Creating BWA SAM/BAM header");
    let sam_header = aligner.sam_header().map_err(|e| anyhow::anyhow!(e))?;
    let bam_header = sam_header.parse::<sam::Header>()?;

    let path_out_unsorted_tmp = atomic_temp_path(path_out_unsorted);
    info!(path = ?path_out_unsorted_tmp, "Creating unsorted BAM writer");
    let mut writer_bam =
        create_tagged_bam_writer(&path_out_unsorted_tmp, &bam_header, numof_threads_writebam)?;
    let output_pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(numof_threads_writebam.max(1))
            .thread_name(|index| format!("BWAOutput@{index}"))
            .build()?,
    );

    align_tirp_with_bwa_mem2(
        path_in,
        &mut aligner,
        &bam_header,
        &mut writer_bam,
        numof_threads_read,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        batch_target_bases,
        rayon_pool,
        output_pool,
    )?;
    finish_tagged_bam_writer(writer_bam)?;
    publish_atomic_output(&path_out_unsorted_tmp, path_out_unsorted)?;

    info!("Sorting BAM file");
    sort_bam(path_out_unsorted, path_out_sorted, path_temp, total_threads)
        .expect("Failed to sort output");

    info!("Indexing BAM file");
    index_bam(
        path_out_sorted
            .to_str()
            .expect("error getting unsorted path"),
    )
    .expect("Failed to index output");

    info!("All alignment steps complete");
    Ok(())
}

fn validate_bwa_mem2_index(index_prefix: &Path) -> Result<u64> {
    let required_suffixes = [".0123", ".amb", ".ann", ".bwt.2bit.64", ".pac"];
    let mut total_size = 0_u64;
    for suffix in required_suffixes {
        let path = PathBuf::from(format!("{}{}", index_prefix.display(), suffix));
        if !path.is_file() {
            anyhow::bail!(
                "BWA aligner requires an existing bwa-mem2 index prefix; missing required index file {path:?}. Build the index first, then pass the reference prefix with `--genome`."
            );
        }
        total_size = total_size.saturating_add(
            path.metadata()
                .with_context(|| format!("failed to stat BWA index file {path:?}"))?
                .len(),
        );
    }

    Ok(total_size)
}

fn align_tirp_with_bwa_mem2<P>(
    path_in: P,
    aligner: &mut bwa_mem2_rs::mem_api::MemAligner,
    header: &sam::Header,
    writer_bam: &mut TaggedBamWriter,
    num_threads: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    batch_target_bases: usize,
    rayon_pool: Arc<rayon::ThreadPool>,
    output_pool: Arc<rayon::ThreadPool>,
) -> Result<()>
where
    P: AsRef<Path>,
{
    info!("Starting TIRP stream into BWA");
    let (batch_rx, reader_handle) = spawn_bwa_batch_reader(
        path_in.as_ref().to_path_buf(),
        num_threads,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        batch_target_bases,
        rayon_pool,
    );

    let mut total_read_pairs = 0_u64;
    loop {
        let read_wait_start = std::time::Instant::now();
        let batch = match batch_rx.recv() {
            Ok(batch) => batch,
            Err(_) => break,
        };
        let read_wait = read_wait_start.elapsed();
        let batch = batch?;
        total_read_pairs = batch.read_pairs_seen;
        info!(
            read_pairs = total_read_pairs,
            batch_pairs = batch.pairs.len(),
            batch_bases = batch.bases,
            "Aligning BWA input batch"
        );
        flush_bwa_mem2_batch(
            aligner,
            header,
            writer_bam,
            &batch.pairs,
            Arc::clone(&output_pool),
            read_wait,
        )?;
        info!(
            read_pairs = total_read_pairs,
            batch_pairs = batch.pairs.len(),
            "Finished BWA input batch"
        );
    }

    join_bwa_reader(reader_handle)?;
    info!(read_pairs = total_read_pairs, "Finished BWA TIRP stream");
    Ok(())
}

fn spawn_bwa_batch_reader(
    path_in: PathBuf,
    num_threads: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    batch_target_bases: usize,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> (
    crossbeam::channel::Receiver<Result<BwaReadBatch>>,
    JoinHandle<Result<()>>,
) {
    let (batch_tx, batch_rx) = crossbeam::channel::bounded(BWA_MEM2_BATCH_QUEUE);
    let reader_handle = thread::Builder::new()
        .name("BWAReadProducer".to_owned())
        .spawn(move || {
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
            let mut batch = BwaReadBatch::default();
            let mut num_read = 0_u64;

            loop {
                match query.next_into::<tirp::Record>() {
                    Ok(Some(record)) => {
                        let record_id = *record.get_ref::<Id>();
                        let record_r1 = *record.get_ref::<R1>();
                        let record_r2 = *record.get_ref::<R2>();
                        let record_q1 = *record.get_ref::<Q1>();
                        let record_q2 = *record.get_ref::<Q2>();
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

                        batch.bases += record_r1.len() + record_r2.len();
                        batch.pairs.push(OwnedBwaReadPair {
                            name: read_name,
                            cell_id,
                            umi,
                            r1: record_r1.to_vec(),
                            q1: record_q1.to_vec(),
                            r2: record_r2.to_vec(),
                            q2: record_q2.to_vec(),
                        });

                        num_read += 1;
                        batch.read_pairs_seen = num_read;
                        if num_read % 10_000 == 0 {
                            info!(
                                read_pairs = num_read,
                                batch_pairs = batch.pairs.len(),
                                batch_bases = batch.bases,
                                batch_target_bases,
                                "Reading BWA input batch"
                            );
                        }
                        if num_read % 1_000_000 == 0 {
                            info!("{}M read pairs read for BWA", num_read / 1_000_000);
                        }
                        if batch.bases >= batch_target_bases {
                            send_bwa_batch(&batch_tx, &mut batch)?;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => return Err(anyhow::anyhow!("{e:?}")),
                };
            }

            send_bwa_batch(&batch_tx, &mut batch)?;
            Ok(())
        })
        .expect("failed to spawn BWA read producer");

    (batch_rx, reader_handle)
}

fn send_bwa_batch(
    batch_tx: &crossbeam::channel::Sender<Result<BwaReadBatch>>,
    batch: &mut BwaReadBatch,
) -> Result<()> {
    if batch.pairs.is_empty() {
        return Ok(());
    }

    let next_batch = BwaReadBatch {
        pairs: Vec::with_capacity(batch.pairs.len()),
        bases: 0,
        read_pairs_seen: batch.read_pairs_seen,
    };
    let ready_batch = std::mem::replace(batch, next_batch);
    batch_tx
        .send(Ok(ready_batch))
        .context("failed to queue BWA read batch")?;
    Ok(())
}

fn join_bwa_reader(reader_handle: JoinHandle<Result<()>>) -> Result<()> {
    reader_handle.join().map_err(|panic| {
        if let Some(message) = panic.downcast_ref::<&str>() {
            anyhow::anyhow!("BWA read producer panicked: {message}")
        } else if let Some(message) = panic.downcast_ref::<String>() {
            anyhow::anyhow!("BWA read producer panicked: {message}")
        } else {
            anyhow::anyhow!("BWA read producer panicked")
        }
    })?
}

#[derive(Default)]
struct BwaReadBatch {
    pairs: Vec<OwnedBwaReadPair>,
    bases: usize,
    read_pairs_seen: u64,
}

struct OwnedBwaReadPair {
    name: String,
    cell_id: String,
    umi: Option<String>,
    r1: Vec<u8>,
    q1: Vec<u8>,
    r2: Vec<u8>,
    q2: Vec<u8>,
}

fn flush_bwa_mem2_batch(
    aligner: &mut bwa_mem2_rs::mem_api::MemAligner,
    header: &sam::Header,
    writer: &mut TaggedBamWriter,
    batch: &[OwnedBwaReadPair],
    rayon_pool: Arc<rayon::ThreadPool>,
    read_wait: Duration,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let timing = std::env::var_os("BASCET_BWA_TIMINGS").is_some();
    let t0 = std::time::Instant::now();
    let pairs: Vec<_> = batch
        .iter()
        .map(|pair| bwa_mem2_rs::mem_api::MemReadPair {
            name: pair.name.clone(),
            r1: &pair.r1,
            q1: &pair.q1,
            r2: &pair.r2,
            q2: &pair.q2,
        })
        .collect();
    let t_pairs = std::time::Instant::now();

    let mut sam_lines = Vec::new();
    aligner
        .align_pairs_into_indexed(&pairs, |pair_index, line| {
            if pair_index >= batch.len() {
                return Err(format!("BWA returned invalid pair index {pair_index}"));
            }
            sam_lines.push(IndexedBwaSamLine {
                pair_index,
                line: line.trim_end_matches('\n').to_owned(),
            });
            Ok(())
        })
        .map_err(anyhow::Error::msg)?;
    let t_align = std::time::Instant::now();

    let records: Vec<_> = rayon_pool.install(|| {
        sam_lines
            .par_iter()
            .map(|sam_line| {
                let pair = &batch[sam_line.pair_index];
                let record = parse_tagged_record_with_cell_umi(
                    &sam_line.line,
                    header,
                    "BWA",
                    &pair.cell_id,
                    pair.umi.as_deref(),
                )?;
                Ok(record)
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let t_parse = std::time::Instant::now();

    for record in &records {
        writer.write_alignment_record(header, record)?;
    }
    let t_write = std::time::Instant::now();

    if timing {
        info!(
            batch_pairs = batch.len(),
            sam_records = records.len(),
            read_wait_s = read_wait.as_secs_f64(),
            make_pairs_s = (t_pairs - t0).as_secs_f64(),
            align_s = (t_align - t_pairs).as_secs_f64(),
            parse_tag_s = (t_parse - t_align).as_secs_f64(),
            write_s = (t_write - t_parse).as_secs_f64(),
            total_s = (t_write - t0).as_secs_f64(),
            "BWA batch timing"
        );
    }
    Ok(())
}

struct IndexedBwaSamLine {
    pair_index: usize,
    line: String,
}
