use std::{
    collections::BTreeMap,
    fs::{self},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Instant,
};

use anyhow::{Context, Result};
use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use noodles::{bam, sam, sam::alignment::io::Write as _};
use tracing::{debug, info};

use super::output::{
    create_tagged_bam_writer, finish_tagged_bam_writer, make_bascet_read_name, parse_tagged_record,
};
use crate::command::{
    bamsort::sort_and_index_encoded_bam_chunk_receiver,
    samtools_rs::sort::{EncodedBamChunk, ReferenceOrder},
};
use crate::utils::{atomic_temp_path, atomic_temp_path_in_dir, publish_atomic_output};
use star_rs::{
    ReadAlignChunkMapChunkResult, ReadAlignChunkProcessChunksResult, Stats,
    direct::{
        DirectStarContext, DirectStarMappedChunk, DirectStarWorker, StarReadChunk, StarReadMate,
        StarReadPair,
    },
    stats_l4_stats_resetn, stats_l21_stats_addstats,
};

const STAR_READ_PAIRS_PER_CHUNK: usize = 10_000;
const STAR_OUTPUT_BUDGET_SAMPLE_READ_PAIRS: u64 = 1_000;
const STAR_OUTPUT_RECORD_OVERHEAD_BYTES: u64 = 384;
const STAR_OUTPUT_PAYLOAD_SAFETY_NUMERATOR: u64 = 5;
const STAR_OUTPUT_PAYLOAD_SAFETY_DENOMINATOR: u64 = 4;
const STAR_WRITER_QUEUE_CHUNKS: usize = 8;
const STAR_WRITER_CONVERTER_THREADS: usize = 8;

pub fn try_execute_star_rs(
    path_in: &Path,
    path_genome: &Path,
    path_out_unsorted: &PathBuf,
    path_out_sorted: &PathBuf,
    path_temp: &PathBuf,
    numof_threads_writebam: usize,
    align_threads: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    total_memory: ByteSize,
    total_threads: u64,
    rayon_pool: Arc<rayon::ThreadPool>,
    max_read_pairs: Option<u64>,
) -> Result<()> {
    info!("Using direct star-rs aligner");
    let index_disk_size = validate_star_index_dir(path_genome)?;
    super::common::warn_if_index_disk_size_exceeds_memory(
        "STAR",
        path_genome,
        index_disk_size,
        total_memory,
    );
    info!(
        total_threads,
        star_threads = align_threads,
        write_bam_threads = numof_threads_writebam,
        "Configured STAR alignment threading"
    );

    fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create STAR temp directory {:?}", path_temp))?;

    let path_star_tmp = atomic_temp_path(&path_temp.join("star-rs-tmp"));
    fs::create_dir_all(&path_star_tmp)
        .with_context(|| format!("failed to create STAR work directory {:?}", path_star_tmp))?;

    let path_out_unsorted_tmp = atomic_temp_path_in_dir(path_out_unsorted, path_temp);
    let star_run = match run_star_rs(
        path_genome,
        path_in,
        &path_star_tmp,
        &path_out_unsorted_tmp,
        path_out_sorted,
        path_temp,
        numof_threads_writebam,
        align_threads,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        total_memory,
        total_threads,
        rayon_pool,
        max_read_pairs,
    ) {
        Ok(result) => result,
        Err(err) => {
            cleanup_star_temp(&path_star_tmp);
            let _ = fs::remove_file(&path_out_unsorted_tmp);
            return Err(err);
        }
    };
    publish_atomic_output(&path_out_unsorted_tmp, path_out_unsorted)?;

    cleanup_star_temp(&path_star_tmp);

    let star_output_budget =
        estimate_star_output_budget(star_run.output_records, star_run.read_sample);
    info!(
        output_records = star_run.output_records,
        sampled_read_pairs = star_output_budget.sampled_read_pairs,
        average_payload_bytes_per_output_record =
            star_output_budget.average_payload_bytes_per_output_record,
        star_output_bytes_per_record = star_output_budget.bytes_per_output_record,
        star_output_budget = %star_output_budget.bytes,
        "Estimated STAR output memory footprint"
    );

    info!("All alignment steps complete");
    Ok(())
}

fn validate_star_index_dir(genome_dir: &Path) -> Result<u64> {
    anyhow::ensure!(
        genome_dir.is_dir(),
        "STAR aligner requires an existing STAR genome directory; got {genome_dir:?}"
    );

    let required_files = [
        "genomeParameters.txt",
        "chrName.txt",
        "Genome",
        "SA",
        "SAindex",
    ];
    let mut total_size = 0_u64;
    for name in required_files {
        let path = genome_dir.join(name);
        if !path.is_file() {
            anyhow::bail!(
                "STAR aligner requires an existing STAR genome directory; missing required genome file {path:?}. Build the STAR genome first, then pass the genome directory with `--genome`."
            );
        }
        total_size = total_size.saturating_add(
            path.metadata()
                .with_context(|| format!("failed to stat STAR index file {path:?}"))?
                .len(),
        );
    }

    Ok(total_size)
}

fn collect_star_map_chunk_records(
    map_chunk: &ReadAlignChunkMapChunkResult,
    header: &sam::Header,
    writer: &mut bam::io::Writer<Vec<u8>>,
) -> Result<u64> {
    let mut records_collected = 0_u64;
    records_collected = records_collected.saturating_add(collect_star_sam_bytes(
        &map_chunk.direct_sam_output,
        header,
        writer,
    )?);
    records_collected = records_collected.saturating_add(collect_star_sam_bytes(
        &map_chunk.paired_keep_input_order_tmp,
        header,
        writer,
    )?);
    Ok(records_collected)
}

fn collect_star_sam_bytes(
    bytes: &[u8],
    header: &sam::Header,
    writer: &mut bam::io::Writer<Vec<u8>>,
) -> Result<u64> {
    let mut records_collected = 0_u64;
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() || line.starts_with(b"@") {
            continue;
        }
        let line = std::str::from_utf8(line).context("STAR SAM output is not UTF-8")?;
        let record = parse_tagged_record(line, header, "STAR")?;
        writer.write_alignment_record(header, &record)?;
        records_collected += 1;
    }
    Ok(records_collected)
}

fn spawn_star_chunk_collector(
    path: PathBuf,
    header: sam::Header,
    num_threads: usize,
    writer_rx: crossbeam_channel::Receiver<StarConvertedChunk>,
    sort_tx: crossbeam_channel::Sender<EncodedBamChunk>,
    metrics: Arc<StarWriterPipelineMetrics>,
) -> std::result::Result<JoinHandle<std::result::Result<u64, String>>, String> {
    std::thread::Builder::new()
        .name("STARChunkCollector".to_string())
        .spawn(move || {
            let mut bam_writer = create_tagged_bam_writer(&path, &header, num_threads)
                .map_err(|err| format!("failed to create STAR unsorted BAM writer: {err:?}"))?;
            let mut records_written = 0_u64;
            let mut pending = BTreeMap::<u32, StarConvertedChunk>::new();
            let mut next_write_chunk = 0_u32;
            while let Ok(converted) = writer_rx.recv() {
                pending.insert(converted.chunk_index, converted);
                while let Some(converted) = pending.remove(&next_write_chunk) {
                    let write_start = Instant::now();
                    {
                        use std::io::Write as _;
                        bam_writer
                            .get_mut()
                            .write_all(&converted.bam_records)
                            .map_err(|err| {
                                format!("failed to write STAR unsorted BAM chunk: {err:?}")
                            })?;
                    }
                    let records_written_in_chunk = converted.records_written;
                    sort_tx
                        .send(EncodedBamChunk {
                            chunk_idx: converted.chunk_index as usize,
                            data: converted.bam_records,
                            records: converted.records_written,
                        })
                        .map_err(|_| {
                            "STAR streamed BAM sort thread closed unexpectedly".to_string()
                        })?;
                    metrics.writer_write_nanos.fetch_add(
                        write_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                    metrics.writer_chunks.fetch_add(1, Ordering::Relaxed);
                    records_written = records_written.saturating_add(records_written_in_chunk);
                    next_write_chunk += 1;
                }
            }
            drop(sort_tx);
            finish_tagged_bam_writer(bam_writer)
                .map_err(|err| format!("failed to finish STAR unsorted BAM writer: {err:?}"))?;
            Ok(records_written)
        })
        .map_err(|err| format!("failed to spawn STAR chunk collector thread: {err}"))
}

fn spawn_star_converter_workers(
    header: sam::Header,
    converter_rx: crossbeam_channel::Receiver<StarWriterChunk>,
    writer_tx: crossbeam_channel::Sender<StarConvertedChunk>,
    worker_count: usize,
    metrics: Arc<StarWriterPipelineMetrics>,
) -> Vec<JoinHandle<std::result::Result<(), String>>> {
    let mut handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let header = header.clone();
        let converter_rx = converter_rx.clone();
        let writer_tx = writer_tx.clone();
        let metrics = Arc::clone(&metrics);
        let handle = std::thread::Builder::new()
            .name(format!("STARSamConvert@{worker_id}"))
            .spawn(move || {
                while let Ok(chunk) = converter_rx.recv() {
                    let mut bam_record_writer = bam::io::Writer::from(Vec::new());
                    let convert_start = Instant::now();
                    let records_written = collect_star_map_chunk_records(
                        &chunk.map_result,
                        &header,
                        &mut bam_record_writer,
                    )
                    .map_err(|err| format!("failed to convert STAR SAM chunk to BAM: {err:?}"))?;
                    let bam_records = bam_record_writer.into_inner();
                    metrics.converter_convert_nanos.fetch_add(
                        convert_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                    let send_start = Instant::now();
                    writer_tx
                        .send(StarConvertedChunk {
                            chunk_index: chunk.chunk_index,
                            bam_records,
                            records_written,
                        })
                        .map_err(|_| "STAR BAM writer thread closed unexpectedly".to_string())?;
                    metrics.converter_writer_send_nanos.fetch_add(
                        send_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                    metrics.converter_chunks.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .converter_records
                        .fetch_add(records_written, Ordering::Relaxed);
                    metrics.update_max_writer_queue_len(writer_tx.len());
                }
                Ok(())
            })
            .expect("failed to spawn STAR SAM converter thread");
        handles.push(handle);
    }
    handles
}

fn join_star_chunk_collector(
    writer_handle: JoinHandle<std::result::Result<u64, String>>,
) -> std::result::Result<u64, String> {
    writer_handle
        .join()
        .map_err(|_| "STAR chunk collector thread panicked".to_string())?
}

fn join_star_converter_workers(
    converter_handles: Vec<JoinHandle<std::result::Result<(), String>>>,
) -> std::result::Result<(), String> {
    for handle in converter_handles {
        handle
            .join()
            .map_err(|_| "STAR SAM converter thread panicked".to_string())??;
    }
    Ok(())
}

struct StarWriterChunk {
    chunk_index: u32,
    map_result: ReadAlignChunkMapChunkResult,
}

struct StarConvertedChunk {
    chunk_index: u32,
    bam_records: Vec<u8>,
    records_written: u64,
}

#[derive(Default)]
struct StarWriterPipelineMetrics {
    converter_chunks: AtomicU64,
    converter_records: AtomicU64,
    converter_convert_nanos: AtomicU64,
    converter_writer_send_nanos: AtomicU64,
    writer_chunks: AtomicU64,
    writer_write_nanos: AtomicU64,
    max_writer_queue_len: AtomicUsize,
}

impl StarWriterPipelineMetrics {
    fn update_max_writer_queue_len(&self, len: usize) {
        let mut current = self.max_writer_queue_len.load(Ordering::Relaxed);
        while len > current {
            match self.max_writer_queue_len.compare_exchange_weak(
                current,
                len,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn snapshot(&self) -> StarWriterPipelineMetricsSnapshot {
        StarWriterPipelineMetricsSnapshot {
            converter_chunks: self.converter_chunks.load(Ordering::Relaxed),
            converter_records: self.converter_records.load(Ordering::Relaxed),
            converter_convert_seconds: nanos_to_seconds(
                self.converter_convert_nanos.load(Ordering::Relaxed),
            ),
            converter_writer_send_seconds: nanos_to_seconds(
                self.converter_writer_send_nanos.load(Ordering::Relaxed),
            ),
            writer_chunks: self.writer_chunks.load(Ordering::Relaxed),
            writer_write_seconds: nanos_to_seconds(self.writer_write_nanos.load(Ordering::Relaxed)),
            max_writer_queue_len: self.max_writer_queue_len.load(Ordering::Relaxed),
        }
    }
}

struct StarWriterPipelineMetricsSnapshot {
    converter_chunks: u64,
    converter_records: u64,
    converter_convert_seconds: f64,
    converter_writer_send_seconds: f64,
    writer_chunks: u64,
    writer_write_seconds: f64,
    max_writer_queue_len: usize,
}

fn nanos_to_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

fn cleanup_star_temp(path_star_tmp: &Path) {
    let _ = fs::remove_dir_all(path_star_tmp);
}

#[derive(Clone, Copy)]
struct StarOutputBudget {
    bytes: ByteSize,
    sampled_read_pairs: u64,
    average_payload_bytes_per_output_record: u64,
    bytes_per_output_record: u64,
}

fn estimate_star_output_budget(
    output_records: u64,
    read_sample: StarReadLengthSample,
) -> StarOutputBudget {
    let average_payload_bytes_per_output_record =
        read_sample.average_payload_bytes_per_output_record();
    let bytes_per_output_record = average_payload_bytes_per_output_record
        .saturating_mul(STAR_OUTPUT_PAYLOAD_SAFETY_NUMERATOR)
        .div_ceil(STAR_OUTPUT_PAYLOAD_SAFETY_DENOMINATOR)
        .saturating_add(STAR_OUTPUT_RECORD_OVERHEAD_BYTES);

    StarOutputBudget {
        bytes: ByteSize(output_records.saturating_mul(bytes_per_output_record)),
        sampled_read_pairs: read_sample.read_pairs,
        average_payload_bytes_per_output_record,
        bytes_per_output_record,
    }
}

fn streaming_sort_memory_budget(total_memory: ByteSize) -> ByteSize {
    let budget = total_memory.as_u64() / 10;
    ByteSize(budget.clamp(ByteSize::gib(1).as_u64(), ByteSize::gib(4).as_u64()))
}

fn current_rss_display() -> String {
    memory_stats::memory_stats()
        .map(|memory| ByteSize(memory.physical_mem as u64).to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn max_rss_display() -> String {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return "unknown".to_string();
    }
    let usage = unsafe { usage.assume_init() };
    #[cfg(target_os = "linux")]
    {
        return ByteSize((usage.ru_maxrss as u64).saturating_mul(1024)).to_string();
    }
    #[cfg(not(target_os = "linux"))]
    {
        ByteSize(usage.ru_maxrss as u64).to_string()
    }
}

fn run_star_rs(
    path_genome: &Path,
    path_in: &Path,
    path_star_tmp: &Path,
    path_out_unsorted_tmp: &Path,
    path_out_sorted: &Path,
    path_temp: &Path,
    numof_threads_writebam: usize,
    align_threads: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    total_memory: ByteSize,
    total_threads: u64,
    rayon_pool: Arc<rayon::ThreadPool>,
    max_read_pairs: Option<u64>,
) -> Result<StarRunResult> {
    info!("Starting star-rs alignment");
    let args = vec![
        "STAR".to_string(),
        "--genomeDir".to_string(),
        path_genome.display().to_string(),
        "--readFilesIn".to_string(),
        "bascet-r1".to_string(),
        "bascet-r2".to_string(),
        "--runThreadN".to_string(),
        align_threads.to_string(),
        "--outSAMtype".to_string(),
        "SAM".to_string(),
        "--outSAMunmapped".to_string(),
        "Within".to_string(),
        "--outSAMattributes".to_string(),
        "Standard".to_string(),
        "--outStd".to_string(),
        "SAM".to_string(),
        "--outTmpDir".to_string(),
        path_star_tmp.display().to_string(),
        "--outFileNamePrefix".to_string(),
        "./".to_string(),
    ];

    debug!(?args, "Running star-rs");
    let star_run = run_star_rs_with_tirp(
        &args,
        path_in,
        path_out_unsorted_tmp,
        path_out_sorted,
        path_temp,
        numof_threads_writebam,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        total_memory,
        total_threads,
        rayon_pool,
        max_read_pairs,
    )
    .map_err(anyhow::Error::msg)?;
    if star_run.exit_code != 0 {
        anyhow::bail!(
            "star-rs failed with exit code {}:\n{}{}{}",
            star_run.exit_code,
            star_run.log_main,
            star_run.log_stdout,
            star_run.log_final_out
        );
    }

    info!("star-rs alignment complete");
    Ok(star_run)
}

fn run_star_rs_with_tirp(
    args: &[String],
    path_in: &Path,
    path_out_unsorted_tmp: &Path,
    path_out_sorted: &Path,
    path_temp: &Path,
    numof_threads_writebam: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    total_memory: ByteSize,
    total_threads: u64,
    rayon_pool: Arc<rayon::ThreadPool>,
    max_read_pairs: Option<u64>,
) -> std::result::Result<StarRunResult, String> {
    let index_cpu_start = CpuSnapshot::now();
    let context = Arc::new(DirectStarContext::new(args)?);
    let index_cpu = index_cpu_start.stage();
    info!(
        wall_seconds = index_cpu.wall_seconds,
        cpu_seconds = index_cpu.cpu_seconds,
        cpu_percent = index_cpu.cpu_percent,
        current_rss = current_rss_display().as_str(),
        max_rss = max_rss_display().as_str(),
        "STAR index loaded"
    );
    if context.sam_header().is_empty() {
        return Err("star-rs did not return a SAM header".to_string());
    }
    let header = context
        .sam_header()
        .parse::<sam::Header>()
        .map_err(|err| format!("failed to parse STAR SAM header: {err:?}"))?;
    let mut header_writer = bam::io::Writer::from(Vec::new());
    header_writer
        .write_header(&header)
        .map_err(|err| format!("failed to encode STAR BAM header: {err:?}"))?;
    let header_bytes = header_writer.into_inner();
    let (sort_tx, sort_rx) =
        crossbeam_channel::bounded::<EncodedBamChunk>(STAR_WRITER_QUEUE_CHUNKS);
    let sort_memory = streaming_sort_memory_budget(total_memory);
    let sort_threads = (total_threads as usize).max(1);
    let path_out_sorted = path_out_sorted.to_path_buf();
    let path_temp = path_temp.to_path_buf();
    let sort_handle = std::thread::Builder::new()
        .name("STARStreamedBamSort".to_string())
        .spawn(move || {
            sort_and_index_encoded_bam_chunk_receiver(
                header_bytes,
                sort_rx,
                &path_out_sorted,
                &path_temp,
                sort_memory,
                sort_threads,
                ReferenceOrder::Lexicographic,
            )
            .map_err(|err| format!("{err:?}"))
        })
        .map_err(|err| format!("failed to spawn streamed STAR BAM sort thread: {err}"))?;
    let worker_count = context.run_thread_n().max(1);
    let converter_count = STAR_WRITER_CONVERTER_THREADS
        .min(worker_count.max(1))
        .max(1);
    let (converter_tx, converter_rx) =
        crossbeam_channel::bounded::<StarWriterChunk>(STAR_WRITER_QUEUE_CHUNKS);
    let (writer_tx, writer_rx) =
        crossbeam_channel::bounded::<StarConvertedChunk>(STAR_WRITER_QUEUE_CHUNKS);
    let writer_metrics = Arc::new(StarWriterPipelineMetrics::default());
    let writer_handle = spawn_star_chunk_collector(
        path_out_unsorted_tmp.to_path_buf(),
        header.clone(),
        numof_threads_writebam,
        writer_rx,
        sort_tx.clone(),
        Arc::clone(&writer_metrics),
    )?;
    drop(sort_tx);
    let converter_handles = spawn_star_converter_workers(
        header,
        converter_rx,
        writer_tx.clone(),
        converter_count,
        Arc::clone(&writer_metrics),
    );
    let writer_tx_metrics = writer_tx.clone();
    drop(writer_tx);

    let sizeof_stream_buffer = super::stream_helpers::stream_buffer_after_index_load(
        "STAR",
        total_memory,
        sizeof_stream_buffer,
        sizeof_stream_arena,
        star_stream_buffer_max(worker_count),
    );
    let decode_inflight_cap = star_decode_inflight_cap(worker_count);

    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .with_opt_rayon_pool(Arc::clone(&rayon_pool))
        .with_opt_rayon_pool_max_inflight(BoundedU64::new_saturating(decode_inflight_cap as u64))
        .build();
    let parser = parse::Tirp::builder().build();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();
    let mut query = stream.query::<tirp::Record>();

    let mut num_read = 0_u64;
    let mut pending = PendingStarRead::default();
    let mut idle_workers = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        idle_workers.push(context.make_worker(worker_id as i32)?);
    }
    info!(
        workers = worker_count,
        rayon_threads = rayon_pool.current_num_threads(),
        decode_inflight_cap,
        converter_count,
        current_rss = current_rss_display().as_str(),
        max_rss = max_rss_display().as_str(),
        "Streaming TIRP reads into parallel STAR workers"
    );
    let alignment_cpu_start = CpuSnapshot::now();

    let (mapped_tx, mapped_rx) = crossbeam_channel::bounded::<
        std::result::Result<TimedMappedStarChunk, String>,
    >(worker_count);
    let mut in_flight = 0_usize;
    let mut first_error = None::<String>;
    let mut completed = BTreeMap::<u32, ReadAlignChunkMapChunkResult>::new();
    let mut next_emit_chunk = 0_u32;
    let mut mapped_read_count = 0_u64;
    let mut next_mapped_log_million = 1_u64;
    let mut process = ReadAlignChunkProcessChunksResult::default();
    let mut stats_all = Stats::default();
    let mut scheduler_metrics = StarSchedulerMetrics::default();
    let mut read_sample = StarReadLengthSample::default();
    let mut read_pair_limit_reached = false;
    stats_l4_stats_resetn(&mut stats_all);

    let mut chunk_index = 0_u32;

    loop {
        let chunk_build_start = Instant::now();
        let mut chunk = BascetStarReadChunk::new(chunk_index);
        let mut chunk_estimated_mate_bytes = [0_usize; 2];
        let mut chunk_has_reads = false;

        loop {
            let pending_read = if let Some(pending_read) = pending.take() {
                pending_read
            } else {
                if max_read_pairs.is_some_and(|limit| num_read >= limit) {
                    read_pair_limit_reached = true;
                    break;
                }
                let record = match query.next_into::<tirp::Record>() {
                    Ok(Some(record)) => record,
                    Ok(None) => break,
                    Err(err) => {
                        first_error = Some(format!("{err:?}"));
                        break;
                    }
                };
                PendingStarReadPair {
                    read_index: num_read,
                    record,
                }
            };
            let read_estimated_bytes = estimated_star_read_pair_packed_bytes(
                &pending_read.record,
                pending_read.read_index,
            );
            let chunk_input_limit = context.chunk_input_limit_bytes().max(1) as usize;
            let would_exceed = chunk_has_reads
                && (chunk_estimated_mate_bytes[0].saturating_add(read_estimated_bytes.mate1)
                    > chunk_input_limit
                    || chunk_estimated_mate_bytes[1].saturating_add(read_estimated_bytes.mate2)
                        > chunk_input_limit
                    || chunk.records.len() >= STAR_READ_PAIRS_PER_CHUNK);
            if would_exceed {
                pending = Some(pending_read);
                break;
            }
            chunk_estimated_mate_bytes[0] =
                chunk_estimated_mate_bytes[0].saturating_add(read_estimated_bytes.mate1);
            chunk_estimated_mate_bytes[1] =
                chunk_estimated_mate_bytes[1].saturating_add(read_estimated_bytes.mate2);
            read_sample.record(&pending_read.record);
            chunk.push(pending_read, read_estimated_bytes);
            num_read += 1;
            chunk_has_reads = true;
            if num_read % 1_000_000 == 0 {
                info!("{}M read pairs chunked for STAR", num_read / 1_000_000);
            }

            if chunk_estimated_mate_bytes[0] >= chunk_input_limit
                || chunk_estimated_mate_bytes[1] >= chunk_input_limit
                || chunk.records.len() >= STAR_READ_PAIRS_PER_CHUNK
            {
                break;
            }
        }

        if first_error.is_some() || !chunk_has_reads {
            break;
        }
        scheduler_metrics.chunk_build_wall_seconds += chunk_build_start.elapsed().as_secs_f64();
        scheduler_metrics.chunks_built += 1;

        while idle_workers.is_empty() {
            match receive_mapped_star_chunk(
                &mapped_rx,
                &mut idle_workers,
                &mut completed,
                &mut next_emit_chunk,
                &mut mapped_read_count,
                &mut next_mapped_log_million,
                &alignment_cpu_start,
                &mut scheduler_metrics,
                &mut process,
                &mut stats_all,
                &mut in_flight,
                &converter_tx,
                &writer_tx_metrics,
                &writer_metrics,
            ) {
                Ok(()) => {}
                Err(err) => {
                    first_error = Some(err);
                    break;
                }
            }
        }
        if first_error.is_some() {
            break;
        }

        let worker = idle_workers.pop().expect("idle STAR worker");
        let tx = mapped_tx.clone();
        let context = Arc::clone(&context);
        in_flight += 1;
        debug!(
            chunk_index,
            read_pairs = chunk.records.len(),
            estimated_input_bytes = chunk.estimated_input_bytes,
            estimated_mate1_input_bytes = chunk.estimated_mate_input_bytes[0],
            estimated_mate2_input_bytes = chunk.estimated_mate_input_bytes[1],
            "Submitting STAR read chunk"
        );
        rayon_pool.spawn(move || {
            let map_start = Instant::now();
            let map_cpu_start = thread_cpu_seconds();
            let result = context.map_read_chunk(worker, chunk);
            let map_wall_seconds = map_start.elapsed().as_secs_f64();
            let map_cpu_seconds = thread_cpu_seconds() - map_cpu_start;
            let result = result.map(|mapped| TimedMappedStarChunk {
                mapped,
                map_wall_seconds,
                map_cpu_seconds,
            });
            let _ = tx.send(result);
        });
        chunk_index += 1;
    }

    while in_flight > 0 {
        match receive_mapped_star_chunk(
            &mapped_rx,
            &mut idle_workers,
            &mut completed,
            &mut next_emit_chunk,
            &mut mapped_read_count,
            &mut next_mapped_log_million,
            &alignment_cpu_start,
            &mut scheduler_metrics,
            &mut process,
            &mut stats_all,
            &mut in_flight,
            &converter_tx,
            &writer_tx_metrics,
            &writer_metrics,
        ) {
            Ok(()) => {}
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
                break;
            }
        }
    }

    drop(query);
    drop(stream);

    if let Some(err) = first_error {
        drop(converter_tx);
        drop(writer_tx_metrics);
        let _ = join_star_converter_workers(converter_handles);
        let _ = join_star_chunk_collector(writer_handle);
        let _ = sort_handle.join();
        return Err(err);
    }

    if let Some(limit) = max_read_pairs
        && read_pair_limit_reached
    {
        info!(
            read_pairs = num_read,
            limit, "Stopped STAR input after requested read-pair limit"
        );
    } else if max_read_pairs.is_some() {
        info!(
            read_pairs = num_read,
            "STAR input exhausted before requested read-pair limit"
        );
    }

    drop(converter_tx);
    join_star_converter_workers(converter_handles)?;
    drop(writer_tx_metrics);
    let output_records = join_star_chunk_collector(writer_handle)?;
    sort_handle
        .join()
        .map_err(|_| "STAR streamed BAM sort thread panicked".to_string())??;
    Ok(StarRunResult {
        exit_code: 0,
        log_main: process.log_main,
        log_stdout: String::new(),
        log_final_out: String::new(),
        read_sample,
        output_records,
    })
}

struct StarRunResult {
    exit_code: i32,
    log_main: String,
    log_stdout: String,
    log_final_out: String,
    read_sample: StarReadLengthSample,
    output_records: u64,
}

type MappedStarChunk = DirectStarMappedChunk<BascetStarReadChunk>;
type PendingStarRead = Option<PendingStarReadPair>;

fn star_decode_inflight_cap(star_worker_count: usize) -> usize {
    star_worker_count.saturating_sub(1).clamp(1, 4)
}

fn star_stream_buffer_max(star_worker_count: usize) -> ByteSize {
    let scaled_mib = star_worker_count.div_ceil(10).clamp(1, 4) as u64 * 256;
    ByteSize::mib(scaled_mib)
}

struct TimedMappedStarChunk {
    mapped: MappedStarChunk,
    map_wall_seconds: f64,
    map_cpu_seconds: f64,
}

#[derive(Default)]
struct StarSchedulerMetrics {
    chunks_built: u64,
    chunks_mapped: u64,
    chunks_sent_to_writer: u64,
    chunk_build_wall_seconds: f64,
    wait_for_worker_wall_seconds: f64,
    aggregate_wall_seconds: f64,
    writer_queue_send_wall_seconds: f64,
    worker_map_wall_seconds: f64,
    worker_map_cpu_seconds: f64,
    max_worker_map_wall_seconds: f64,
    max_worker_map_cpu_percent: f64,
    max_writer_queue_len: usize,
}

impl StarSchedulerMetrics {
    fn worker_map_parallelism(&self, alignment_wall_seconds: f64) -> f64 {
        if alignment_wall_seconds > 0.0 {
            self.worker_map_wall_seconds / alignment_wall_seconds
        } else {
            0.0
        }
    }

    fn worker_map_cpu_parallelism(&self, alignment_wall_seconds: f64) -> f64 {
        if alignment_wall_seconds > 0.0 {
            self.worker_map_cpu_seconds / alignment_wall_seconds
        } else {
            0.0
        }
    }

    fn worker_map_cpu_percent(&self) -> f64 {
        if self.worker_map_wall_seconds > 0.0 {
            self.worker_map_cpu_seconds * 100.0 / self.worker_map_wall_seconds
        } else {
            0.0
        }
    }
}

struct PendingStarReadPair {
    read_index: u64,
    record: tirp::Record,
}

#[derive(Default, Clone, Copy)]
struct StarReadLengthSample {
    read_pairs: u64,
    payload_bytes: u64,
}

impl StarReadLengthSample {
    fn record(&mut self, record: &tirp::Record) {
        if self.read_pairs >= STAR_OUTPUT_BUDGET_SAMPLE_READ_PAIRS {
            return;
        }

        let payload_bytes = record
            .get_ref::<R1>()
            .len()
            .saturating_add(record.get_ref::<Q1>().len())
            .saturating_add(record.get_ref::<R2>().len())
            .saturating_add(record.get_ref::<Q2>().len());
        self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes as u64);
        self.read_pairs += 1;
    }

    fn average_payload_bytes_per_output_record(self) -> u64 {
        if self.read_pairs == 0 {
            return 0;
        }
        self.payload_bytes / self.read_pairs.saturating_mul(2)
    }
}

struct BascetStarReadChunk {
    chunk_index: u32,
    first_read_number: u64,
    records: Vec<tirp::Record>,
    names: Vec<String>,
    estimated_mate_input_bytes: [usize; 2],
    estimated_input_bytes: usize,
}

impl BascetStarReadChunk {
    fn new(chunk_index: u32) -> Self {
        Self {
            chunk_index,
            first_read_number: 0,
            records: Vec::with_capacity(STAR_READ_PAIRS_PER_CHUNK),
            names: Vec::with_capacity(STAR_READ_PAIRS_PER_CHUNK),
            estimated_mate_input_bytes: [0; 2],
            estimated_input_bytes: 0,
        }
    }

    fn push(
        &mut self,
        pending_read: PendingStarReadPair,
        estimated_bytes: StarReadPairPackedBytes,
    ) {
        let record_id = *pending_read.record.get_ref::<Id>();
        let record_umi = *pending_read.record.get_ref::<Umi>();
        if self.records.is_empty() {
            self.first_read_number = pending_read.read_index + 1;
        }
        self.estimated_mate_input_bytes[0] =
            self.estimated_mate_input_bytes[0].saturating_add(estimated_bytes.mate1);
        self.estimated_mate_input_bytes[1] =
            self.estimated_mate_input_bytes[1].saturating_add(estimated_bytes.mate2);
        self.estimated_input_bytes = self
            .estimated_input_bytes
            .saturating_add(estimated_bytes.total());
        self.names.push(make_bascet_read_name(
            record_id,
            record_umi,
            pending_read.read_index,
        ));
        self.records.push(pending_read.record);
    }
}

struct BascetStarReadChunkIter<'a> {
    chunk: &'a BascetStarReadChunk,
    index: usize,
}

impl<'a> Iterator for BascetStarReadChunkIter<'a> {
    type Item = StarReadPair<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.chunk.records.get(self.index)?;
        let name = self.chunk.names.get(self.index)?;
        let read_number = self.chunk.first_read_number + self.index as u64;
        self.index += 1;
        Some(StarReadPair {
            name,
            mate1: StarReadMate {
                seq: *record.get_ref::<R1>(),
                qual: Some(*record.get_ref::<Q1>()),
            },
            mate2: Some(StarReadMate {
                seq: *record.get_ref::<R2>(),
                qual: Some(*record.get_ref::<Q2>()),
            }),
            read_number,
            read_files_index: 0,
            filter: 0,
            extra: "",
        })
    }
}

impl StarReadChunk for BascetStarReadChunk {
    type Iter<'a> = BascetStarReadChunkIter<'a>;

    fn chunk_index(&self) -> u32 {
        self.chunk_index
    }

    fn reads(&self) -> Self::Iter<'_> {
        BascetStarReadChunkIter {
            chunk: self,
            index: 0,
        }
    }

    fn estimated_input_bytes(&self) -> usize {
        self.estimated_input_bytes
    }
}

fn receive_mapped_star_chunk(
    mapped_rx: &crossbeam_channel::Receiver<std::result::Result<TimedMappedStarChunk, String>>,
    idle_workers: &mut Vec<DirectStarWorker>,
    completed: &mut BTreeMap<u32, ReadAlignChunkMapChunkResult>,
    next_emit_chunk: &mut u32,
    mapped_read_count: &mut u64,
    next_mapped_log_million: &mut u64,
    alignment_cpu_start: &CpuSnapshot,
    scheduler_metrics: &mut StarSchedulerMetrics,
    process: &mut ReadAlignChunkProcessChunksResult,
    stats_all: &mut Stats,
    in_flight: &mut usize,
    writer_tx: &crossbeam_channel::Sender<StarWriterChunk>,
    converted_tx: &crossbeam_channel::Sender<StarConvertedChunk>,
    writer_metrics: &StarWriterPipelineMetrics,
) -> std::result::Result<(), String> {
    let wait_start = Instant::now();
    let timed_mapped = mapped_rx
        .recv()
        .map_err(|_| "STAR worker channel closed unexpectedly".to_string())??;
    scheduler_metrics.wait_for_worker_wall_seconds += wait_start.elapsed().as_secs_f64();
    *in_flight = in_flight.saturating_sub(1);
    scheduler_metrics.chunks_mapped += 1;
    scheduler_metrics.worker_map_wall_seconds += timed_mapped.map_wall_seconds;
    scheduler_metrics.worker_map_cpu_seconds += timed_mapped.map_cpu_seconds;
    scheduler_metrics.max_worker_map_wall_seconds = scheduler_metrics
        .max_worker_map_wall_seconds
        .max(timed_mapped.map_wall_seconds);
    if timed_mapped.map_wall_seconds > 0.0 {
        scheduler_metrics.max_worker_map_cpu_percent = scheduler_metrics
            .max_worker_map_cpu_percent
            .max(timed_mapped.map_cpu_seconds * 100.0 / timed_mapped.map_wall_seconds);
    }
    let mapped = timed_mapped.mapped;
    let DirectStarMappedChunk {
        chunk_index,
        input: _input,
        worker,
        map_result,
        stats,
        ..
    } = mapped;
    idle_workers.push(worker);
    let aggregate_start = Instant::now();
    if stats_all.time_start == 0 {
        stats_all.time_start = stats.time_start;
        stats_all.time_start_map = stats.time_start_map;
        stats_all.time_last_report = stats.time_last_report;
    }
    stats_l21_stats_addstats(stats_all, &stats);
    completed.insert(chunk_index, map_result);
    while let Some(mut map_result) = completed.remove(next_emit_chunk) {
        *mapped_read_count = mapped_read_count.saturating_add(map_result.reads_processed);
        while *mapped_read_count >= next_mapped_log_million.saturating_mul(1_000_000) {
            let alignment_cpu = alignment_cpu_start.stage();
            let writer_pipeline = writer_metrics.snapshot();
            debug!(
                mapped_millions = *next_mapped_log_million,
                alignment_wall_seconds = alignment_cpu.wall_seconds,
                alignment_cpu_seconds = alignment_cpu.cpu_seconds,
                alignment_cpu_percent = alignment_cpu.cpu_percent,
                chunks_built = scheduler_metrics.chunks_built,
                chunks_mapped = scheduler_metrics.chunks_mapped,
                chunks_sent_to_writer = scheduler_metrics.chunks_sent_to_writer,
                chunk_build_wall_seconds = scheduler_metrics.chunk_build_wall_seconds,
                wait_for_worker_wall_seconds = scheduler_metrics.wait_for_worker_wall_seconds,
                aggregate_wall_seconds = scheduler_metrics.aggregate_wall_seconds,
                writer_queue_send_wall_seconds = scheduler_metrics.writer_queue_send_wall_seconds,
                worker_map_wall_seconds = scheduler_metrics.worker_map_wall_seconds,
                worker_map_cpu_seconds = scheduler_metrics.worker_map_cpu_seconds,
                worker_map_parallelism =
                    scheduler_metrics.worker_map_parallelism(alignment_cpu.wall_seconds),
                worker_map_cpu_parallelism =
                    scheduler_metrics.worker_map_cpu_parallelism(alignment_cpu.wall_seconds),
                worker_map_cpu_percent = scheduler_metrics.worker_map_cpu_percent(),
                max_worker_map_wall_seconds = scheduler_metrics.max_worker_map_wall_seconds,
                max_worker_map_cpu_percent = scheduler_metrics.max_worker_map_cpu_percent,
                converter_queue_max_len = scheduler_metrics.max_writer_queue_len,
                converter_queue_len = writer_tx.len(),
                converted_queue_len = converted_tx.len(),
                converted_queue_max_len = writer_pipeline.max_writer_queue_len,
                converter_chunks = writer_pipeline.converter_chunks,
                converter_records = writer_pipeline.converter_records,
                converter_convert_seconds = writer_pipeline.converter_convert_seconds,
                converter_writer_send_seconds = writer_pipeline.converter_writer_send_seconds,
                writer_chunks = writer_pipeline.writer_chunks,
                writer_write_seconds = writer_pipeline.writer_write_seconds,
                current_rss = current_rss_display().as_str(),
                max_rss = max_rss_display().as_str(),
                "{next_mapped_log_million}M read pairs mapped by STAR"
            );
            *next_mapped_log_million += 1;
        }
        process.log_main.push_str(&map_result.log_main);
        map_result.log_main.clear();
        let writer_send_start = Instant::now();
        writer_tx
            .send(StarWriterChunk {
                chunk_index: *next_emit_chunk,
                map_result,
            })
            .map_err(|_| "STAR BAM writer thread closed unexpectedly".to_string())?;
        scheduler_metrics.writer_queue_send_wall_seconds +=
            writer_send_start.elapsed().as_secs_f64();
        scheduler_metrics.chunks_sent_to_writer += 1;
        scheduler_metrics.max_writer_queue_len =
            scheduler_metrics.max_writer_queue_len.max(writer_tx.len());
        process.chunks_read += 1;
        *next_emit_chunk += 1;
    }
    scheduler_metrics.aggregate_wall_seconds += aggregate_start.elapsed().as_secs_f64();
    Ok(())
}

struct CpuSnapshot {
    wall: Instant,
    cpu_seconds: f64,
}

struct CpuStage {
    wall_seconds: f64,
    cpu_seconds: f64,
    cpu_percent: f64,
}

impl CpuSnapshot {
    fn now() -> Self {
        Self {
            wall: Instant::now(),
            cpu_seconds: process_cpu_seconds(),
        }
    }

    fn stage(&self) -> CpuStage {
        let wall_seconds = self.wall.elapsed().as_secs_f64();
        let cpu_seconds = process_cpu_seconds() - self.cpu_seconds;
        CpuStage {
            wall_seconds,
            cpu_seconds,
            cpu_percent: if wall_seconds > 0.0 {
                cpu_seconds * 100.0 / wall_seconds
            } else {
                0.0
            },
        }
    }
}

fn process_cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return 0.0;
    }
    let usage = unsafe { usage.assume_init() };
    timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime)
}

fn thread_cpu_seconds() -> f64 {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) };
    if rc != 0 {
        return 0.0;
    }
    let time = unsafe { time.assume_init() };
    time.tv_sec as f64 + time.tv_nsec as f64 / 1_000_000_000.0
}

fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

#[derive(Clone, Copy)]
struct StarReadPairPackedBytes {
    mate1: usize,
    mate2: usize,
}

impl StarReadPairPackedBytes {
    fn total(self) -> usize {
        self.mate1.saturating_add(self.mate2)
    }
}

fn estimated_star_read_pair_packed_bytes(
    record: &tirp::Record,
    zero_based_read_index: u64,
) -> StarReadPairPackedBytes {
    let read_name_len = record
        .get_ref::<Id>()
        .len()
        .saturating_add(1)
        .saturating_add(record.get_ref::<Umi>().len())
        .saturating_add(1)
        .saturating_add(decimal_len(zero_based_read_index));
    let read_number_len = decimal_len(zero_based_read_index.saturating_add(1));
    // Matches STAR's FASTQ-shaped direct chunk record:
    // "@{name} {read_number} N {read_files_index}\n{seq}\n+\n{qual}\n".
    // `read_files_index` comes from STAR parameters, so reserve the full u32 width.
    let header_len = 1usize
        .saturating_add(read_name_len)
        .saturating_add(1)
        .saturating_add(read_number_len)
        .saturating_add(1)
        .saturating_add(1)
        .saturating_add(1)
        .saturating_add(decimal_len(u32::MAX as u64));
    let mate_overhead = header_len.saturating_add(5);

    StarReadPairPackedBytes {
        mate1: mate_overhead
            .saturating_add(record.get_ref::<R1>().len())
            .saturating_add(record.get_ref::<Q1>().len()),
        mate2: mate_overhead
            .saturating_add(record.get_ref::<R2>().len())
            .saturating_add(record.get_ref::<Q2>().len()),
    }
}

fn decimal_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}
