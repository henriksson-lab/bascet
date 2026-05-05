use std::{
    fs::{self},
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
use noodles::sam;
use tracing::{debug, info};

use super::output::{
    SamRecordSink, TaggedBamSamSink, create_tagged_bam_writer, finish_tagged_bam_writer,
    make_bascet_read_name,
};
use crate::command::bamsort::sort_and_index_bam;
use crate::utils::{atomic_temp_path, publish_atomic_output};
use star_rs::{
    direct::{DirectReadPair, DirectStarRun},
    generated::structs::{ReadAlignChunkProcessChunksResult, StarMainResult},
};

pub fn try_execute_star_rs(
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
        read_threads = numof_threads_read.get(),
        write_bam_threads = numof_threads_writebam,
        "Configured STAR alignment threading"
    );

    fs::create_dir_all(path_temp)
        .with_context(|| format!("failed to create STAR temp directory {:?}", path_temp))?;

    let path_star_tmp = atomic_temp_path(&path_temp.join("star-rs-tmp"));
    fs::create_dir_all(&path_star_tmp)
        .with_context(|| format!("failed to create STAR work directory {:?}", path_star_tmp))?;

    let result = match run_star_rs(
        path_genome,
        path_in,
        &path_star_tmp,
        align_threads,
        numof_threads_read,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        total_memory,
        rayon_pool,
    ) {
        Ok(result) => result,
        Err(err) => {
            cleanup_star_temp(&path_star_tmp);
            return Err(err);
        }
    };

    let path_out_unsorted_tmp = atomic_temp_path(path_out_unsorted);
    if let Err(err) =
        write_star_sam_chunks_to_tagged_bam(&result, &path_out_unsorted_tmp, numof_threads_writebam)
    {
        cleanup_star_temp(&path_star_tmp);
        let _ = fs::remove_file(&path_out_unsorted_tmp);
        return Err(err);
    }
    publish_atomic_output(&path_out_unsorted_tmp, path_out_unsorted)?;

    cleanup_star_temp(&path_star_tmp);

    info!("Sorting + indexing BAM file (in-process)");
    sort_and_index_bam(
        path_out_unsorted,
        path_out_sorted,
        path_temp,
        total_memory,
        total_threads as usize,
    )?;

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

fn write_star_sam_chunks_to_tagged_bam(
    result: &StarMainResult,
    path: &Path,
    num_threads: usize,
) -> Result<()> {
    if result.parameters.sam_header.is_empty() {
        anyhow::bail!("star-rs did not return a SAM header");
    }

    let header = result.parameters.sam_header.parse::<sam::Header>()?;
    let mut writer = create_tagged_bam_writer(path, &header, num_threads)?;
    {
        let mut sink = TaggedBamSamSink::new(&mut writer, &header, "STAR");
        for process in &result.process_chunks {
            write_star_process_sam_chunks(process, &mut sink)?;
        }
    }
    finish_tagged_bam_writer(writer)
}

fn write_star_process_sam_chunks(
    process: &ReadAlignChunkProcessChunksResult,
    sink: &mut impl SamRecordSink,
) -> Result<()> {
    for map_chunk in &process.map_chunks {
        write_star_sam_bytes(&map_chunk.direct_sam_output, sink)?;
        write_star_sam_bytes(&map_chunk.paired_keep_input_order_tmp, sink)?;
    }
    Ok(())
}

fn write_star_sam_bytes(bytes: &[u8], sink: &mut impl SamRecordSink) -> Result<()> {
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() || line.starts_with(b"@") {
            continue;
        }
        let line = std::str::from_utf8(line).context("STAR SAM output is not UTF-8")?;
        sink.record(line)?;
    }
    Ok(())
}

fn cleanup_star_temp(path_star_tmp: &Path) {
    let _ = fs::remove_dir_all(path_star_tmp);
}

fn run_star_rs(
    path_genome: &Path,
    path_in: &Path,
    path_star_tmp: &Path,
    align_threads: usize,
    numof_threads_read: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    total_memory: ByteSize,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> Result<StarMainResult> {
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
    let result = run_star_rs_with_tirp(
        &args,
        path_in,
        numof_threads_read,
        sizeof_stream_arena,
        sizeof_stream_buffer,
        total_memory,
        rayon_pool,
    )
    .map_err(anyhow::Error::msg)?;
    if result.exit_code != 0 {
        anyhow::bail!(
            "star-rs failed with exit code {}:\n{}{}{}",
            result.exit_code,
            result.log_main,
            result.log_stdout,
            result.log_final_out
        );
    }
    if result.parameters.sam_header.is_empty() {
        anyhow::bail!(
            "star-rs did not return a SAM header:\n{}{}{}",
            result.log_main,
            result.log_stdout,
            result.log_final_out
        );
    }

    info!("star-rs alignment complete");
    Ok(result)
}

fn run_star_rs_with_tirp(
    args: &[String],
    path_in: &Path,
    numof_threads_read: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    total_memory: ByteSize,
    rayon_pool: Arc<rayon::ThreadPool>,
) -> std::result::Result<StarMainResult, String> {
    let mut runner = DirectStarRun::new(args)?;
    info!("STAR index loaded");
    let sizeof_stream_buffer = super::stream_helpers::stream_buffer_after_index_load(
        "STAR",
        total_memory,
        sizeof_stream_buffer,
        sizeof_stream_arena,
    );
    info!("Streaming TIRP reads directly into STAR chunks");

    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .countof_threads(numof_threads_read)
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

    let mut num_read = 0_u64;
    let mut pending = PendingStarRead::default();

    loop {
        runner.clear_chunk_input();
        let mut chunk_has_reads = false;

        loop {
            let record = if let Some(record) = pending.take() {
                record
            } else {
                match query.next_into::<tirp::Record>() {
                    Ok(Some(record)) => owned_star_read_from_record(&record, num_read),
                    Ok(None) => break,
                    Err(err) => return Err(format!("{err:?}")),
                }
            };

            if chunk_has_reads && runner.read_pair_would_exceed_chunk(&record.as_direct_read()) {
                pending = Some(record);
                break;
            }

            runner.append_read_pair(&record.as_direct_read());
            num_read += 1;
            chunk_has_reads = true;
            if num_read % 1_000_000 == 0 {
                info!("{}M read pairs sent to STAR", num_read / 1_000_000);
            }

            if runner.chunk_reached_limit() {
                break;
            }
        }

        if !chunk_has_reads {
            break;
        }

        runner.finalize_and_map_chunk()?;
    }

    Ok(runner.finish())
}

type PendingStarRead = Option<OwnedStarReadPair>;

struct OwnedStarReadPair {
    name: String,
    r1: Vec<u8>,
    q1: Vec<u8>,
    r2: Vec<u8>,
    q2: Vec<u8>,
}

impl OwnedStarReadPair {
    fn as_direct_read(&self) -> DirectReadPair<'_> {
        DirectReadPair {
            name: &self.name,
            r1: &self.r1,
            q1: &self.q1,
            r2: &self.r2,
            q2: &self.q2,
        }
    }
}

fn owned_star_read_from_record(record: &tirp::Record, num_read: u64) -> OwnedStarReadPair {
    let record_id = *record.get_ref::<Id>();
    let record_r1 = *record.get_ref::<R1>();
    let record_r2 = *record.get_ref::<R2>();
    let record_q1 = *record.get_ref::<Q1>();
    let record_q2 = *record.get_ref::<Q2>();
    let record_umi = *record.get_ref::<Umi>();
    OwnedStarReadPair {
        name: make_bascet_read_name(record_id, record_umi, num_read),
        r1: record_r1.to_vec(),
        q1: record_q1.to_vec(),
        r2: record_r2.to_vec(),
        q2: record_q2.to_vec(),
    }
}
