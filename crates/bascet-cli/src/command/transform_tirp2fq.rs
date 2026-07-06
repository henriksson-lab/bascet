use std::{
    fs::File,
    io::Write,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use bascet_core::{
    Composite, Stream,
    attr::{meta::*, quality::*, sequence::*},
};
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use noodles::bgzf::io::MultithreadedWriter;
use tracing::{debug, info};

use crate::{
    fileformat::{DetectedFileformat, detect_shard_format},
    utils::{atomic_temp_path, publish_atomic_output},
};

const DEFAULT_STREAM_BUFFER: ByteSize = ByteSize::gib(1);
const MAX_STREAM_BUFFER: ByteSize = ByteSize::gib(4);

pub fn try_tirp_to_fastq_fast_path(
    path_in: &Path,
    path_out: &Path,
    threads: usize,
    memory: Option<ByteSize>,
) -> Result<()> {
    let output_format = detect_shard_format(&path_out.to_path_buf());
    let read_threads = BoundedU64::new(threads.max(1) as u64).expect("thread count is nonzero");
    let stream_buffer = stream_buffer_size(memory);
    let write_threads_total = threads.max(1);
    let writer_threads_per_file = match output_format {
        DetectedFileformat::PairedFASTQ => (write_threads_total / 2).max(1),
        _ => write_threads_total,
    };
    let writer_threads_per_file =
        NonZeroUsize::new(writer_threads_per_file).expect("thread count is nonzero");

    info!(
        input = %path_in.display(),
        output = %path_out.display(),
        output_format = ?output_format,
        threads,
        read_threads = read_threads.get(),
        writer_threads_per_file = writer_threads_per_file.get(),
        memory = ?memory,
        stream_buffer = %stream_buffer,
        "TIRP->FASTQ fast path: starting"
    );

    match output_format {
        DetectedFileformat::PairedFASTQ => write_paired_fastq(
            path_in,
            path_out,
            read_threads,
            writer_threads_per_file,
            stream_buffer,
        )?,
        DetectedFileformat::SingleFASTQ => write_single_fastq(
            path_in,
            path_out,
            read_threads,
            writer_threads_per_file,
            stream_buffer,
        )?,
        _ => anyhow::bail!(
            "TIRP->FASTQ fast path requires FASTQ output, got {}",
            path_out.display()
        ),
    }

    info!("TIRP->FASTQ fast path: finished");
    Ok(())
}

fn stream_buffer_size(memory: Option<ByteSize>) -> ByteSize {
    let Some(memory) = memory else {
        return DEFAULT_STREAM_BUFFER;
    };
    let headroom = ByteSize(
        ByteSize::mib(512)
            .as_u64()
            .max((memory.as_u64() as f64 * 0.10) as u64),
    );
    let available = memory.as_u64().saturating_sub(headroom.as_u64());
    ByteSize(available.clamp(DEFAULT_STREAM_BUFFER.as_u64(), MAX_STREAM_BUFFER.as_u64()))
}

fn write_paired_fastq(
    path_in: &Path,
    path_r1: &Path,
    read_threads: BoundedU64<1, { u64::MAX }>,
    writer_threads_per_file: NonZeroUsize,
    stream_buffer: ByteSize,
) -> Result<()> {
    let path_r1 = path_r1.to_path_buf();
    let path_r2 = r2_path_from_r1(&path_r1)?;
    let path_r1_tmp = atomic_temp_path(&path_r1);
    let path_r2_tmp = r2_path_from_r1(&path_r1_tmp)?;

    let file_r1 = File::create(&path_r1_tmp)
        .with_context(|| format!("failed to create FASTQ output {}", path_r1_tmp.display()))?;
    let file_r2 = File::create(&path_r2_tmp)
        .with_context(|| format!("failed to create FASTQ output {}", path_r2_tmp.display()))?;
    let mut writer_r1 = MultithreadedWriter::with_worker_count(writer_threads_per_file, file_r1);
    let mut writer_r2 = MultithreadedWriter::with_worker_count(writer_threads_per_file, file_r2);

    stream_tirp_to_fastq(path_in, read_threads, stream_buffer, |record, num_read| {
        write_read(
            &mut writer_r1,
            *record.get_ref::<Id>(),
            *record.get_ref::<R1>(),
            *record.get_ref::<Q1>(),
            *record.get_ref::<Umi>(),
            num_read,
            1,
        )?;
        write_read(
            &mut writer_r2,
            *record.get_ref::<Id>(),
            *record.get_ref::<R2>(),
            *record.get_ref::<Q2>(),
            *record.get_ref::<Umi>(),
            num_read,
            2,
        )
    })?;

    writer_r1.finish()?;
    writer_r2.finish()?;
    publish_atomic_output(path_r1_tmp, path_r1)?;
    publish_atomic_output(path_r2_tmp, path_r2)?;
    Ok(())
}

fn write_single_fastq(
    path_in: &Path,
    path_out: &Path,
    read_threads: BoundedU64<1, { u64::MAX }>,
    writer_threads: NonZeroUsize,
    stream_buffer: ByteSize,
) -> Result<()> {
    let path_tmp = atomic_temp_path(&path_out.to_path_buf());
    let file = File::create(&path_tmp)
        .with_context(|| format!("failed to create FASTQ output {}", path_tmp.display()))?;
    let mut writer = MultithreadedWriter::with_worker_count(writer_threads, file);

    stream_tirp_to_fastq(path_in, read_threads, stream_buffer, |record, num_read| {
        write_read(
            &mut writer,
            *record.get_ref::<Id>(),
            *record.get_ref::<R1>(),
            *record.get_ref::<Q1>(),
            *record.get_ref::<Umi>(),
            num_read,
            1,
        )?;
        write_read(
            &mut writer,
            *record.get_ref::<Id>(),
            *record.get_ref::<R2>(),
            *record.get_ref::<Q2>(),
            *record.get_ref::<Umi>(),
            num_read,
            2,
        )
    })?;

    writer.finish()?;
    publish_atomic_output(path_tmp, path_out)?;
    Ok(())
}

fn stream_tirp_to_fastq(
    path_in: &Path,
    read_threads: BoundedU64<1, { u64::MAX }>,
    stream_buffer: ByteSize,
    mut write_record: impl FnMut(&tirp::Record, u64) -> Result<()>,
) -> Result<()> {
    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .countof_threads(read_threads)
        .build();
    let parser = parse::Tirp::builder().build();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(bascet_core::DEFAULT_SIZEOF_ARENA)
        .sizeof_decode_buffer(stream_buffer)
        .build();

    let mut query = stream.query::<tirp::Record>();
    let mut num_read = 0_u64;

    debug!("TIRP->FASTQ fast path: streaming records");
    loop {
        match query.next_into::<tirp::Record>() {
            Ok(Some(record)) => {
                write_record(&record, num_read)?;
                num_read += 1;
                if num_read % 1_000_000 == 0 {
                    info!(read_pairs_m = num_read / 1_000_000, "TIRP->FASTQ fast path");
                }
            }
            Ok(None) => break,
            Err(err) => return Err(err).context("failed to read TIRP record"),
        }
    }

    info!(
        read_pairs = num_read,
        "TIRP->FASTQ fast path: records written"
    );
    Ok(())
}

fn write_read<W: Write>(
    writer: &mut W,
    cell_id: &[u8],
    read: &[u8],
    qual: &[u8],
    umi: &[u8],
    num_read: u64,
    read_index: u8,
) -> Result<()> {
    writer.write_all(b"@")?;
    writer.write_all(cell_id)?;
    writer.write_all(b":")?;
    writer.write_all(umi)?;
    writer.write_all(b":")?;
    write!(writer, "{num_read} {read_index}")?;
    writer.write_all(b"\n")?;
    writer.write_all(read)?;
    writer.write_all(b"\n+\n")?;
    writer.write_all(qual)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn r2_path_from_r1(path_r1: &Path) -> Result<PathBuf> {
    let path_string = path_r1.to_string_lossy();
    let last_pos = path_string
        .rfind("R1")
        .ok_or_else(|| anyhow::anyhow!("Could not find R2 path for {}", path_r1.display()))?;
    let mut bytes = path_string.as_bytes().to_vec();
    bytes[last_pos + 1] = b'2';
    Ok(PathBuf::from(String::from_utf8(bytes)?))
}
