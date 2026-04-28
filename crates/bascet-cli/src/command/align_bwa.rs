use std::{
    fs::File,
    io::Cursor,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use noodles::{
    bam, bgzf, sam,
    sam::alignment::{
        RecordBuf, io::Write as _, record::data::field::Tag, record_buf::data::field::Value,
    },
};
use tracing::info;

use super::align::{index_bam, sort_bam};
use crate::utils::{atomic_temp_path, publish_atomic_output};

const BWA_MEM2_BATCH_BASES: usize = 10_000_000;
type NoodlesBamWriter = bam::io::Writer<bgzf::io::MultithreadedWriter<File>>;

pub fn try_execute_bwa_mem2(
    path_in: &Path,
    path_genome: &Path,
    path_out_unsorted: &PathBuf,
    path_out_sorted: &PathBuf,
    path_temp: &PathBuf,
    numof_threads_writebam: usize,
    total_threads: u64,
    numof_threads_read: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
) -> Result<()> {
    info!("Using direct bwa-mem2-rs aligner");

    let mut aligner = bwa_mem2_rs::mem_api::MemAligner::new(path_genome, total_threads as usize)
        .map_err(|e| anyhow::anyhow!(e))?;

    let sam_header = aligner.sam_header().map_err(|e| anyhow::anyhow!(e))?;
    let bam_header = sam_header.parse::<sam::Header>()?;
    let worker_count = NonZeroUsize::new(numof_threads_writebam)
        .context("BAM writer thread count must be nonzero")?;
    let path_out_unsorted_tmp = atomic_temp_path(path_out_unsorted);
    let file = File::create(&path_out_unsorted_tmp).with_context(|| {
        format!(
            "failed to create BAM writer for {:?}",
            path_out_unsorted_tmp
        )
    })?;
    let bgzf_writer = bgzf::io::MultithreadedWriter::with_worker_count(worker_count, file);
    let mut writer_bam = bam::io::Writer::from(bgzf_writer);
    writer_bam.write_header(&bam_header)?;

    align_tirp_with_bwa_mem2(
        path_in,
        &mut aligner,
        &bam_header,
        &mut writer_bam,
        numof_threads_read,
        sizeof_stream_arena,
        sizeof_stream_buffer,
    )?;
    let mut bgzf_writer = writer_bam.into_inner();
    bgzf_writer.finish()?;
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

fn align_tirp_with_bwa_mem2<P>(
    path_in: P,
    aligner: &mut bwa_mem2_rs::mem_api::MemAligner,
    header: &sam::Header,
    writer_bam: &mut NoodlesBamWriter,
    num_threads: BoundedU64<1, { u64::MAX }>,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
) -> Result<()>
where
    P: AsRef<Path>,
{
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
    let mut batch = Vec::new();
    let mut batch_bases = 0_usize;
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

                batch_bases += record_r1.len() + record_r2.len();
                batch.push(OwnedBwaReadPair {
                    name: read_name,
                    r1: record_r1.to_vec(),
                    q1: record_q1.to_vec(),
                    r2: record_r2.to_vec(),
                    q2: record_q2.to_vec(),
                });

                num_read += 1;
                if num_read % 1_000_000 == 0 {
                    info!("{}M read pairs aligned", num_read / 1_000_000);
                }
                if batch_bases >= BWA_MEM2_BATCH_BASES {
                    flush_bwa_mem2_batch(aligner, header, writer_bam, &batch)?;
                    batch.clear();
                    batch_bases = 0;
                }
            }
            Ok(None) => break,
            Err(e) => panic!("{:?}", e),
        };
    }

    flush_bwa_mem2_batch(aligner, header, writer_bam, &batch)?;
    Ok(())
}

struct OwnedBwaReadPair {
    name: String,
    r1: Vec<u8>,
    q1: Vec<u8>,
    r2: Vec<u8>,
    q2: Vec<u8>,
}

fn flush_bwa_mem2_batch(
    aligner: &mut bwa_mem2_rs::mem_api::MemAligner,
    header: &sam::Header,
    writer: &mut NoodlesBamWriter,
    batch: &[OwnedBwaReadPair],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

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

    let sam_lines = aligner
        .align_pairs(&pairs)
        .map_err(|e| anyhow::anyhow!(e))?;
    for line in sam_lines {
        write_tagged_bam_alignment(writer, header, &line)?;
    }
    Ok(())
}

fn write_tagged_bam_alignment(
    writer: &mut NoodlesBamWriter,
    header: &sam::Header,
    line: &str,
) -> Result<()> {
    let line = line.trim_end_matches('\n');
    if line.is_empty() {
        return Ok(());
    }

    let mut sam_reader = sam::io::Reader::new(Cursor::new(line.as_bytes()));
    let mut record = RecordBuf::default();
    sam_reader
        .read_record_buf(header, &mut record)
        .with_context(|| format!("failed to parse BWA SAM record: {line}"))?;

    let read_name = record.name().context("BWA SAM record is missing QNAME")?;
    let (cell_id, umi) = crate::fileformat::bam::readname_to_cell_umi(read_name.as_ref());
    let cell_id = std::str::from_utf8(cell_id)
        .with_context(|| format!("cell id in read name is not UTF-8: {:?}", read_name))?
        .to_owned();
    let umi = if umi.is_empty() {
        None
    } else {
        Some(
            std::str::from_utf8(umi)
                .with_context(|| format!("UMI in read name is not UTF-8: {:?}", read_name))?
                .to_owned(),
        )
    };

    record
        .data_mut()
        .insert(Tag::CELL_BARCODE_ID, Value::from(cell_id));
    if let Some(umi) = umi.as_deref() {
        record
            .data_mut()
            .insert(Tag::new(b'U', b'B'), Value::from(umi));
    }

    writer.write_alignment_record(header, &record)?;
    Ok(())
}

fn make_bascet_read_name(record_id: &[u8], record_umi: &[u8], num_read: u64) -> String {
    let mut read_name = String::with_capacity(record_id.len() + record_umi.len() + 32);
    read_name.push_str(&String::from_utf8_lossy(record_id));
    read_name.push(':');
    read_name.push_str(&String::from_utf8_lossy(record_umi));
    read_name.push(':');
    read_name.push_str(&num_read.to_string());
    read_name
}
