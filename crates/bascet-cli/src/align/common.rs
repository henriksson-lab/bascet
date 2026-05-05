//! Helpers shared by the alignment dispatch (`command::align`) and the `tofq` subcommand.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use bascet_core::Stream;
use bascet_core::attr::{meta::*, quality::*, sequence::*};
use bascet_core::*;
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use tracing::{debug, info, warn};

/// Warn (don't fail) if the on-disk size of an aligner's index exceeds the user's memory
/// budget. Indexes are typically mmap'd or fully loaded — exceeding budget is a soft signal
/// the run will swap or OOM, but some setups can tolerate it.
pub fn warn_if_index_disk_size_exceeds_memory(
    aligner_name: &str,
    index_path: &Path,
    index_disk_size_bytes: u64,
    total_memory: ByteSize,
) {
    let index_disk_size = ByteSize(index_disk_size_bytes);
    if index_disk_size.as_u64() > total_memory.as_u64() {
        warn!(
            aligner = aligner_name,
            index_path = ?index_path,
            index_disk_size = %index_disk_size,
            total_memory = %total_memory,
            "Aligner index files on disk are larger than the provided memory budget"
        );
    }
}

/// Stream a TIRP file to two FASTQ writers (R1, R2). Read names are encoded as
/// `cell_id:umi:num` so downstream aligners that don't preserve tags can recover the cell
/// identity from QNAME. Used by the `tofq` subcommand and by tests.
pub fn write_tirp_to_2fq<P>(
    path_in: P,
    writer_r1: &mut impl Write,
    writer_r2: &mut impl Write,
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

    debug!("Sending read pairs");
    let mut num_read: u64 = 0;
    loop {
        match query.next_into::<tirp::Record>() {
            Ok(Some(record)) => {
                let record_id = *record.get_ref::<Id>();
                let record_r1 = *record.get_ref::<R1>();
                let record_r2 = *record.get_ref::<R2>();
                let record_q1 = *record.get_ref::<Q1>();
                let record_q2 = *record.get_ref::<Q2>();
                let record_umi = *record.get_ref::<Umi>();

                write_read_bascetfq(
                    writer_r1, record_id, record_r1, record_q1, record_umi, num_read,
                )?;
                write_read_bascetfq(
                    writer_r2, record_id, record_r2, record_q2, record_umi, num_read,
                )?;

                num_read += 1;
                if num_read % 1_000_000 == 0 {
                    info!("{}M Read pairs written", num_read / 1_000_000);
                }
            }
            Ok(None) => break,
            Err(e) => panic!("{e:?}"),
        };
    }
    debug!("All readpairs sent");

    writer_r1.flush()?;
    writer_r2.flush()?;
    debug!("All readpairs flushed");
    Ok(())
}

fn write_read_bascetfq<W>(
    writer: &mut W,
    record_id: &[u8],
    record_read: &[u8],
    record_qual: &[u8],
    record_umi: &[u8],
    num_read: u64,
) -> Result<()>
where
    W: Write,
{
    writer.write_all(b"@")?;
    writer.write_all(record_id)?;
    writer.write_all(b":")?;
    writer.write_all(record_umi)?;
    writer.write_all(b":")?;
    writer.write_all(format!("{num_read}").as_bytes())?;
    writer.write_all(b"\n")?;
    writer.write_all(record_read)?;
    writer.write_all(b"\n+\n")?;
    writer.write_all(record_qual)?;
    writer.write_all(b"\n")?;
    Ok(())
}
