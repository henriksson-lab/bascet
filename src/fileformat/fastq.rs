use bgzip::{write::BGZFMultiThreadWriter, BGZFError, Compression};
use log::debug;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::fileformat::{
    shard::{CellID, ReadPair},
    CellUMI,
};
//use crate::fileformat::DetectedFileformat;
use crate::fileformat::ReadPairWriter;

use super::ConstructFromPath;
//use crate::fileformat::ReadPairReader;

use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

#[derive(Debug, Clone)]
pub struct BascetFastqWriterFactory {}
impl BascetFastqWriterFactory {
    pub fn new() -> BascetFastqWriterFactory {
        BascetFastqWriterFactory {}
    }
}
impl ConstructFromPath<BascetFastqWriter> for BascetFastqWriterFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BascetFastqWriter> {
        ///////// maybe anyhow prevents spec of reader?
        BascetFastqWriter::new(fname)
    }
}

pub struct BascetFastqWriter {
    pub writer: BGZFMultiThreadWriter<File>,
}
impl BascetFastqWriter {
    fn new(path: &PathBuf) -> anyhow::Result<BascetFastqWriter> {
        let out_buffer = File::create(&path).expect("Failed to create fastq.gz output file");
        let writer = BGZFMultiThreadWriter::new(out_buffer, Compression::default());

        Ok(BascetFastqWriter { writer: writer })
    }
}

impl ReadPairWriter for BascetFastqWriter {
    fn write_reads_for_cell(&mut self, cell_id: &CellID, list_reads: &Arc<Vec<ReadPair>>) {
        let mut read_num = 0;
        for rp in list_reads.iter() {
            write_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 1),
                &rp.r1,
                &rp.q1,
            )
            .unwrap();

            write_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 2),
                &rp.r2,
                &rp.q2,
            )
            .unwrap();

            read_num += 1;
        }
    }
}

////////// Write one FASTQ read
fn write_fastq_read<W: std::io::Write>(
    writer: &mut W,
    head: &Vec<u8>,
    seq: &Vec<u8>,
    qual: &Vec<u8>,
) -> Result<(), BGZFError> {
    writer.write_all(b">")?;
    writer.write_all(head.as_slice())?;
    writer.write_all(b"\n")?;
    writer.write_all(seq.as_slice())?;
    writer.write_all(b"\n+\n")?;
    writer.write_all(&qual.as_slice())?;
    writer.write_all(b"\n")?;
    Ok(())
}

//// Format FASTQ read names
fn make_fastq_readname(
    read_num: u32,
    cell_id: &CellID,
    cell_umi: &CellUMI,
    illumna_read_index: u32,
) -> Vec<u8> {
    // typical readname from a random illumina library from miseq, @M03699:250:000000000-DT36J:1:1102:5914:5953 1:N:0:GACGAGATTA+ACATTATCCT
    let name = format!(
        "BASCET_{}:{}:{} {}",
        cell_id,
        String::from_utf8(cell_umi.clone()).unwrap(),
        read_num,
        illumna_read_index
    );
    name.as_bytes().to_vec() //TODO best if we can avoid making a String
}

pub fn open_fastq(file_handle: &PathBuf) -> anyhow::Result<FastqReader<Box<dyn std::io::Read>>> {
    let opened_handle = File::open(file_handle)
        .expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    let (reader, compression) = niffler::get_reader(Box::new(opened_handle))
        .expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    debug!(
        "Opened file {} with compression {:?}",
        &file_handle.display(),
        &compression
    );
    Ok(FastqReader::new(reader))
}
