use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use bgzip::{write::BGZFMultiThreadWriter, BGZFError, Compression};


use crate::fileformat::{shard::{CellID, ReadPair}, CellUMI};
use crate::fileformat::ReadPairWriter;

use super::ConstructFromPath;



#[derive(Debug,Clone)]
pub struct BascetSingleFastqWriterFactory {
}
impl BascetSingleFastqWriterFactory {
    pub fn new() -> BascetSingleFastqWriterFactory {
        BascetSingleFastqWriterFactory {}
    } 
}
impl ConstructFromPath<BascetSingleFastqWriter> for BascetSingleFastqWriterFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BascetSingleFastqWriter> {  ///////// maybe anyhow prevents spec of reader?
        BascetSingleFastqWriter::new(fname)
    }
}




pub struct BascetSingleFastqWriter {
    pub writer: BGZFMultiThreadWriter<File>
}
impl BascetSingleFastqWriter {

    fn new(path: &PathBuf) -> anyhow::Result<BascetSingleFastqWriter>{

        println!("starting writer for single FASTQ {:?}", path);

        let out_buffer = File::create(&path).expect("Failed to create fastq.gz output file");
        let writer = BGZFMultiThreadWriter::new(out_buffer, Compression::default());
    
        Ok(BascetSingleFastqWriter {
            writer: writer
        })
    }

}


impl ReadPairWriter for BascetSingleFastqWriter {


    fn write_reads_for_cell(&mut self, cell_id:&CellID, list_reads: &Arc<Vec<ReadPair>>) {
        let mut read_num = 0;
        for rp in list_reads.iter() {

            write_single_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 1),
                &rp.r1,
                &rp.q1
            ).unwrap();

            write_single_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 2),
                &rp.r2,
                &rp.q2
            ).unwrap();

            read_num+=1;
        }
    }
   
}













////////// Write one FASTQ read
fn write_single_fastq_read<W: std::io::Write>(
    writer: &mut W,
    head: &Vec<u8>,
    seq:&Vec<u8>,
    qual:&Vec<u8>
) -> Result<(), BGZFError> {
    writer.write_all(b"@")?;
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
    illumna_read_index: u32
) -> Vec<u8> {
    // typical readname from a random illumina library from miseq, @M03699:250:000000000-DT36J:1:1102:5914:5953 1:N:0:GACGAGATTA+ACATTATCCT
    let name=format!("BASCET_{}:{}:{} {}", 
        cell_id, 
        String::from_utf8(cell_umi.clone()).unwrap(), 
        read_num, 
        illumna_read_index);
    name.as_bytes().to_vec()  //TODO best if we can avoid making a String
}


