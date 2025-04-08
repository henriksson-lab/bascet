use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use bgzip::{write::BGZFMultiThreadWriter, BGZFError, Compression};


use crate::fileformat::{shard::{CellID, ReadPair}, CellUMI};
use crate::fileformat::ReadPairWriter;

use super::ConstructFromPath;



#[derive(Debug,Clone)]
pub struct BascetPairedFastqWriterFactory {
}
impl BascetPairedFastqWriterFactory {
    pub fn new() -> BascetPairedFastqWriterFactory {
        BascetPairedFastqWriterFactory {}
    } 
}
impl ConstructFromPath<BascetPairedFastqWriter> for BascetPairedFastqWriterFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BascetPairedFastqWriter> {  ///////// maybe anyhow prevents spec of reader?
        BascetPairedFastqWriter::new(fname)
    }
}


pub struct BascetPairedFastqWriter {
    pub writer_r1: BGZFMultiThreadWriter<File>,
    pub writer_r2: BGZFMultiThreadWriter<File>
}
impl BascetPairedFastqWriter {

    fn new(path: &PathBuf) -> anyhow::Result<BascetPairedFastqWriter>{

        println!("starting writer for paired FASTQ {:?}", path);

        //See if this is R1 of a pair of FASTQ
        let spath = path.to_string_lossy();
        let last_pos = spath.rfind("R1");
        
        if let Some(last_pos) = last_pos {
            //Create R2 path
            let mut spath_r2 = spath.as_bytes().to_vec();
            spath_r2[last_pos+1] = b'2';
            let path_r2 = String::from_utf8(spath_r2).unwrap();

            //Open writers
            let out_buffer_r1 = File::create(&path).expect("Failed to create fastq.gz output file");
            let writer_r1 = BGZFMultiThreadWriter::new(out_buffer_r1, Compression::default());
        
            let out_buffer_r2 = File::create(&path_r2).expect("Failed to create fastq.gz output file");
            let writer_r2 = BGZFMultiThreadWriter::new(out_buffer_r2, Compression::default());
    
    
            Ok(BascetPairedFastqWriter {
                writer_r1: writer_r1,
                writer_r2: writer_r2
            })
        } else {
            anyhow::bail!("Could not find R2 for fastq file {:?}", path);
        }

    }

}


impl ReadPairWriter for BascetPairedFastqWriter {


    fn write_reads_for_cell(&mut self, cell_id:&CellID, list_reads: &Arc<Vec<ReadPair>>) {
        let mut read_num = 0;
        for rp in list_reads.iter() {

            write_paired_fastq_read(
                &mut self.writer_r1,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 1),
                &rp.r1,
                &rp.q1
            ).unwrap();

            write_paired_fastq_read(
                &mut self.writer_r2,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 2),
                &rp.r2,
                &rp.q2
            ).unwrap();

            read_num+=1;
        }
    }
   
}













////////// Write one FASTQ read
fn write_paired_fastq_read<W: std::io::Write>(
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


