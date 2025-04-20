use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use bgzip::{write::BGZFMultiThreadWriter, BGZFError, Compression};


use crate::{command::getraw, fileformat::{shard::{CellID, ReadPair}, CellUMI}};
use crate::fileformat::ReadPairWriter;

use super::{bam, detect_fileformat::get_fq_filename_r2_from_r1, ConstructFromPath, StreamingReadPairReader};

use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;

///////////////////////////////
/////////////////////////////// Writer
///////////////////////////////


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
   
    fn writing_done(&mut self) -> anyhow::Result<()> {
        anyhow::Ok(())
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






///////////////////////////////
/////////////////////////////// Streaming reader
///////////////////////////////







pub struct PairedFastqStreamingReadPairReader {

    forward_file: FastqReader<Box<dyn std::io::Read>>,
    reverse_file: FastqReader<Box<dyn std::io::Read>>,

    last_rp: Option<(Vec<u8>,ReadPair)>
}
impl PairedFastqStreamingReadPairReader {
    pub fn new(fname: &PathBuf) -> anyhow::Result<PairedFastqStreamingReadPairReader> {

        
        //Figure out name of R2 from R1
        let fname_r2 = get_fq_filename_r2_from_r1(&fname).unwrap();

        // Open fastq files
        let mut forward_file = getraw::open_fastq(&fname).unwrap();  /////////// TODO detect if fastq or fasta depending on first character
        let mut reverse_file = getraw::open_fastq(&fname_r2).unwrap();

        //Read the first read right away
        let r1 = forward_file.next();
        let r2 = reverse_file.next();

        let rp = if let Some(r1) = r1 {


            let r1= r1.as_ref().expect("Error reading record r1");
            let r2 = r2.unwrap().expect("Error reading record r2");
        
            let (cell_id, umi) = bam::readname_to_cell_umi(r1.head());
            Some((cell_id.to_vec(), ReadPair {
                r1: r1.seq().to_vec(),
                r2: r2.seq().to_vec(),
                q1: r1.qual().to_vec(),
                q2: r2.qual().to_vec(),
                umi: umi.to_vec()
            }))  
        } else {
            //The FASTQ file is empty!
            println!("Warning: empty input BAM");
            None    
        };

        Ok(PairedFastqStreamingReadPairReader {
            forward_file: forward_file,
            reverse_file: reverse_file,
            last_rp: rp
        })


    }
}


impl StreamingReadPairReader for PairedFastqStreamingReadPairReader {

    fn get_reads_for_next_cell(
        &mut self
    ) -> anyhow::Result<Option<ListReadWithBarcode>> {

        //Check if we arrived at the end already
        if let Some((current_cell, last_rp)) = self.last_rp.clone()  {

            //First push the last read pair we had
            let mut reads:Vec<ReadPair> = Vec::new();
            reads.push(last_rp);
            self.last_rp = None;
    
            //Keep reading lines until we reach the next cell or the end
            while let Some(r1) = self.forward_file.next() {
    
                let r1 = r1.expect("Error reading record r1");
                let r2 = self.reverse_file.next().unwrap().expect("Error reading record r2");
            
                let (cell_id, umi) = bam::readname_to_cell_umi(r1.head());

                let rp = ReadPair {
                    r1: r1.seq().to_vec(),
                    r2: r2.seq().to_vec(),
                    q1: r1.qual().to_vec(),
                    q2: r2.qual().to_vec(),
                    umi: umi.to_vec()
                };            
    
                if cell_id == current_cell {
                    //This read belongs to this cell, so add to the list and continue
                    reads.push(rp);
                } else {
                    //This read belongs to the next cell, so stop reading for now
                    self.last_rp = Some((
                        cell_id.to_vec(),
                        rp
                    ));
                    break;
                }
            }

            //Package and return data
            let reads = Arc::new(reads);
            let cellid_reads = (
                String::from_utf8(current_cell).unwrap(), 
                reads
            );

            Ok(Some(Arc::new(cellid_reads)))
        } else {
            //There is nothing more to read
            Ok(None)
        }
    }
   
}







#[derive(Debug,Clone)]
pub struct PairedFastqStreamingReadPairReaderFactory {
}
impl PairedFastqStreamingReadPairReaderFactory {
    pub fn new() -> PairedFastqStreamingReadPairReaderFactory {
        PairedFastqStreamingReadPairReaderFactory {}
    } 
}
impl ConstructFromPath<PairedFastqStreamingReadPairReader> for PairedFastqStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<PairedFastqStreamingReadPairReader> {
        PairedFastqStreamingReadPairReader::new(fname)
    }
}
