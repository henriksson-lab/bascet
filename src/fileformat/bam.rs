use std::sync::Arc;
use std::path::PathBuf;

use super::ConstructFromPath;
use crate::fileformat::shard::ReadPair;
use super::shard::StreamingReadPairReader;

use rust_htslib::bam::Read;
use super::CellID;
use rust_htslib::bam::record::Record as BamRecord;




#[derive(Debug)]
pub struct BAMStreamingReadPairReader {
    reader: rust_htslib::bam::Reader,
    last_rp: Option<(Vec<u8>,ReadPair)>,
}
impl BAMStreamingReadPairReader {
    pub fn new(fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {

        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads
        let mut reader = rust_htslib::bam::Reader::from_path(&fname)?;

        //Activate multithreaded reading
        //bam.set_threads(params.num_threads).unwrap();   //TODO: how to set this well? use other library that has a shared rayon pool of threads instead?

        //Read the first read right away
        let mut record = BamRecord::new();
        if let Some(_r) = reader.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //let header = self.reader.header();
            let last_rp = read_to_readpair(&record);

            Ok(BAMStreamingReadPairReader {
                reader: reader,
                last_rp: Some(last_rp)
            })
        } else {
            //The BAM file is empty!
            println!("Warning: empty input BAM");

            Ok(BAMStreamingReadPairReader {
                reader: reader,
                last_rp: None
            })
    
        }


    }
}


////////////////////////
/// Parse one BAM entry to a readpair
fn read_to_readpair(
    record: &BamRecord
) -> (Vec<u8>, ReadPair) {

    let read_name = record.qname();
    let mut splitter = read_name.split(|b| *b == b':'); 
    let cell_id = splitter.next().expect("Could not parse cellID from read name");

    let umi = splitter.next().expect("Could not parse UMI from read name");

    let rp = ReadPair {
        r1: record.seq().as_bytes(),
        r2: Vec::new(),
        q1: record.qual().to_vec(),
        q2: Vec::new(),
        umi: umi.to_vec()
    };

    (cell_id.to_vec(), rp)  //this copying hurts a bit...
}


type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;


impl StreamingReadPairReader for BAMStreamingReadPairReader {


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
            let mut record = BamRecord::new();
            while let Some(_r) = self.reader.read(&mut record) {
                let (cell_id, rp) = read_to_readpair(&record);
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
pub struct BAMStreamingReadPairReaderFactory {
}
impl BAMStreamingReadPairReaderFactory {
    pub fn new() -> BAMStreamingReadPairReaderFactory {
        BAMStreamingReadPairReaderFactory {}
    } 
}
impl ConstructFromPath<BAMStreamingReadPairReader> for BAMStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {  ///////// maybe anyhow prevents spec of reader?
        BAMStreamingReadPairReader::new(fname)
    }
}


