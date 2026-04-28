use std::sync::Arc;
use std::{fs::File, num::NonZeroUsize, path::PathBuf};
use tracing::info;

use super::ConstructFromPath;
use super::shard::StreamingReadPairReader;
use crate::fileformat::shard::ReadPair;

use super::CellID;
use noodles::sam::alignment::RecordBuf as BamRecord;

type ListReadWithBarcode = Arc<(CellID, Arc<Vec<ReadPair>>)>;

///////////////////////////////
/// A streaming reader of BAM files, providing read pairs
pub struct BAMStreamingReadPairReader {
    reader: noodles::bam::io::Reader<noodles::bgzf::io::MultithreadedReader<File>>,
    header: noodles::sam::Header,
    last_rp: Option<(Vec<u8>, ReadPair)>,
}
impl BAMStreamingReadPairReader {
    /// Create a new reader from a BAM file
    pub fn new(fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {
        let file = File::open(fname)?;
        let worker_count = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        let bgzf_reader =
            noodles::bgzf::io::MultithreadedReader::with_worker_count(worker_count, file);
        let mut reader = noodles::bam::io::Reader::from(bgzf_reader);
        let header = reader.read_header()?;

        //Read the first read right away
        let mut record = BamRecord::default();
        if reader.read_record_buf(&header, &mut record)? > 0 {
            let last_rp = read_to_readpair(&record);

            Ok(BAMStreamingReadPairReader {
                reader,
                header,
                last_rp: Some(last_rp),
            })
        } else {
            //The BAM file is empty!
            info!("Warning: empty input BAM");

            Ok(BAMStreamingReadPairReader {
                reader,
                header,
                last_rp: None,
            })
        }
    }
}
impl StreamingReadPairReader for BAMStreamingReadPairReader {
    fn get_reads_for_next_cell(&mut self) -> anyhow::Result<Option<ListReadWithBarcode>> {
        //Check if we arrived at the end already
        if let Some((current_cell, last_rp)) = self.last_rp.clone() {
            //First push the last read pair we had
            let mut reads: Vec<ReadPair> = Vec::new();
            reads.push(last_rp);
            self.last_rp = None;

            //Keep reading lines until we reach the next cell or the end
            let mut record = BamRecord::default();
            while self.reader.read_record_buf(&self.header, &mut record)? > 0 {
                let (cell_id, rp) = read_to_readpair(&record);
                if cell_id == current_cell {
                    //This read belongs to this cell, so add to the list and continue
                    reads.push(rp);
                } else {
                    //This read belongs to the next cell, so stop reading for now
                    self.last_rp = Some((cell_id.to_vec(), rp));
                    break;
                }
            }

            //Package and return data
            let reads = Arc::new(reads);
            let cellid_reads = (String::from_utf8(current_cell).unwrap(), reads);

            Ok(Some(Arc::new(cellid_reads)))
        } else {
            //There is nothing more to read
            Ok(None)
        }
    }
}

///////////////////////////////
/// Given the name of a read, divide into cell ID and UMI
pub fn readname_to_cell_umi(read_name: &[u8]) -> (&[u8], &[u8]) {
    let mut splitter = read_name.split(|b| *b == b':');
    let cell_id = splitter
        .next()
        .expect("Could not parse cellID from read name");
    let umi = splitter.next().expect("Could not parse UMI from read name");

    (cell_id, umi)
}

///////////////////////////////
/// Parse one BAM entry to a readpair
fn read_to_readpair(record: &BamRecord) -> (Vec<u8>, ReadPair) {
    /*
        let read_name = record.qname();
        let mut splitter = read_name.split(|b| *b == b':');
        let cell_id = splitter.next().expect("Could not parse cellID from read name");
        let umi = splitter.next().expect("Could not parse UMI from read name");
    */
    let read_name: &[u8] = record.name().expect("missing read name").as_ref();
    let (cell_id, umi) = readname_to_cell_umi(read_name);

    let rp = ReadPair {
        r1: record.sequence().as_ref().to_vec(),
        r2: Vec::new(),
        q1: record
            .quality_scores()
            .as_ref()
            .iter()
            .map(|x| x + 33)
            .collect(),
        q2: Vec::new(),
        umi: umi.to_vec(),
    };

    //println!("got rp: {:?}", rp);

    (cell_id.to_vec(), rp) //this copying hurts a bit...
}

#[derive(Debug, Clone)]
pub struct BAMStreamingReadPairReaderFactory {}
impl BAMStreamingReadPairReaderFactory {
    pub fn new() -> BAMStreamingReadPairReaderFactory {
        BAMStreamingReadPairReaderFactory {}
    }
}
impl ConstructFromPath<BAMStreamingReadPairReader> for BAMStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {
        ///////// maybe anyhow prevents spec of reader?
        BAMStreamingReadPairReader::new(fname)
    }
}
