use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use super::ZipBascetShardReader;
use super::TirpBascetShardReader;
use super::DetectedFileformat;

///////////////////////////////
/////////////////////////////// The type of the cell ID
///////////////////////////////

pub type CellID = String;
pub type CellUMI = Vec<u8>;



///////////////////////////////
/////////////////////////////// One pair of reads with UMI
///////////////////////////////


#[derive(Debug,Clone)]
pub struct ReadPair {
    pub r1: Vec<u8>,
    pub r2: Vec<u8>,
    pub q1: Vec<u8>,
    pub q2: Vec<u8>,
    pub umi: Vec<u8>
}



///////////////////////////////
/////////////////////////////// Common shard reader trait
///////////////////////////////


pub trait ShardReader {

    fn extract_to_outdir (
        &mut self, 
        cell_id: &CellID, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>;

    fn new(fname: &PathBuf) -> anyhow::Result<Self> where Self: Sized;

    fn get_files_for_cell(&mut self, cell_id: &CellID) -> anyhow::Result<Vec<String>>;

    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>>;
}







//////////////////
/// Try to figure out what cells are present in an input file.           
/// If we cannot list the cells for this file then it will have to stream all the content
pub fn try_get_cells_in_file(p: &PathBuf) -> anyhow::Result<Option<Vec<CellID>>> {
    match crate::fileformat::detect_shard_format(&p) {
        DetectedFileformat::TIRP => {
            let mut f = TirpBascetShardReader::new(&p).expect("Unable to open input TIRP file");
            Ok(Some(f.get_cell_ids().unwrap()))
        },
        DetectedFileformat::ZIP => {




            panic!("TODO")
        },
        DetectedFileformat::FASTQ => {
            Ok(None)
        },
        DetectedFileformat::BAM => {

            //// need separate index file

            panic!("TODO")
        },
        _ => { anyhow::bail!("File format for {} not supported for this operation", p.display()) }

    }
    
}








///////////////////////////////
/////////////////////////////// Histogram for cell barcode counting
///////////////////////////////



#[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct HistogramCsvRow {
    bc: String,
    cnt: u64
}


pub struct BarcodeHistogram {
    histogram: HashMap<CellID, u64>
}
impl BarcodeHistogram {

    pub fn new() -> BarcodeHistogram {
        BarcodeHistogram {
            histogram: HashMap::new()
        }
    }

    pub fn inc(&mut self, cellid: &CellID){        
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += 1;
    }

    pub fn inc_by(&mut self, cellid: &CellID, cnt: &u64){        
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += cnt;
    }

    pub fn add_histogram(&mut self, other: &BarcodeHistogram) {
        for (cellid,v) in other.histogram.iter() {
            let counter = self.histogram.entry(cellid.clone()).or_insert(0);
            *counter += v;    
        }
    }


    pub fn from_file(fname: &PathBuf) -> anyhow::Result<BarcodeHistogram> {

        //Open file
        let file = File::open(fname)?;
        let reader= BufReader::new(file);

        //Read it as a CSV file
        let mut hist = BarcodeHistogram::new();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(reader);
        for result in reader.deserialize() {
            let record: HistogramCsvRow = result.unwrap();
            hist.histogram.insert(record.bc, record.cnt);
        }
        Ok(hist)
    }

    pub fn write(
        &self, 
        fname: &PathBuf
    ) -> anyhow::Result<()> {

            //Open file
            let mut writer = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_path(fname)
                .expect("Could not open histogram file for writing");

            for (bc, cnt) in self.histogram.iter() {
                let _ = writer.serialize(HistogramCsvRow {
                    bc: bc.to_string(),
                    cnt: *cnt
                });
            }

            let _ = writer.flush();
        Ok(())
    }


}



