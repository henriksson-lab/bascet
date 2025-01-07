use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use super::zip::ZipBascetShardReader;
use super::tirp::TirpBascetShardReader;


///////////////////////////////
/////////////////////////////// The type of the cell ID
///////////////////////////////

pub type CellID = String;



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



///////////////////////////////
/////////////////////////////// Detection of file format
///////////////////////////////


#[derive(Debug,Clone,PartialEq,Eq)]
pub enum DetectedFileformat {
    TIRP,
    ZIP,
    FASTQ,
    BAM,
    Other
}


pub fn detect_shard_format(p: &PathBuf) -> DetectedFileformat {
    let p_string = p.file_name().expect("cannot convert OS string when detecting file format").to_string_lossy();

    if p_string.ends_with("tirp.gz") {
        DetectedFileformat::TIRP
    } else if p_string.ends_with("zip") { 
        DetectedFileformat::ZIP
    } else {
        DetectedFileformat::Other
    }
}


pub fn get_suitable_shard_reader(
    p: &PathBuf, 
    format: &DetectedFileformat
) -> Box::<dyn ShardReader> {
    match format {
        DetectedFileformat::TIRP => Box::new(TirpBascetShardReader::new(&p).expect("Failed to create gascet reader")),
        DetectedFileformat::ZIP => Box::new(ZipBascetShardReader::new(&p).expect("Failed to create bascet reader")),
        _ => panic!("Cannot figure out how to open input file as a shard (could not detect type)")
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



