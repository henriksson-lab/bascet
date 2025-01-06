use std::path::PathBuf;

use super::bascet::BascetShardReader;
use super::gascet::GascetShardReader;


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
    Gascet,
    Bascet,
    Other
}


pub fn detect_shard_format(p: &PathBuf) -> DetectedFileformat {
    let p_string = p.file_name().expect("cannot convert OS string when detecting file format").to_string_lossy();

    if p_string.ends_with("gascet.gz") {
        DetectedFileformat::Gascet
    } else if p_string.ends_with("zip") {
        DetectedFileformat::Bascet
    } else {
        DetectedFileformat::Other
    }
}


pub fn get_suitable_shard_reader(
    p: &PathBuf, 
    format: &DetectedFileformat
) -> Box::<dyn ShardReader> {
    match format {
        DetectedFileformat::Gascet => Box::new(GascetShardReader::new(&p).expect("Failed to create gascet reader")),
        DetectedFileformat::Bascet => Box::new(BascetShardReader::new(&p).expect("Failed to create bascet reader")),
        _ => panic!("Cannot figure out how to open input file as a shard (could not detect type)")
    }
}