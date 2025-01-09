

//use std::path::PathBuf;

//use crate::fileformat::cell_list_file::


use crate::fileformat::CellID;
use crate::fileformat::ReadPair;



pub trait CellReadPairProcessor {

    fn new(cellid: CellID) -> Self;
    fn process_read(&mut self, rp: ReadPair);

}










