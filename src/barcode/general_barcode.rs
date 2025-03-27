use super::CombinatorialBarcode;
use super::Chemistry;
use seq_io::fastq::Reader as FastqReader;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;




#[derive(Clone)]
pub struct GeneralCombinatorialBarcode {
    barcode: CombinatorialBarcode
}

impl Chemistry for GeneralCombinatorialBarcode {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        println!("Preparing to debarcode data assuming a general barcode");
        println!("TODO could scan for adapter position to set trimming");

        //Atrandi barcode is in R2
        self.barcode.find_probable_barcode_boundaries(fastq_file_r2, 1000).expect("Failed to detect barcode setup from reads");
        Ok(())
    }
    

    ////////// Detect barcode, and trim if ok
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8]
    ) -> (bool, CellID, ReadPair) {

        //Note that atrandi barcode is in R2
        self.barcode.detect_barcode_and_trim(
            r2_seq,
            r2_qual,
            r1_seq,
            r1_qual
        )

        //TODO support barcodes on one side, or two? for ss3, need to support both!!
    } 



}

impl GeneralCombinatorialBarcode {

    pub fn new(path_bc: &PathBuf) -> GeneralCombinatorialBarcode {

        //Read the barcodes 
        let bc_file = File::open(path_bc).expect("Could not open BC file");
        let reader = BufReader::new(bc_file);
        let barcode = CombinatorialBarcode::read_barcodes(reader);

        GeneralCombinatorialBarcode {
            barcode: barcode
        }
    }


} 