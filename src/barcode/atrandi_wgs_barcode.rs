use super::CombinatorialBarcode;
use super::Chemistry;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;


// system should suggest combinatorial barcoder!!


// todo prepare barcodes for 10x and parse

// https://lib.rs/crates/rust_code_visualizer   useful for documentation?

#[derive(Clone)]
pub struct AtrandiWGSChemistry {
    barcode: CombinatorialBarcode
}

impl Chemistry for AtrandiWGSChemistry {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        //This could optionally be pre-set !!


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

        //Detect barcode, which for atrandi barcode is in R2
        let total_distance_cutoff = 1; // appears that we can be strict
        let (isok, bc) = self.barcode.detect_barcode(
            r2_seq,
            false,
            total_distance_cutoff
        );

        if isok {

            //Initial part of R1 (gDNA) is always fine
            //TODO R1 must be trimmed as it might go into R2 barcodes; requires aligment with R2
            let r1_from=0;
            let r1_to=r1_seq.len();
            
            //R2 need to have the first part with barcodes removed. 4 barcodes*8, with 4bp spacers
            //TODO search for the truseq adapter that may appear toward the end
            let r2_from = 8+4+8+4+8+4+8;
            let r2_to = r2_seq.len();

            

            (true, bc, ReadPair{
                r1: r1_seq[r1_from..r1_to].to_vec(), 
                r2: r2_seq[r2_from..r2_to].to_vec(), 
                q1: r1_qual[r1_from..r1_to].to_vec(), 
                q2: r2_qual[r2_from..r2_to].to_vec(), 
                umi: vec![].to_vec()})


        } else {
            //Just return the sequence as-is
            (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()})
        }

    } 

}

impl AtrandiWGSChemistry {

    pub fn new() -> AtrandiWGSChemistry {

        //Read the barcodes relevant for atrandi
        let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(atrandi_bcs));

        AtrandiWGSChemistry {
            barcode: barcode
        }
    }


} 