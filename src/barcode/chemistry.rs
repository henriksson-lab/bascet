use seq_io::fastq::Reader as FastqReader;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;


///// This module defines a "single cell chemistry" i.e. barcoding, UMI-definition, trimming, etc


pub trait Chemistry {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()>;
    


    ////////// Detect barcode, and trim if ok
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8]
    ) -> (bool, CellID, ReadPair);  // get back if ok, cellid, readpair


}


