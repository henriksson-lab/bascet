use seq_io::fastq::Reader as FastqReader;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;
use crate::io::traits::BascetCell;

///////////////////////////////
/// This trait defines a "single cell chemistry" i.e. barcoding, UMI-definition, trimming, etc
pub trait Chemistry {
    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare_using_rp_files(
        &mut self,
        fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {
        unimplemented!();
    }

    fn prepare_using_rp_vecs<C: BascetCell>(
        &mut self,
        _vec_r1: Vec<C>,
        _vec_r2: Vec<C>,
    ) -> anyhow::Result<()> {
        unimplemented!();
    }

    ///////////////////////////////
    /// Detect barcode, and trim if ok
    fn _depreciated_detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8],
    ) -> (bool, CellID, ReadPair) {
        unimplemented!();
    } // get back if ok, cellid, readpair

    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8],
    ) -> (&[u8], crate::common::ReadPair) {
        unimplemented!();
    } // get back if ok, cellid, readpair
}
