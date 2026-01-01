// use crate::command::getraw::GetRawChemistry;

///////////////////////////////
/// This trait defines a "single cell chemistry" i.e. barcoding, UMI-definition, trimming, etc
///
#[enum_dispatch::enum_dispatch]
pub trait Chemistry {
    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare_using_rp_files(
        &mut self,
        fastq_file_r1: &mut seq_io::fastq::Reader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut seq_io::fastq::Reader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {
        unimplemented!();
    }

    fn prepare_using_rp_vecs<C: bascet_core::Composite>(
        &mut self,
        _vec_r1: Vec<C>,
        _vec_r2: Vec<C>,
    ) -> anyhow::Result<()>
    where
        C: bascet_core::Get<bascet_core::R0>,
        <C as bascet_core::Get<bascet_core::R0>>::Value: AsRef<[u8]>,
    {
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
    ) -> (
        bool,
        crate::fileformat::shard::CellID,
        crate::fileformat::shard::ReadPair,
    ) {
        unimplemented!();
    } // get back if ok, cellid, readpair

    fn detect_barcode_and_trim<'a>(
        &mut self,
        r1_seq: &'a [u8],
        r1_qual: &'a [u8],
        r2_seq: &'a [u8],
        r2_qual: &'a [u8],
    ) -> (u32, crate::common::ReadPair<'a>) {
        unimplemented!();
    } // get back if ok, cellid, readpair

    fn bcindexu32_to_bcu8(&self, index32: &u32) -> Vec<u8> {
        unimplemented!()
    }
}
