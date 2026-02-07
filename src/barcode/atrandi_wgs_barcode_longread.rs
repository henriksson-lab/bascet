use std::io::{BufRead, Cursor};
use crate::{barcode::{CombinatorialBarcode8bp}, common::ReadPair};

#[derive(Clone)]
pub struct DebarcodeAtrandiWGSChemistryLongread {
    barcode: CombinatorialBarcode8bp,
}
impl DebarcodeAtrandiWGSChemistryLongread {
    pub fn new() -> Self {
        let mut result = DebarcodeAtrandiWGSChemistryLongread {
            barcode: CombinatorialBarcode8bp::new(),
        };

        let reader = Cursor::new(include_bytes!("../barcode/atrandi_barcodes.tsv"));
        for (index, line) in reader.lines().enumerate() {
            if index == 0 {
                continue;
            }

            let line = line.unwrap();
            let parts: Vec<&str> = line.split('\t').collect();
            result.barcode.add_bc(parts[1], parts[0], parts[2]);
        }

        result.barcode.pools[3].pos_anchor = (8 + 4) * 0;
        result.barcode.pools[3].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[2].pos_anchor = (8 + 4) * 1;
        result.barcode.pools[2].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[1].pos_anchor = (8 + 4) * 2;
        result.barcode.pools[1].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[0].pos_anchor = (8 + 4) * 3;
        result.barcode.pools[0].pos_rel_anchor = vec![0, 1];

        result.barcode.trim_bcread_len=8+4+8+4+8+4+8+1; //8 barcodes, 3 spacers, and 1 to account for ligation

        result
    }
}
impl crate::barcode::Chemistry for DebarcodeAtrandiWGSChemistryLongread {

    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position.
    /// This is not needed for this chemistry (noop)
    fn prepare_using_rp_vecs<C: bascet_core::Composite>(
        &mut self,
        _vec_r1: Vec<C>,
        _vec_r2: Vec<C>,
    ) -> anyhow::Result<()>
    where
        C: bascet_core::Get<bascet_core::attr::sequence::R0>,
        <C as bascet_core::Get<bascet_core::attr::sequence::R0>>::Value: AsRef<[u8]>,
    {
        Ok(())
    }



    ///////////////////////////////
    /// Detect barcode, and trim if ok
    fn detect_barcode_and_trim<'a>(
        &mut self,
        r1_seq: &'a [u8],
        r1_qual: &'a [u8],
        r2_seq: &'a [u8],
        r2_qual: &'a [u8],
    ) -> (u32, crate::common::ReadPair<'a>) {
        //Detect barcode, which here is in R2
        let total_distance_cutoff = 4;
        let part_distance_cutoff = 1;




        //TODO find position of barcode



        //TODO discard if not enough of read left



        //Detect the barcode
        let (bc, score) =
            self.barcode
                .detect_barcode(r2_seq, true, total_distance_cutoff, part_distance_cutoff);

        //Trim the remaining read if the barcode is good enough
        if score >= 0 {



            //TODO detect end of the read



        }

            

        return (
            u32::MAX, //=Discard
            ReadPair {
                r1: &r1_seq,
                r2: &r2_seq,
                q1: &r1_qual,
                q2: &r2_qual,
                umi: &[],
            },
        )
        
    }

}
