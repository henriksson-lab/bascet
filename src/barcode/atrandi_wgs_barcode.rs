use std::io::{BufRead, Cursor};
use crate::{barcode::{CombinatorialBarcode8bp}, common::ReadPair};
use blart::AsBytes;

#[derive(Clone)]
pub struct DebarcodeAtrandiWGSChemistry {
    barcode: CombinatorialBarcode8bp,
}
impl DebarcodeAtrandiWGSChemistry {
    pub fn new() -> Self {
        let mut result = DebarcodeAtrandiWGSChemistry {
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

        result
    }
}
impl crate::barcode::Chemistry for DebarcodeAtrandiWGSChemistry {

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

        let (bc, score) =
            self.barcode
                .detect_barcode(r2_seq, true, total_distance_cutoff, part_distance_cutoff);

        match score {
            0.. => {
                //R2 need to have the first part with barcodes removed. Figure out total size!
                let r2_from = self.barcode.trim_bcread_len;
                let r2_to = r2_seq.len();

                //Get UMI position
                let umi_from = self.barcode.umi_from;
                let umi_to = self.barcode.umi_to;
                (
                    bc,
                    ReadPair {
                        r1: &r1_seq,
                        r2: &r2_seq[r2_from..r2_to],
                        q1: &r1_qual,
                        q2: &r2_qual[r2_from..r2_to],
                        umi: &r2_seq[umi_from..umi_to],
                    },
                )
            }
            ..0 => {
                //Discard the read pair
                (
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
    }

    fn bcindexu32_to_bcu8(&self, index32: &u32) -> Vec<u8> {
        let mut result = Vec::new();
        let bytes = index32.as_bytes();
        result.extend_from_slice(
            self.barcode.pools[0].barcode_name_list[bytes[3] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[1].barcode_name_list[bytes[2] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[2].barcode_name_list[bytes[1] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[3].barcode_name_list[bytes[0] as usize].as_bytes(),
        );

        return result;
    }
}
