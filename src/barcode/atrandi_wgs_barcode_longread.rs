use std::io::{BufRead, Cursor};
use blart::AsBytes;
//use tracing::{info};

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

        //result.barcode.trim_bcread_len=8+4+8+4+8+4+8+1; //8 barcodes, 3 spacers, and 1 to account for ligation

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


        // /husky/henriksson/atrandi/rawdata/cleanbar_longread
        // Example index primer p7: UDP0005_p7	CAAGCAGAAGACGGCATACGAGAT TAATCTCGTC GTGACTGGAGTTCAGACGTGTGCTCTT
        // generic P5	AATGATACGGCGACCACCGAGATCT    ACACTCTTTCCCTAC ACGAC

        // Adapters confirmed in cleanbar fig https://academic.oup.com/view-large/figure/530094580/ycaf134f2.tif  
        // https://pubmed.ncbi.nlm.nih.gov/40860566/
        // zcat SRR31758484.fastq.gz | grep CTTCCGATCT........AGGA........ACTC........AAGG........T
        // zcat SRR31758484.fastq.gz | grep ........AGGA........ACTC........AAGG........T
        // ==> reverse complement
        // zcat SRR31758484.fastq.gz | grep T........CCTT........GAGT........TCCT........ # AGATC
        // these fragments are rare in cleanbar. depends on how longreads where made .. methods are unclear. possible that amplicons were made, all in the same orientation

        // zcat SRR31758484.fastq.gz | grep CTTCCGATCT........AGGA........ACTC........AAGG........T
        // 83 results (1000 possible)
        // zcat SRR31758484.fastq.gz | grep ........AGGA........ACTC........AAGG........T
        // 112 results (1000 possible)
        // From https://academic.oup.com/ismecommun/article/5/1/ycaf134/8220722 , it appears that no final PCR was done.
        // This goes against the Atrandi manual recommendation and thus many failed adapters are expected.
        // As longread should not be done in this manner and we should not attempt to support non-recommended solutions.

        // We can scan for up to 13bp of just linkers
        // Prob of random hit: 13bp (1/4)**13 = 1.490116e-08
        // This is highly unlikely and to ensure we do not miss reads, we need to do some fuzzy searching. Already scanning for AGGA...ACTC would be sufficient (p=1.5-e5; 0.15 such matches in 10kb)

        
        // Scan for the position of barcode
        let scan_bc = b"\0\0\0\0AGGAACTCAAGG"; //4x u32
        let scan_bc_u64 = copy_u8_to_u64(scan_bc);
        let bc_len = 8+4+8+4+8+4+8+1;

        //info!("scanning a long read");

        //Scanning too far can be costly, so limit search to a sensible range
        let r1_possible_bc = &r1_seq[0..300.min(r1_seq.len())];
        'linker_scan: for (curpos,inp) in r1_possible_bc.windows(bc_len).enumerate() { 

            let ad1 = &inp[(0+8 )..(4+8 )];
            let ad2 = &inp[(0+20)..(4+20)];
            let ad3 = &inp[(0+32)..(4+32)];

            let ad1 = copy_u8_to_u32(ad1) as u64;
            let ad2 = copy_u8_to_u32(ad2) as u64;
            let ad3 = copy_u8_to_u32(ad3) as u64;

            let all_ad = ad1<<16 | ad2<<8 | ad3;
            let adapter_score = hamming_u64(all_ad, scan_bc_u64);

            //Expected similarity: letters A-T are in the range dec 65 .. dec 74 ; 1000001 .. 1001010   so there are 4 bits that can change
            //Least similarity (ish): 4*3+8 = 20. Highest: 32
            if adapter_score >= 29 {

                //Detect barcode at this position
                let total_distance_cutoff = 4;
                let part_distance_cutoff = 1;
                let (bc, bc_score) = self.barcode.detect_barcode(inp, true, total_distance_cutoff, part_distance_cutoff);

                //info!("Possible longread barcode at position {}", curpos);

                //Trim the remaining read if the barcode is good enough
                if bc_score >= 0 {
                    //At this point we need to detect where the read ends.
                    //Note that the final barcode can be /very/ incomplete the way it was done previously, https://academic.oup.com/ismecommun/article/5/1/ycaf134/8220722?login=false
                    //This also makes scanning for the end slow and error prone. Instead, we simply trim the final 80 bp, which is margin beyond the expected adapter size (55bp)

                    //Ensure fragment is long enough
                    let end_trim = 80;
                    if r1_seq.len() < end_trim {
                        break 'linker_scan;
                    }

                    //Stop if there is no insert left
                    let seq_from = r1_seq.len().min(curpos+bc_len);
                    let seq_to = r1_seq.len()-end_trim;
                    if seq_to < seq_from {
                        break 'linker_scan;
                    }

                    return (
                        bc, 
                        ReadPair {
                            r1: &r1_seq[seq_from..seq_to],
                            r2: &r2_seq,
                            q1: &r1_qual[seq_from..seq_to],
                            q2: &r2_qual,
                            umi: &[],
                        },
                    )

                } else if adapter_score==32 {
                    //If we found a perfect linker hit then we are done, even if the barcode was bad. Otherwise we should continue scanning
                    break 'linker_scan;
                }

            }
        }


        //TODO discard if not enough of read left
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


/// Copy a list of u8 into a u32 (almost a transmute).
/// This function is as fast as if the size was explicitly given; size is likely added on top during inlining
#[inline(always)]
fn copy_u8_to_u32(out: &[u8]) -> u32 {
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&out[0..4]);
    let toret = u32::from_ne_bytes(arr);
    toret
}



/// Copy a list of u8 into a u64 (almost a transmute).
/// This function is as fast as if the size was explicitly given; size is likely added on top during inlining
#[inline(always)]
fn copy_u8_to_u64(out: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&out[0..8]);
    let toret = u64::from_ne_bytes(arr);
    toret
}

#[inline(always)]
fn hamming_u64(a: u64, b: u64) -> u8 {
    (a ^ b).count_ones() as u8
}
