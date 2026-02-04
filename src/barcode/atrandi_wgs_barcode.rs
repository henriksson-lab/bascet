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

        result.barcode.trim_bcread_len=8+4+8+4+8+4+8+1; //8 barcodes, 3 spacers, and 1 to account for ligation

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

        if score >= 0 {
            //Barcode score seems ok.                    
            //Find overlap. This will be used for trimming. Note that we swap r1 and r2 for compatibility with trimmer code, which assumes barcode in r1
            let overlap = compute_read_overlap(
                r2_seq,
                r1_seq,
            );

            //Get subset ranges from trimming
            let trim_ranges = get_trimmed_ranges(
                self.barcode.trim_bcread_len,
                r2_seq,
                r1_seq,
                overlap
            );

            //Check if read worth keeping still.
            //It might be faster to trim before barcode identification (requires benchmarking).
            //Swap back r1 and r2
            if let Some((range_r2, range_r1))=trim_ranges {
                //Get UMI position
                let umi_range = self.barcode.umi_from..self.barcode.umi_to;

                let tot_len = (range_r1.end - range_r1.start) + (range_r2.end - range_r2.start);
                //println!("----{:?} {:?} {:?}", range_r1, range_r2, umi_range);
            
                //Return trimmed read if long enough
                if tot_len > 30*2 {
                    return (
                        bc,
                        ReadPair {
                            r1: &r1_seq[range_r1.clone()],
                            r2: &r2_seq[range_r2.clone()],
                            q1: &r1_qual[range_r1],
                            q2: &r2_qual[range_r2],
                            umi: &r2_seq[umi_range],
                        },
                    )
                }
            } else {
                //Discard readpairs which overlap too much. These tend to have broken barcodes as well
            }
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











//////////////////////////////////////////////////////////////////////////
/////////////// trimmer code /////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////





/*
likelihood of rando 8bp match: 1.525879e-05
if we allow one error then 0.0004882812  => 0.04882812%
TODO do some polyG detection in addition?
*/




/// Reverse complement ATCGN
/// Using the trick from https://doi.org/10.1101/082214 , extended to handle N
fn revcomp_n(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|c| {
            if c & 2 != 0 {
                if c & 8 != 0 {
                    //N
                    b'N'
                } else {
                    //G or C
                    c ^ 4
                }
            } else {
                //A or T
                c ^ 21
            }
        })
        .collect()
}




pub fn compute_overlap_from_r1(r1_seq: &[u8], r2_seq: &[u8], tryoverlap_pos_r1: usize, tryoverlap_pos_r2: usize, tryoverlap_seq: &[u8]) -> usize {    
    let see_size = tryoverlap_seq.len() + tryoverlap_pos_r2 + tryoverlap_pos_r1;
    let overlap = r1_seq.len() + r2_seq.len() - see_size;
    overlap
}



struct OverlapSearch<'a> {
    tryoverlap_pos_r2: usize,
    tryoverlap_seq: &'a [u8],
}

impl<'a> OverlapSearch<'a> {
    fn compute_overlap(&self, r1_seq: &[u8], r2_seq: &[u8], tryoverlap_pos_r1: usize) -> usize {
        let see_size = self.tryoverlap_seq.len() + self.tryoverlap_pos_r2 + tryoverlap_pos_r1;
        let overlap = r1_seq.len() + r2_seq.len() - see_size;
        overlap
    }

    fn create(r2_seq: &'a [u8], tryoverlap_pos_r2: usize) -> OverlapSearch<'a> {
        let tryoverlap_size = 16;
        let tryoverlap_seq = &r2_seq[(tryoverlap_pos_r2)..(tryoverlap_pos_r2 + tryoverlap_size)];
        OverlapSearch {
            //tryoverlap_size: tryoverlap_size,
            tryoverlap_pos_r2: tryoverlap_pos_r2,
            tryoverlap_seq: tryoverlap_seq
        }
    }

    fn findmin(
        &self, r1_seq_hotencoded: &[u8]
    ) -> Option<(usize, usize)> {  //Return (position, how much left)
        let tryoverlap_seq_rc = revcomp_n(&self.tryoverlap_seq);
        if let Some(pos) = bithack_findmin_final(
            r1_seq_hotencoded, 
            tryoverlap_seq_rc.as_slice()
        ) {            
            Some((pos, r1_seq_hotencoded.len()-pos))
        } else {
            None
        }
    }
}


//Adapter sequence, defined from the perspective of p5. just the first 8bp
//    let seq_p5 = "AATGATACGGCGACCACCGAGATCTACAC";
//    let seq_truseq_r1 = "TCTTTCCCTACACGACGCTCTTCCGATCT";


/// Return overlap, which may be 0
fn compute_read_overlap(
    r1_seq: &[u8],
    r2_seq: &[u8],
) -> usize {
    let r1_seq_hotencoded = onehot_encode_bytes_if(r1_seq);


    //Strategy #1: Find overlap at the end of the barcodes. These are the most important ones to trim.
    //An alternative is to scan for the adapter from the side of R2. Then the adapter could be hot-encoded a single time.
    //But then we would need to hotencode the entire other read as well
    let strat1 = OverlapSearch::create(r2_seq, 8+4+8+4+8+4+8 - 16);

    //Strategy #2: Try to find the tip. This might fail if they overlap too much, so important that this is the second test
    let strat2 = OverlapSearch::create(r2_seq, r2_seq.len() - 16);

    let strat1_pos_r1 = strat1.findmin(r1_seq_hotencoded.as_slice());
    let strat2_pos_r1 = strat2.findmin(r1_seq_hotencoded.as_slice());

    //Two options; 
    let primary_candidate = if let Some((strat1_pos_r1, strat1_left)) = strat1_pos_r1 {
        if let Some((strat2_pos_r1, strat2_left)) = strat2_pos_r1 {
            //Two candidates. We should trust the one that is the furthest from the edge as it is better informed
            if strat1_left>strat2_left {
                Some((strat1, strat1_pos_r1, strat1_left))
            } else {
                Some((strat2, strat2_pos_r1, strat2_left))
            }
        } else {
            //Just one candidate
            Some((strat1, strat1_pos_r1, strat1_left))
        }
    } else {
        if let Some((strat2_pos_r1, strat2_left)) = strat2_pos_r1 {
            //Just one candidate
            Some((strat2, strat2_pos_r1, strat2_left))
        } else {
            //No candidates
            None
        }
    };


    //Trim reads if overlap detected - based on last gDNA part in R2
    if let Some((strat, strat_pos_r1, _strat_left)) = primary_candidate {
        strat.compute_overlap(r1_seq, r2_seq, strat_pos_r1)
    } else {
        //No overlap
        return 0;
    }    
}






/// Given two reads and overlap, return trimmed ranges, or None if to discard reads
/// NOTE!! barcode assumed to be in r1, not r2. This is the opposite of how atrandi works. Swap before and after call
fn get_trimmed_ranges(
    bc_len: usize,
    r1: &[u8],
    r2: &[u8],
    ov: usize
) -> Option<(std::ops::Range<usize>, std::ops::Range<usize>)> {

    let len1=r1.len();
    let len2=r2.len();

    //Handle case of no overlap
    if ov==0 {
        //println!("no overlap");
        return Some((bc_len..len1, 0..len2))
    }

    //Start of R1 is always the same
    let r1_from = bc_len;

    //End of R1 depends on length of R2 if there is overlap
    let mut r1_to = len1;
    if len2 < ov {
        if len1+len2 < bc_len+ov {  // to=len1+len2-ov < bc_len=from
            //R1 barcode region goes beyond end of R2, so fragmentation happened in the barcode
            //println!("Giving up read as fragmentation in the barcode");
            return None;
        } else {
            r1_to = len1+len2-ov;
        }
    }

    //End of R2 depends on length of R1 if there is overlap
    let mut r2_to = len2;
    if len1 < ov + bc_len { // r2_to < len2
        r2_to = len2 - bc_len + len1 - ov;
    }

    //Start of R2 is always the same. Remove first base which might always be A or T from dA-tailing.
    //However, if there is no content, start at 0
    let r2_from=1.min(r2_to);

    Some((
        r1_from..r1_to, 
        r2_from..r2_to
    ))
}





fn onehot_encode_bytes_if(all_inp: &[u8]) -> Vec<u8> {
    let allchar:Vec<u8> = all_inp.iter().map(
        |b| {
            let b=*b;

            let mask_a = 0b0000_0100u8;
            let mask_c = 0b0000_0010u8;
            let mask_t = 0b0000_0001u8;
            let mask_g = 0b0000_1000u8;
            let mask_n = 0b0000_0000u8;

            if b == b'A' {
                mask_a
            } else if b==b'T' {
                mask_t
            } else if b==b'C' {
                mask_c
            } else if b==b'G' {
                mask_g
            } else {
                mask_n
            }
        }
    ).collect();
    allchar
}


fn bithack_findmin_final(allchar: &[u8], scanfor: &[u8]) -> Option<usize> {

    //can break this out
    let scanfor = onehot_encode_bytes_if(scanfor);
    let scanfor_first_u64 = copy_u8_to_u64(&scanfor[0..8]);
    let scanfor_second_u64 = copy_u8_to_u64(&scanfor[8..16]);


    let allchar_first_even16 = &allchar[0..(allchar.len()-16)]; 
    
    ////////////////////// Compare as much as possible using two u64
    let mut all_dist: Vec<(usize, u8)> = Vec::new();
    for (curpos,inp) in allchar_first_even16.windows(16).enumerate() { //keep enumerate. oddly faster (re-check later)
        //Doing 16bp windows and taking first 8bp results in no extra cost at all!
        let inp1 = &inp[0..8];
        let toret = copy_u8_to_u64(inp1);
        let oneret = hamming_sim_onehot_u64_and(scanfor_first_u64,toret); //with this option, N is equally far away from all characters, as other chars. 8 is the maximum value

        //This filter reduces the number of positions to scan for min element later
        if oneret>5 {

            let inp2 = &inp[8..16];
            let toret2 = copy_u8_to_u64(inp2);
            let oneret2 = hamming_sim_onehot_u64_and(scanfor_second_u64,toret2); //with this option, N is equally far away from all characters, as other chars. 8 is the maximum value

            let total_match = oneret+oneret2;

            if total_match > 14 {  // TODO set to 14 later. now disabled
                //println!("bye");
                return Some(curpos);
            }            

            all_dist.push((curpos,total_match));
        }
    };

    //This is possibly the most expensive operation; but if list is short, not so much.
    //If 13bp out of 16bp matched, can call this a match
    if let Some((curpos, score)) = all_dist.iter().max() {
        if *score > 13 {
            return Some(*curpos);
        }
    }

    ////////////////////// Now we cannot use windows anymore. 8 + N bp to compare
    for curpos in (allchar.len()-16)..(allchar.len()-8) {

        let inp1 = &allchar[curpos..(curpos+8)];
        let toret = copy_u8_to_u64(inp1);
        let match1 = hamming_sim_onehot_u64_and(scanfor_first_u64,toret); //with this option, N is equally far away from all characters, as other chars. 8 is the maximum value

        //The code below is truly expensive to run. The cutoff here: 2.8s => 2s
        if match1>5 {

            let remain_len = allchar.len() - curpos - 8;
            let inp2 = &allchar[(curpos+8)..(curpos+8+remain_len)];
            let scan2 = &scanfor[(8)..(8+remain_len)];

            let match2 = hamming_sim_onehot_list_u8_and(inp2,scan2);

            let total_match = match1+match2;

            if total_match > 6+(remain_len as u8) {  // TODO set to 6 later for 2bp mismatch. now disabled
                //println!("bye");
                return Some(curpos);
            }            

            //all_dist.push((curpos,remain_len as u8-total_match));
        }
    }

    // TODO anything with 3 bp match? do we care? only for the first loops?
    // because the end is too short and we don't want to prioritize matches there (earlier is better)
    

    ////////////////////// Only N<8 bp left to test
    for remain_len in (1u8..8).rev() {
        //println!("scanning end {}", matchlen);

        let curpos = allchar.len()-(remain_len as usize);
        let sub_scanfor = &scanfor[0..(remain_len as usize)];
        let sub_read = &allchar[curpos..allchar.len()];
        let score = hamming_sim_onehot_list_u8_and(sub_scanfor, sub_read);
        
        if score >= remain_len - 1 {
            return Some(curpos);
        }
    }

    return None;
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



/// Copy a variable length list of u8 (up to 8bp) into a u64
#[inline(always)]
pub fn copy_var_u8_to_u64(out: &[u8]) -> u64 {  
    let mut arr = [0u8; 8];
    for i in 0..out.len() {
        arr[i] = out[i] << i*8;       
    }
    let toret = u64::from_ne_bytes(arr);
    toret
}


/// Hamming "similarity" - counts the number of agreeing bases. AND version! gives N a different treatment.
/// This function operates on lists of any length
#[inline(always)]
fn hamming_sim_onehot_list_u8_and(a: &[u8], b: &[u8]) -> u8 {
    a.iter().zip(b).map(|(x,y)| (*x & *y).count_ones() as u8).sum()
}


/// Hamming "similarity" - counts the number of agreeing bases. AND version! Gives N a different treatment
/// 8bp i.e. 8 bases
#[inline(always)]
fn hamming_sim_onehot_u64_and(a: u64, b: u64) -> u8 {
    (a & b).count_ones() as u8
}



