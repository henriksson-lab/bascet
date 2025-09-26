use super::trim_pairwise;
use super::Chemistry;
use super::CombinatorialBarcode;
use seq_io::fastq::Reader as FastqReader;
use std::cmp::min;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

// system should suggest combinatorial barcoder!!

// todo prepare barcodes for 10x and parse

// https://lib.rs/crates/rust_code_visualizer   useful for documentation?

#[derive(Clone)]
pub struct AtrandiWGSChemistry {
    barcode: CombinatorialBarcode,
    num_reads_pass: usize,
    num_reads_fail: usize,
    num_adapt_trim1: usize,
    //num_adapt_trim2: usize
    total_barcode_error_tol: i32,
    part_barcode_error_tol: i32,
}
impl AtrandiWGSChemistry {
    pub fn new(
        total_barcode_error_tol: Option<usize>,
        part_barcode_error_tol: Option<usize>,
    ) -> AtrandiWGSChemistry {
        //Read the barcodes relevant for atrandi
        let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(atrandi_bcs));

        let total_barcode_error_tol = total_barcode_error_tol.unwrap_or(1);
        let part_barcode_error_tol = part_barcode_error_tol.unwrap_or(1);

        AtrandiWGSChemistry {
            barcode: barcode,
            num_reads_pass: 0,
            num_reads_fail: 0,
            num_adapt_trim1: 0,
            total_barcode_error_tol: total_barcode_error_tol as i32,
            part_barcode_error_tol: part_barcode_error_tol as i32,
            //num_adapt_trim2: 0
        }
    }
}
impl Chemistry for AtrandiWGSChemistry {
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {
        //This could optionally be pre-set !!

        println!("Preparing to debarcode Atrandi WGS data");

        //Atrandi barcode is in R2
        self.barcode
            .find_probable_barcode_boundaries(fastq_file_r2, 1000)
            .expect("Failed to detect barcode setup from reads");
        Ok(())
    }

    /// Detect barcode, and trim if ok
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8],
    ) -> (bool, CellID, ReadPair) {
        //Detect barcode, which for atrandi barcode is in R2
        let (isok, bc) = self.barcode.detect_barcode(
            r2_seq,
            false,
            self.total_barcode_error_tol, /////////////////
            self.part_barcode_error_tol,
        );

        if isok {
            //Initial part of R1 (gDNA) is always fine
            //TODO R1 must be trimmed as it might go into R2 barcodes; requires aligment with R2
            let r1_from = 0;
            let mut r1_to = r1_seq.len();

            //R2 need to have the first part with barcodes removed. 4 barcodes*8, with 4bp spacers
            //TODO search for the truseq adapter that may appear toward the end
            //Add 2bp to barcode to remove dA-tailed part for sure
            let barcode_size = 8 + 4 + 8 + 4 + 8 + 4 + 8 + 2;
            let r2_from = barcode_size;
            let mut r2_to = r2_seq.len();

            //Pick last 10bp of barcode read. Scan for this segment in the gDNA read. Probability of it appearing randomly is 9.536743e-07. but multiply by 150bp to get 0.00014.
            //update: 12b, 1bp mismatch
            //If this part is not present then we can ignore any type of overlap
            let overlap_size = 12; //10
            
            let adapter_seq = &r2_seq[(r2_seq.len() - overlap_size)..(r2_seq.len())];

            //Revcomp adapter for comparison. It is cheaper to revcomp the adapter than the whole other read
            let adapter_seq_rc = trim_pairwise::revcomp_n(&adapter_seq);

            //Scan gDNA read for adapter
            let adapter_pos = find_subsequence_mismatch(r1_seq, adapter_seq_rc.as_slice(), 1);

            //Trim reads if overlap detected - based on last gDNA part in R2
            if let Some(adapter_pos) = adapter_pos {
                self.num_adapt_trim1 += 1;

                let insert_size = r2_seq.len() + adapter_pos;

                //Discard read pair if it is too small, i.e., it only fits the barcode
                if insert_size < barcode_size {
                    //Just return the sequence as-is
                    return (
                        false,
                        "".to_string(),
                        ReadPair {
                            r1: r1_seq.to_vec(),
                            r2: r2_seq.to_vec(),
                            q1: r1_qual.to_vec(),
                            q2: r2_qual.to_vec(),
                            umi: vec![].to_vec(),
                        },
                    );
                }

                //Trim gDNA read, if it is long enough that it reaches the barcode region
                let max_r1 = insert_size - barcode_size;
                r1_to = min(r1_to, max_r1);

                //Trim barcode read. This is only needed if it is larger than the insert size
                r2_to = min(r2_to, insert_size);
            }

            /*

            /////////////// significant slowdown. using fastp anyway

            //If the insert is too small then the previous trick may not work,
            //as the sought sequence at the end of one read will be beyond
            //the other read primer adapter site. Thus, we should also attempt
            //to also just find the adapters

            let _adapter_fragment_full = b"GATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT";
            let adapter_fragment       = b"GATCGGAAGAGC";

            //Can scan for start of R1 (DNA) in R2 (holding barcodes), to prove the end position
            //let dna_end_seq = &r1_seq[0..12];
            //let dna_end_seq_rc = trim_pairwise::revcomp_n(&dna_end_seq);

            //Scan gDNA read for adapter
            let dna_end_pos = find_subsequence_mismatch(
                r2_seq,
                adapter_fragment.as_slice(),
                1);

            //See if this trims the read even more than previous scans
            if let Some(dna_end_pos) = dna_end_pos {
                self.num_adapt_trim2 += 1;

                let new_r2_to = adapter_fragment.len() + dna_end_pos;
                if new_r2_to < r2_to {

                    //It may still happen that the read is cropped.
                    //Just give up in such case
                    if new_r2_to<barcode_size {
                        return (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()})
                    } else {
                        r2_to = new_r2_to;

                        let insert_size = r2_to - barcode_size;
                        r1_to = min(r1_to, insert_size);
                    }
                }
            }

            */
            /*
            println!("Trim stats: {}\t{}\t{}\t{}",
                self.num_reads_pass,
                self.num_reads_fail,
                self.num_adapt_trim1,
                self.num_adapt_trim2,
            );*/

            //Return trimmed reads
            self.num_reads_pass += 1;
            (
                true,
                bc,
                ReadPair {
                    r1: r1_seq[r1_from..r1_to].to_vec(), // range end index 129 out of range for slice of length 128
                    r2: r2_seq[r2_from..r2_to].to_vec(),
                    q1: r1_qual[r1_from..r1_to].to_vec(),
                    q2: r2_qual[r2_from..r2_to].to_vec(),
                    umi: vec![].to_vec(),
                },
            )
        } else {
            //If barcode is bad, just return the sequence as-is
            self.num_reads_fail += 1;
            (
                false,
                "".to_string(),
                ReadPair {
                    r1: r1_seq.to_vec(),
                    r2: r2_seq.to_vec(),
                    q1: r1_qual.to_vec(),
                    q2: r2_qual.to_vec(),
                    umi: vec![].to_vec(),
                },
            )
        }
    }
}

pub fn find_subsequence<T>(haystack: &[T], needle: &[T]) -> Option<usize>
where
    for<'a> &'a [T]: PartialEq,
{
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn find_subsequence_mismatch(
    haystack: &[u8],
    needle: &[u8],
    allow_mismatches: u8,
) -> Option<usize> {
    // Modelled after https://github.com/OpenGene/fastp/blob/master/src/adaptertrimmer.cpp  AdapterTrimmer::trimBySequence
    // surprisingly there is no attempt to make this fast

    for pos in 0..(haystack.len() - needle.len()) {
        let mut mismatch = 0;
        let mut is_match = true;
        for i in 0..needle.len() {
            if needle[i] != haystack[pos + i] {
                mismatch += 1;
                if mismatch > allow_mismatches {
                    is_match = false;
                    break;
                }
            }
        }
        if is_match {
            return Some(pos);
        }
    }
    None
}

/*
https://gist.github.com/photocyte/3edd9401d0b13476e60f8b104c2575f8

>TruSeq Universal Adapter
AATGATACGGCGACCACCGAGATCTACACTCTTTCCCTACACGACGCTCTTCCGATCT

(base) mahogny@beagle:/husky/henriksson/atrandi/v2_wgs_novaseq1/temp$ zcat asfq.1.R2.fq.gz | grep ACACGACGCTCTTCCGA
CCTCGCGCGACCGCTGGATGGTCACGGCCTGCGCCAGCTGCGTCTCCCAGAGCGGGACCGTGTTGACGAGGGTCGAGTTGATCCGCGTGACCAGCGCCTTGTCGTTCTCCTACACGACGCTCTTCCGATCT


fastqc claims to find it
fastqc asfq.1.R1.fq.gz asfq.1.R2.fq.gz -t 5

/husky/henriksson/atrandi/v2_wgs_novaseq1/temp   suspicious seq: ACACGACGCTCTTCCGA

                                                                                                           ___________________________
   CCTCGCGCGACCGCTGGATGGTCACGGCCTGCGCCAGCTGCGTCTCCCAGAGCGGGACCGTGTTGACGAGGGTCGAGTTGATCCGCGTGACCAGCGCCTTGTCGTTCTCCTACACGACGCTCTTCCGATCT
(base) mahogny@beagle:/husky/henriksson/atrandi/v2_wgs_novaseq1/temp$ zcat asfq.1.R1.fq.gz |                 grep ACACGACGCTCTTCCGA
                 AACTACAATCGGTTACCCTTCCATAGCAGAGTTAGTAGCGTCCTAGTCTCACAGATCGGAAGAGCACACGTCTGAACTCCAGTCACCCTCTTCCCCTACACGACGCTCTTCCGATCTAACCAAAAGAG
                                                                                             AAAGTCTCACCCTCTTTCCCTACACGACGCTCTTCCGATCTAAGGTGGGAGCTCCCGTCGTAAAGCGTGTTAAGTTGGACACCGGGCAGCACATGGCCCCCGTTCTCCTCGTAAATGATAAAAATTTC
     CGCACCCTCCCACTCTGGCCCTCACCTTGTCCTCACCAGTTAACTCGGCAGCGCACCCCTCCTAACTCCCGCCACCACCCCACCTCGCAAACGTGCCCCCTCTTTCCCCACACGACGCTCTTCCGATC
                               GTCCTTACAGTTCCGAGTTCAAGGTGTCCTGGCTGATAAGATCGGAAGAGCACACGTCTGAACTCCAGTCACCCTCTTCCCCTACACGACGCTCTTCCGATCTGCCCGTAAAGGTGAGGGGGGGGGGG
   CCCCACCCCGCTCCTCCCCTTCCACCACACCCCCCCCCCCCACATCACAACCTCACCCGTCCCCCCTCCCGCCACCAAGCCACCTCCGCTGACCACGTCCCCTCTTTCCCTACACGACGCTCTTCCGA
   CCCCACCCCGCTCCTCCCCTTCCACCACACCCCCCCCCCCCACATCACAACCTCACCCGTCCCCCCTCCCGCCACCAAGCCACCTTCGCTGACCCCGTCCCCTCTTTCCCCACACGACGCTCTTCCGA

*/

/*
println!();
println!("detect overlap, insert size {},  r1_from {} r1_to {},        r2_from {} r2_to {},    ad_pos {}", insert_size, r1_from, r1_to, r2_from, r2_to, adapter_pos);
let rp = ReadPair{
    r1: r1_seq.to_vec(),
    r2: trim_pairwise::revcomp_n(r2_seq),
    q1: r1_qual.to_vec(),
    q2: r2_qual.to_vec(),
    umi: vec![].to_vec()
};

let rp_trim = ReadPair{
    r1: r1_seq[r1_from..r1_to].to_vec(),
    r2: trim_pairwise::revcomp_n(&r2_seq[r2_from..r2_to]),
    q1: r1_qual[r1_from..r1_to].to_vec(),
    q2: r2_qual[r2_from..r2_to].to_vec(),
    umi: vec![].to_vec()
};

println!("{}", rp);
println!("{}", rp_trim);
*/
