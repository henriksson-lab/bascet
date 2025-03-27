use super::trim_pairwise;
use super::CombinatorialBarcode;
use super::Chemistry;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;
use std::cmp::min;

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

        println!("Preparing to debarcode Atrandi WGS data");

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
            let mut r1_to=r1_seq.len();
            
            //R2 need to have the first part with barcodes removed. 4 barcodes*8, with 4bp spacers
            //TODO search for the truseq adapter that may appear toward the end
            //Add 2bp to barcode to remove dA-tailed part for sure
            let barcode_size = 8+4+8+4+8+4+8 + 2;
            let r2_from = barcode_size;
            let mut r2_to = r2_seq.len();


            //Pick last 10bp of barcode read. Scan for this segment in the gDNA read. Probability of it appearing randomly is 9.536743e-07. but multiply by 150bp to get 0.00014
            //If this part is not present then we can ignore any type of overlap
            let adapter_seq = &r2_seq[(r2_seq.len()-10)..(r2_seq.len())];

            //Revcomp adapter for comparison. It is cheaper to revcomp the adapter than the whole other read
            let adapter_seq_rc = trim_pairwise::revcomp_n(&adapter_seq);

            //Scan gDNA read for adapter
            let adapter_pos = find_subsequence(r1_seq,adapter_seq_rc.as_slice());

            //Trim reads if overlap detected
            if let Some(adapter_pos) = adapter_pos {

                let insert_size = r2_seq.len() + adapter_pos;

                //Discard read pair if it is too small, i.e., it only fits the barcode
                if insert_size<barcode_size {
                    //Just return the sequence as-is
                    return (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()});
                }

                //Trim gDNA read, if it is long enough that it reaches the barcode region
                let max_r1 = insert_size - barcode_size;
                r1_to = min(r1_to, max_r1); 

                //Trim barcode read. This is only needed if it is larger than the insert size
                r2_to = min(r2_to,insert_size);

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
            } 

            //Return trimmed reads
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


fn find_subsequence<T>(haystack: &[T], needle: &[T]) -> Option<usize>
    where for<'a> &'a [T]: PartialEq
{
    haystack.windows(needle.len()).position(|window| window == needle)
}