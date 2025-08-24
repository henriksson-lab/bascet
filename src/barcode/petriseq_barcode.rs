use super::Chemistry;
use super::CombinatorialBarcode;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

/*
https://tavazoielab.c2b2.columbia.edu/PETRI-seq/

https://teichlab.github.io/scg_lib_structs/methods_html/PETRI-seq.html


5'- AATGATACGGCGACCACCGAGATCTACACNNNNNNNNACACTCTTTCCCTACACGACGCTCTTCCGATCTNNNNNNNNNNNNNNGGTCCTTGGCTTCGCNNNNNNNCCTCCTACGCCAGANNNNNNNXXX.XXXCTGTCTCTTATACACATCTCCGAGCCCACGAGACNNNNNNNNATCTCGTATGCCGTCTTCTGCTTG -3'
3'- TTACTATGCCGCTGGTGGCTCTAGATGTGNNNNNNNNTGTGAGAAAGGGATGTGCTGCGAGAAGGCTAGANNNNNNNNNNNNNNCCAGGAACCGAAGCGNNNNNNNGGAGGATGCGGTCTNNNNNNNXXX.XXXGACAGAGAATATGTGTAGAGGCTCGGGTGCTCTGNNNNNNNNTAGAGCATACGGCAGAAGACGAAC -5'
            Illumina P5           8bp i5          TruSeq Read 1            7-bp   7-bp      Round3      7-bp      Round2     7-bp   cDNA          ME              s7         8bp i7       Illumina P7
                                                                           UMI   Round3     linker     Round2     linker    Round1
                                                                                 barcode               barcode              barcode

*/

#[derive(Clone)]
pub struct PetriseqChemistry {
    barcode: CombinatorialBarcode,
}

impl Chemistry for PetriseqChemistry {
    ////////////////
    ///  Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        _fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {
        //This could optionally be pre-set !!

        //Petri-seq barcode is in R1
        self.barcode
            .find_probable_barcode_boundaries(fastq_file_r1, 10000)
            .expect("Failed to detect barcode setup from reads");
        Ok(())
    }

    ////////////////
    ///  Detect barcode, and trim if ok
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8],
    ) -> (bool, CellID, ReadPair) {
        //Detect barcode, which for atrandi barcode is in R2
        let total_score_cutoff = 2; // relaxed comparison
        let part_score_cutoff = 2;
        let (isok, bc) =
            self.barcode
                .detect_barcode(r1_seq, true, total_score_cutoff, part_score_cutoff); //parse BC as far as possible

        if isok {
            //let me_seq= "CTGTCTCTTATACACATCT"; ///////////// TODO this sequence may appear in R1 at the end. this means that we have hit the other side
            let umi_len = 7;
            let bc_len = umi_len + 7 + 15 + 7 + 14 + 7 + 6; //The last 6 is random hexamer; do not trust this to be correct. UMI also included here

            //TODO search for the ME adapter that may appear toward the end
            let r1_from = bc_len;
            let r1_to = r1_seq.len();

            let umi = r1_seq[0..umi_len].to_vec();

            //Initial part of R2 (cDNA) is always fine
            //TODO R2 must be trimmed as it might go into R1 barcodes; requires aligment with R1
            let r2_from = 0;
            let r2_to = r2_seq.len();

            //if searching in R2 for overlap, could partially scan for CCTCCTACGCCAGA; but round1 barcode is added after and thus pairwise trimming is the only option

            (
                true,
                bc,
                ReadPair {
                    r1: r1_seq[r1_from..r1_to].to_vec(),
                    r2: r2_seq[r2_from..r2_to].to_vec(),
                    q1: r1_qual[r1_from..r1_to].to_vec(),
                    q2: r2_qual[r2_from..r2_to].to_vec(),
                    umi: umi,
                },
            )
        } else {
            //Just return the sequence as-is
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

impl PetriseqChemistry {
    pub fn new() -> PetriseqChemistry {
        //Read the barcodes relevant for atrandi
        let bcs = include_bytes!("petriseq_barcodes.tsv");
        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(bcs));

        PetriseqChemistry { barcode: barcode }
    }
}
