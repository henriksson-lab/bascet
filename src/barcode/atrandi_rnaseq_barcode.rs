use super::Chemistry;
use super::CombinatorialBarcode;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

///////////////////////////////
/// Atrandi RNA-seq chemistry
#[derive(Clone)]
pub struct AtrandiRNAseqChemistry {
    barcode: CombinatorialBarcode,
}
impl AtrandiRNAseqChemistry {
    pub fn new() -> AtrandiRNAseqChemistry {
        //Read the barcodes relevant for atrandi
        let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(atrandi_bcs));

        AtrandiRNAseqChemistry { barcode: barcode }
    }
}
impl Chemistry for AtrandiRNAseqChemistry {
    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare_using_rp_files(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {
        //This could optionally be pre-set !!

        println!("Preparing to debarcode Atrandi RNA-seq data");

        //Atrandi barcode is in R2
        self.barcode
            .find_probable_barcode_boundaries(fastq_file_r2, 10000)
            .expect("Failed to detect barcode setup from reads");
        Ok(())
    }

    ////////// Detect barcode, and trim if ok
    fn _depreciated_detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8],
    ) -> (bool, CellID, ReadPair) {
        //Truseq primer:
        let _top_adapter = "GATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT"; //5'phos   GATCGGAAGAGCG

        //Detect barcode, which for atrandi barcode is in R2
        let total_score_cutoff = 2; // relaxed comparison
        let part_score_cutoff = 2; // relaxed comparison
        let (isok, bc) =
            self.barcode
                .detect_barcode(r2_seq, true, total_score_cutoff, part_score_cutoff); //parse BC as far as possible

        if isok {
            let _ispcr = "AAGCAGTGGTATCAACGCAGAGT";
            let end_ispcr = "CGCAGAGT".as_bytes();
            let tso = "AAGCAGTGGTATCAACGCAGAGTA"; ///////////// TODO search for this sequence early. if not present, can abort!
            let tso_len = tso.len();
            let umi_len = 8;
            let bc_len = 8 + 4 + 8 + 4 + 8 + 4 + 8;

            //Read should always start with:
            //AAGCAGTGGTATCAACGCAGAGT[A/T]
            //if A, then we are from the TSO direction
            //if T, then we are from the polyA direction

            //Initial part of R1 (gDNA) is always fine
            //TODO R1 must be trimmed as it might go into R2 barcodes; requires aligment with R2
            let r1_from = 0;
            let mut r1_to = r1_seq.len();

            //R2 need to have the first part with barcodes removed. This is 4 barcodes*8, with 4bp spacers.
            //Furthermore, need to remove TSO/ISPCR. these are the same length
            //Then there is a random about of GGG depending on if 5' or 3'
            //polyA may follow if 3'

            //TODO search for the truseq adapter that may appear toward the end
            let mut r2_from = bc_len + tso_len + umi_len + 4 + 3;
            let r2_to = r2_seq.len();

            let _umi = r2_seq[(bc_len + tso_len - 4)..(bc_len + tso_len + umi_len)].to_vec(); //More than needed, but this is to get the T/A indicating if 5' or 3'

            //Only continue if the ISPCR is found (TODO: first search where it is expected); TODO2: ensure there is space after it!
            //Leave enough bases after ISPCR for UMI and the T/A indicator
            let pos_end_ispcr = find_subsequence(&r2_seq[0..(r2_seq.len() - umi_len)], end_ispcr);
            if let Some(pos_end_ispcr) = pos_end_ispcr {
                let pos_end_ispcr = pos_end_ispcr + end_ispcr.len();

                let umi = &r2_seq[pos_end_ispcr..(pos_end_ispcr + 1)];

                if umi[0] == b'T' {
                    ///////// In this case, R2 goes into 3' and the polyA tail

                    //trim initial T's from R2
                    let last_pos_t = scan_last_t(&r2_seq[r2_from..]);
                    r2_from = r2_from + last_pos_t;

                    //At the end of R1, there will be A's. but it might then keep reading into the barcode.
                    //As an approximation, search for a stretch of A's and just terminate
                    let pos_as = find_subsequence(r1_seq, "AAAAA".as_bytes());
                    if let Some(pos_as) = pos_as {
                        r1_to = pos_as;
                    }

                    //Check if enough useful cycles for the read to be kepth. Cutoff set at what we really can align
                    let useful_cycles = r1_to - r1_from + r1_to - r1_from;
                    if useful_cycles > 20 {
                        (
                            true,
                            bc,
                            ReadPair {
                                r1: r1_seq[r1_from..r1_to].to_vec(),
                                r2: r2_seq[r2_from..r2_to].to_vec(),
                                q1: r1_qual[r1_from..r1_to].to_vec(),
                                q2: r2_qual[r2_from..r2_to].to_vec(),
                                umi: umi.to_vec(),
                            },
                        )
                    } else {
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
                } else {
                    ///////// In this case, R2 goes into 5'. The rGrGrG can lead to additional G's it seems. remove these

                    //trim initial G's from R2
                    let last_pos_g = scan_last_g(&r2_seq[r2_from..]);
                    r2_from = r2_from + last_pos_g;

                    (
                        true,
                        bc,
                        ReadPair {
                            r1: r1_seq[r1_from..r1_to].to_vec(),
                            r2: r2_seq[r2_from..r2_to].to_vec(),
                            q1: r1_qual[r1_from..r1_to].to_vec(),
                            q2: r2_qual[r2_from..r2_to].to_vec(),
                            umi: umi.to_vec(),
                        },
                    )
                }

                //#TSO2: AAGCAGTGGTATCAACGCAGAGTA[8bp UMI]ACATrGrG+G    [note: nucleic acid RNA bases, including one LNA. keep stock in -80C. Dilute in NFW]
                //#odt2: AAGCAGTGGTATCAACGCAGAGTT[8bp UMI]ACT30VN
                //#ISPCR: AAGCAGTGGTATCAACGCAGAGT    Tm=69C

                /*
                println!("Input {}", ReadPair{
                    r1: r1_seq.to_vec(),
                    r2: r2_seq.to_vec(),
                    q1: r1_qual.to_vec(),
                    q2: r2_qual.to_vec(),
                    umi: Vec::new()
                });

                println!("Output {}", ReadPair{
                    r1: r1_seq[r1_from..r1_to].to_vec(),
                    r2: r2_seq[r2_from..r2_to].to_vec(),
                    q1: r1_qual[r1_from..r1_to].to_vec(),
                    q2: r2_qual[r2_from..r2_to].to_vec(),
                    umi: umi.to_vec()
                });
                println!("");*/
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

///////////////////////////////
/// Scan string until no more T found. Allow some mismatches.
/// This is for polyT trimming
fn scan_last_t(seq: &[u8]) -> usize {
    let mut pos = 0;
    let mut mismatches = 0;

    loop {
        if seq[pos] == b'T' {
            //Match, keep removing
            pos = pos + 1;
            mismatches = 0;
        } else {
            //Mismatch; some mismatches are ok, as it is best to trim more than less
            mismatches = mismatches + 1;
            if mismatches > 1 {
                break;
            }
            pos = pos + 1;
        }

        //End trimming if out of characters
        if pos == seq.len() {
            break;
        }
    }

    pos
}

///////////////////////////////
/// Scan string until no more G found.
/// This is for G-trimming, after ISPCR
fn scan_last_g(seq: &[u8]) -> usize {
    let mut pos = 0;

    loop {
        if seq[pos] == b'G' {
            //Match, keep removing
            pos = pos + 1;
        } else {
            break;
        }

        //End trimming if out of characters
        if pos == seq.len() {
            break;
        }
    }

    pos
}

///////////////////////////////
/// Find location of subsequence
fn find_subsequence<T>(haystack: &[T], needle: &[T]) -> Option<usize>
where
    for<'a> &'a [T]: PartialEq,
{
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
