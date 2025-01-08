use super::CombinatorialBarcode;
use super::Chemistry;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;


#[derive(Clone)]
pub struct AtrandiRNAseqChemistry {
    barcode: CombinatorialBarcode
}

impl Chemistry for AtrandiRNAseqChemistry {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        //This could optionally be pre-set !!


        //Atrandi barcode is in R2
        self.barcode.find_probable_barcode_boundaries(fastq_file_r2, 10000).expect("Failed to detect barcode setup from reads");
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

        //Truseq primer: 
        let top_adapter = "GATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT"; //5'phos   GATCGGAAGAGCG


        //Detect barcode, which for atrandi barcode is in R2
        let (isok, bc) = self.barcode.detect_barcode(r2_seq);

        if isok {

            let tso= "AAGCAGTGGTATCAACGCAGAGTA";
            let tso_len = tso.len();
            let umi_len = 8;
            let bc_len = 8+4+8+4+8+4+8;

            //Initial part of R1 (gDNA) is always fine
            //TODO R1 must be trimmed as it might go into R2 barcodes; requires aligment with R2
            let r1_from=0;
            let r1_to=r1_seq.len();
            
            //R2 need to have the first part with barcodes removed. This is 4 barcodes*8, with 4bp spacers.
            //Furthermore, need to remove TSO/ISPCR. these are the same length
            //Then there is a random about of GGG depending on if 5' or 3'
            //polyA may follow if 3'

            //TODO search for the truseq adapter that may appear toward the end
            let r2_from = bc_len + tso_len+umi_len + 4 + 3;
            let r2_to = r2_seq.len();

            let umi = r2_seq[(bc_len+tso_len-4)..(bc_len+tso_len+umi_len)].to_vec(); //More than needed, but this is to get the T/A indicating if 5' or 3'

            //#TSO2: AAGCAGTGGTATCAACGCAGAGTA[8bp UMI]ACATrGrG+G    [note: nucleic acid RNA bases, including one LNA. keep stock in -80C. Dilute in NFW]
            //#odt2: AAGCAGTGGTATCAACGCAGAGTT[8bp UMI]ACT30VN    
            //#ISPCR: AAGCAGTGGTATCAACGCAGAGT    Tm=69C
            
            

            (true, bc, ReadPair{
                r1: r1_seq[r1_from..r1_to].to_vec(), 
                r2: r2_seq[r2_from..r2_to].to_vec(), 
                q1: r1_qual[r1_from..r1_to].to_vec(), 
                q2: r2_qual[r2_from..r2_to].to_vec(), 
                umi: umi
            })


        } else {
            //Just return the sequence as-is
            (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()})
        }

    } 

}

impl AtrandiRNAseqChemistry {

    pub fn new() -> AtrandiRNAseqChemistry {

        //Read the barcodes relevant for atrandi
        let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(atrandi_bcs));

        AtrandiRNAseqChemistry {
            barcode: barcode
        }
    }


} 