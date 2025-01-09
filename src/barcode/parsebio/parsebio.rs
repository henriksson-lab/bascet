
/* 
head bc_data_v1.csv

bci,sequence,uid,well,stype
1,AACGTGAT,pbs_1000,A1,L
2,AAACATCG,pbs_1001,A2,L
3,ATGCCTAA,pbs_1002,A3,L
4,AGTGGTCA,pbs_1003,A4,L
5,ACCACTGT,pbs_1004,A5,L
6,ACATTGGC,pbs_1005,A6,L
7,CAGATCTG,pbs_1006,A7,L
8,CATCAAGT,pbs_1007,A8,L

*/









use super::CombinatorialBarcode;
use super::Chemistry;
use seq_io::fastq::Reader as FastqReader;
use std::io::Cursor;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;
use crate::barcode::CombinatorialBarcode;


#[derive(Clone)]
pub struct ParseBioChemistry {
    barcode: CombinatorialBarcode
}

impl Chemistry for ParseBioChemistry {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        //This could optionally be pre-set !!

        //Read the barcodes relevant for atrandi
//        let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
//        let barcode = CombinatorialBarcode::read_barcodes(Cursor::new(atrandi_bcs));


        //TODO -- try multiple barcode schemes
        //


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

        //Detect barcode, which for parse is in R2 ...? TODO
        let total_distance_cutoff = 1;
        let (isok, bc) = self.barcode.detect_barcode(
            r2_seq,
            false,
            total_distance_cutoff
        );

        if isok {

            let r1_from=0;
            let r1_to=r1_seq.len();
            
            //R2 need to have the first part with barcodes removed. Figure out total size!
            //TODO search for the truseq adapter that may appear toward the end
            let r2_from = 8+4+8+4+8+4+8;
            let r2_to = r2_seq.len();

            

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

impl ParseBioChemistry {

    pub fn new() -> ParseBioChemistry {

        //Detect barcodes later
        ParseBioChemistry {
            barcode: CombinatorialBarcode::new()
        }
    }



    

    pub fn read_barcodes_pb(round: &str, src: impl Read) -> CombinatorialBarcode {

        let mut cb: CombinatorialBarcode = CombinatorialBarcode::new();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(src);
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                format!("{}{}", round, record.well.as_str()), //  also got indexes such as pbs_bcrX39 -- pbs_tcrX17 ; how to couple? get rid of pbs_? use well in name?
                format!("{}{}", round, round),
                format!("{}{}", round, record.seq.as_str())
            );
        }

        if cb.num_pools()==0 {
            println!("Warning: empty barcodes file");
        }
        cb
    }

} 




//bci,sequence,uid,well,stype
//1,AACGTGAT,pbs_1000,A1,L

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct BarcodeCsvFileRow {
    index: u64,
    seq: String,
    name: String,
    well: String,
    bctype: String
}


