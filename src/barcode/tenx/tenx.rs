

// /data/henlab/software/cellranger-7.1.0/lib/python/cellranger/barcodes$ 


/* 

// https://kb.10xgenomics.com/hc/en-us/articles/115004506263-What-is-a-barcode-whitelist 

not all are included this second

Single Cell 3' v4:   
not included yet

Single Cell 3' v3, Single Cell 3' v3.1, Single Cell 3' HT v3.1:
3M-febrary-2018.txt.gz

Single Cell Multiome (ATAC+GEX) v1:
737k-arc-v1.txt.gz	

Single Cell 5' v3
3M-5pgex-jan-2023.txt.gz

Single Cell 3' v2, Single Cell 5' v1 and v2, Single Cell 5' HT v2
737k-august-2016.txt	

Single Cell 3' v1 
737k-april-2014_rc.txt	

Single Cell Multiome (ATAC+GEX) v1
737k-arc-v1.txt.gz	

Single Cell ATAC 
737-cratac-v1.txt.gz

Single Cell 3' LT
9K-LT-march-2021.txt.gz	

(Present starting from Cell Ranger v7.0)
737k-fixed-rna-profiling.txt.gz	Fixed RNA Profiling 

*/




use std::io::{self, BufRead};

use flate2::read::GzDecoder;


use seq_io::fastq::Reader as FastqReader;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;
use crate::barcode::Chemistry;
use crate::barcode::CombinatorialBarcode;


#[derive(Clone)]
pub struct TenxChemistry {
    barcode: CombinatorialBarcode
}

impl Chemistry for TenxChemistry {

    ////// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        //TODO -- try multiple barcode schemes

        self.barcode = read_barcodes_10x(include_bytes!("3M-february-2018.txt.gz"));


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

impl TenxChemistry {
    pub fn new() -> TenxChemistry {

        //Detect barcodes later
        TenxChemistry {
            barcode: CombinatorialBarcode::new()
        }
    }  
} 



    
fn read_barcodes_10x(bc_bytes: &[u8]) -> CombinatorialBarcode { ///// todo overkill to use the combinatorial system!!

    let mut cb: CombinatorialBarcode = CombinatorialBarcode::new();

    //Read the barcodes
    let gz = GzDecoder::new(&bc_bytes[..]);
    let lines = io::BufReader::new(gz).lines();
    let poolname = "p";
    for bc in lines.flatten() {
        cb.add_bc(
            bc.as_str(),//.to_vec(),
            poolname,//.to_string().clone(),
            bc.as_str(),//.to_vec()
        );
    }

    if cb.num_pools()==0 {
        println!("Warning: empty barcodes file");
    }
    cb
}

