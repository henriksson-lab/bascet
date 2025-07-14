use crate::barcode::CombinatorialBarcode16bp;
use crate::barcode::CombinatorialBarcodePart16bp;
use crate::barcode::Chemistry;
use seq_io::fastq::Reader as FastqReader;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

use seq_io::fastq::Record as FastqRecord;

use flate2::read::GzDecoder;

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::io::Read;
use std::io::Cursor;

#[derive(Clone)]
pub struct TenxRNAChemistry {
    barcode: CombinatorialBarcode16bp
}

impl Chemistry for TenxRNAChemistry {




    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        _fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        /*
        let a=str_to_barcode_16bp("ATCGGGGG");
        let b=str_to_barcode_16bp("TTCGGGNN");
        let score=HotEncodeATCGN::bitwise_hamming_distance_u32(a,b);
        println!("{}", score);
        panic!("asdasd");
         */


        println!("Loading 10x barcodes");

        //Load the possible barcode systems. Possible to multithread
        let mut map_round_bcs = TenxRNAChemistry::read_chemistries(
            Cursor::new(include_bytes!("10x_chemistry_def.csv"))
        );

        //TODO enable user to select one specifically
        //map_round_bcs.retain(|k,_v| k=="WT v2");

        println!("Searching for best barcode match");

        //For each barcode system, try to match it to reads. then decide which barcode system to use.
        //This code is a bit complicated because we wish to compare the same reads for all chemistry options
        let mut map_chem_match_cnt = HashMap::new();
        let n_reads = 5000;
        for _ in 0..n_reads {


            //Parse bio barcode is in R2
            let record = fastq_file_r1.next().unwrap();
            let record = record.expect("Error reading record for checking barcode position; input file too short");

            for (chem_name, bcs) in &map_round_bcs {
                //let prt=std::str::from_utf8(record.seq()).unwrap();
                //println!("{:?}",prt);
                //bcs.scan_oneread_barcode_boundaries(&record.seq());

                let mut bcs=bcs.clone(); //ugly hack. remove the meyer algorithm to solve the problem

                let total_distance_cutoff = 4;
                let part_distance_cutoff = 1;
                let (isok, _bc, _score) = bcs.detect_barcode(
                    record.seq(),
                    false,
                    total_distance_cutoff,
                    part_distance_cutoff
                );

                //Count reads. Ensure entry is created
                let e = map_chem_match_cnt.entry(chem_name.clone()).or_insert(0);
                if isok {
                    *e += 1;
                }
            }
        }

        //Using fraction library to simplify code. Seriously overkill in practice
        type F = fraction::Fraction;

        //See how well each barcode system matched
        let mut map_chem_match_frac = HashMap::new();
        for (chem_name, _bcs) in &mut map_round_bcs {

            let cnt=*map_chem_match_cnt.get(chem_name).unwrap();
            let this_frac = F::from(cnt)  / F::from(n_reads);
            println!("PB chemistry {}\tNormalized score: {:.4}", chem_name, this_frac);
            map_chem_match_frac.insert(chem_name.clone(), this_frac);
        }

        //Pick the best chemistry
        let best_chem_name = map_chem_match_frac
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1)); ///////// TODO: in case of a tie, should prioritize the smaller chemistry

        //There will always be at least one chemistry to pick
        let (best_chem_name, best_chem_score) = best_chem_name.unwrap();
        
        println!("Best fitting Parse biosciences chemistry is {}, with a normalized match score of {:.4}", best_chem_name, best_chem_score);
        //panic!("test");
        self.barcode = map_round_bcs.get(best_chem_name.as_str()).unwrap().clone();

        Ok(())
    }
    


    ///////////////////////////////
    /// Detect barcode, and trim if ok
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &[u8],
        r1_qual: &[u8],
        r2_seq: &[u8],
        r2_qual: &[u8]
    ) -> (bool, CellID, ReadPair) {

        //Detect barcode, which for parse is in R1
        let total_distance_cutoff = 4;
        let part_distance_cutoff = 1;
        let (isok, bc, _match_score) = self.barcode.detect_barcode(
            r1_seq,
            false,
            total_distance_cutoff,
            part_distance_cutoff
        );

        //println!("Total score {}", match_score);
        //if match_score>0 {
        //    println!("{}\t{}", match_score, String::from_utf8_lossy(r2_seq));
        //}

        if isok {

            //R1 need to have the first part with barcodes removed. Figure out total size!
            let r1_from = self.barcode.trim_bcread_len;
            let r1_to = r1_seq.len();

            //R2 can be used as-is
            let r2_from=0;
            let r2_to=r2_seq.len();            

            //Get UMI position
            let umi_from = self.barcode.umi_from;
            let umi_to = self.barcode.umi_to;

            (true, bc, ReadPair{
                r1: r1_seq[r1_from..r1_to].to_vec(), 
                r2: r2_seq[r2_from..r2_to].to_vec(), 
                q1: r1_qual[r1_from..r1_to].to_vec(), 
                q2: r2_qual[r2_from..r2_to].to_vec(), 
                umi: r1_seq[umi_from..umi_to].to_vec()})


        } else {
            //Just return the sequence as-is
            (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()})
        }

    } 

}


impl TenxRNAChemistry {

    ///////////////////////////////
    /// Create chemistry. Detect barcodes later
    pub fn new() -> TenxRNAChemistry {
        TenxRNAChemistry {
            barcode: CombinatorialBarcode16bp::new()
        }
    }

    ///////////////////////////////
    /// Load separate barcode positions. These must be aggregated into full chemistries later
    pub fn load_all_separate_bcs() -> HashMap<String, CombinatorialBarcodePart16bp> {

        let mut map_round_bcs = HashMap::new();

        map_round_bcs.insert(
            "3M-3pgex-may-2023_TRU.txt.gz".to_string(), 
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!("3M-3pgex-may-2023_TRU.txt.gz"))))
        );

        map_round_bcs.insert(
            "3M-5pgex-jan-2023.txt.gz".to_string(), 
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!("3M-5pgex-jan-2023.txt.gz"))))
        );

        map_round_bcs.insert(
            "737k-arc-v1_rna.txt.gz".to_string(), 
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!("737k-arc-v1_rna.txt.gz"))))
        );

        map_round_bcs.insert(
            "3M-february-2018_TRU.txt.gz".to_string(), 
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!("3M-february-2018_TRU.txt.gz"))))
        );

        map_round_bcs.insert(
            "737k-august-2016.txt.gz".to_string(), 
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!("737k-august-2016.txt.gz"))))
        );

        map_round_bcs
    }






    ///////////////////////////////
    /// Read all barcodes for one round
    pub fn read_barcodes(
        src: impl Read
    ) -> CombinatorialBarcodePart16bp {

        let mut cb = CombinatorialBarcodePart16bp::new();
        let reader = BufReader::new(src);

        println!("Reading one");
        let mut cnt=0;
        for line in reader.lines() {
            let line = line.expect("Could not read barcode file line");
            //println!("{}",line);
            cb.add_bc(
                line.as_str(),
                line.as_str()
            );
            cnt += 1;

            if cnt%10000 == 0 {
                println!("{}", cnt)
            }
        }

        cb
    }


    ///////////////////////////////
    /// Read all 10x RNA chemistries
    /// 
    pub fn read_chemistries(
        src: impl Read
    ) -> HashMap<String,CombinatorialBarcode16bp> {

        //Get barcodes for each position
        let map_round_bcs = TenxRNAChemistry::load_all_separate_bcs();

        //For each chemistry, build a barcode setup
        let mut chemlist = HashMap::new();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(src);
        for result in reader.deserialize() {
            let record: ChemistryDefCsvFileRow = result.unwrap();

            let chemname = record.kit; //format!("{}",record.kit, record.chem);

            let mut bc_setup = CombinatorialBarcode16bp::new();

            let mut bc1 = map_round_bcs.get(&record.bc_file).expect("Could not find barcode file for a chemistry").clone();
            bc1.quick_testpos = 0;
            bc1.all_test_pos.push(0);
            bc_setup.add_pool(
                "bc1",
                bc1
            );

            //Below is in a bit of the wrong position, since information used in this class!

            //How much to trim
            bc_setup.trim_bcread_len = record.trim1 as usize;

            //UMI position, if any
            bc_setup.umi_from = record.umi_start as usize;
            bc_setup.umi_to = record.umi_end as usize;

            chemlist.insert(chemname,bc_setup);
        }

        chemlist
    }




} 



///////////////////////////////
/// 
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct ChemistryDefCsvFileRow {
    kit: String,
    bc_file: String,
    umi_start: u64,	
    umi_end: u64,
    trim1: u64,  
    notes: String,
}

