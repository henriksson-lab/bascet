use crate::barcode::Chemistry;
use crate::barcode::CombinatorialBarcode16bp;
use crate::barcode::CombinatorialBarcodePart16bp;
use bascet_core::sequence::R0;
use seq_io::fastq::Reader as FastqReader;

use seq_io::fastq::Record as FastqRecord;

use flate2::read::GzDecoder;

use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::io::{BufRead, BufReader};

#[derive(Clone)]
pub struct TenxRNAChemistry {
    barcode: CombinatorialBarcode16bp,
}

impl Chemistry for TenxRNAChemistry {
    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare_using_rp_files(
        &mut self,
        fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        _fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>,
    ) -> anyhow::Result<()> {

        println!("Loading 10x barcodes");

        //Load the possible barcode systems. Possible to multithread
        let mut map_round_bcs = TenxRNAChemistry::read_chemistries(Cursor::new(include_bytes!(
            "10x_chemistry_def.csv"
        )));

        //TODO enable user to select a chemistry specifically
        //map_round_bcs.retain(|k,_v| k=="WT v2");

        println!("Searching for best barcode match");

        //For each barcode system, try to match it to reads. then decide which barcode system to use.
        //This code is a bit complicated because we wish to compare the same reads for all chemistry options
        let mut map_chem_match_cnt = HashMap::new();
        let n_reads = 100;
        for _cur_read_i in 0..n_reads {
            //Parse bio barcode is in R2
            let record = fastq_file_r1.next().unwrap();
            let record = record
                .expect("Error reading record for checking barcode position; input file too short");

            for (chem_name, bcs) in &map_round_bcs {
                let (isok, _bcm, _score) = bcs.detect_barcode(record.seq(), true, 1, 1);

                //Count reads. Ensure entry for this chemistry is created
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
            let cnt = *map_chem_match_cnt.get(chem_name).unwrap();
            let this_frac = F::from(cnt) / F::from(n_reads);
            println!(
                "Chemistry: {}\tNormalized score: {:.4}",
                chem_name, this_frac
            );
            map_chem_match_frac.insert(chem_name.clone(), this_frac);
        }

        //Pick the best chemistry
        let best_chem_name = map_chem_match_frac.iter().max_by(|a, b| a.1.cmp(&b.1)); ///////// TODO: in case of a tie, should prioritize the smaller chemistry

        //There will always be at least one chemistry to pick
        let (best_chem_name, best_chem_score) = best_chem_name.unwrap();

        println!(
            "Best fitting Parse biosciences chemistry is {}, with a normalized match score of {:.4}",
            best_chem_name, best_chem_score
        );
        //panic!("test");
        self.barcode = map_round_bcs.get(best_chem_name.as_str()).unwrap().clone();

        Ok(())
    }

    fn prepare_using_rp_vecs<C: bascet_core::Composite>(
        &mut self,
        vec_r1: Vec<C>,
        _vec_r2: Vec<C>,
    ) -> anyhow::Result<()>
    where
        C: bascet_core::Get<bascet_core::attr::sequence::R0>,
        <C as bascet_core::Get<bascet_core::attr::sequence::R0>>::Value: AsRef<[u8]>,
    {

        println!("Loading 10x barcodes");

        //Load the possible barcode systems. Possible to multithread
        let mut map_round_bcs = TenxRNAChemistry::read_chemistries(Cursor::new(include_bytes!(
            "10x_chemistry_def.csv"
        )));

        //TODO enable user to select a chemistry specifically
        //map_round_bcs.retain(|k,_v| k=="WT v2");

        println!("Searching for best barcode match");


        //For each barcode system, try to match it to reads. then decide which barcode system to use.
        //This code is a bit complicated because we wish to compare the same reads for all chemistry options
        let mut map_chem_match_cnt = HashMap::new();
        let n_reads = 100;
        
        // 10x barcodes are in r1. 
        for seq in vec_r1.iter().take(n_reads).map(|record| record.as_bytes::<R0>()) {

            for (chem_name, bcs) in &map_round_bcs {
                let (isok, _bcm, _score) = bcs.detect_barcode(seq, true, 1, 1);

                //Count reads. Ensure entry for this chemistry is created
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
            let cnt = *map_chem_match_cnt.get(chem_name).unwrap();
            let this_frac = F::from(cnt) / F::from(n_reads);
            println!(
                "Chemistry: {}\tNormalized score: {:.4}",
                chem_name, this_frac
            );
            map_chem_match_frac.insert(chem_name.clone(), this_frac);
        }

        //Pick the best chemistry
        let best_chem_name = map_chem_match_frac.iter().max_by(|a, b| a.1.cmp(&b.1)); ///////// TODO: in case of a tie, should prioritize the smaller chemistry

        //There will always be at least one chemistry to pick
        let (best_chem_name, best_chem_score) = best_chem_name.unwrap();

        println!(
            "Best fitting Parse biosciences chemistry is {}, with a normalized match score of {:.4}",
            best_chem_name, best_chem_score
        );

        self.barcode = map_round_bcs.get(best_chem_name.as_str()).unwrap().clone();

        Ok(())
    }

    fn detect_barcode_and_trim<'a>(&mut self, r1_seq: &'a[u8], r1_qual: &'a[u8], r2_seq: &'a[u8], r2_qual: &'a[u8])
        -> (u32, crate::common::ReadPair<'a>)
    {
        let total_cutoff = 4;
        let part_cutoff = 1;

        let (bc, cellid, score) = self.barcode.detect_barcode(r1_seq, true, total_cutoff, part_cutoff);

        if score >= 0 {

        }
        
        todo!()
    }

   
}

impl TenxRNAChemistry {
    ///////////////////////////////
    /// Create chemistry. Detect barcodes later
    pub fn new() -> TenxRNAChemistry {
        TenxRNAChemistry {
            barcode: CombinatorialBarcode16bp::new(),
        }
    }

    ///////////////////////////////
    /// Load separate barcode positions. These must be aggregated into full chemistries later
    pub fn load_all_separate_bcs() -> HashMap<String, CombinatorialBarcodePart16bp> {
        let mut map_round_bcs = HashMap::new();

        map_round_bcs.insert(
            "3M-3pgex-may-2023_TRU.txt.gz".to_string(),
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!(
                "3M-3pgex-may-2023_TRU.txt.gz"
            )))),
        );

        map_round_bcs.insert(
            "3M-5pgex-jan-2023.txt.gz".to_string(),
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!(
                "3M-5pgex-jan-2023.txt.gz"
            )))),
        );

        map_round_bcs.insert(
            "737k-arc-v1_rna.txt.gz".to_string(),
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!(
                "737K-arc-v1_rna.txt.gz"
            )))),
        );

        map_round_bcs.insert(
            "3M-february-2018_TRU.txt.gz".to_string(),
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!(
                "3M-february-2018_TRU.txt.gz"
            )))),
        );

        map_round_bcs.insert(
            "737k-august-2016.txt.gz".to_string(),
            TenxRNAChemistry::read_barcodes(GzDecoder::new(Cursor::new(include_bytes!(
                "737K-august-2016.txt.gz"
            )))),
        );

        map_round_bcs
    }

    ///////////////////////////////
    /// Read all barcodes for one round
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcodePart16bp {
        let mut cb = CombinatorialBarcodePart16bp::new();
        let reader = BufReader::new(src);

        let mut cnt = 0;
        for line in reader.lines() {
            let line = line.expect("Could not read barcode file line");
            cb.add_bc(line.as_str(), line.as_str());
            cnt += 1;
        }
        if cnt % 100000 == 0 {
            println!("Read barcode system with count: {}", cnt)
        }

        cb
    }

    ///////////////////////////////
    /// Read all 10x RNA chemistries
    ///
    pub fn read_chemistries(src: impl Read) -> HashMap<String, CombinatorialBarcode16bp> {
        //Get barcodes for each position
        let map_round_bcs = TenxRNAChemistry::load_all_separate_bcs();

        //For each chemistry, build a barcode setup
        let mut chemlist = HashMap::new();

        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(src);
        for result in reader.deserialize() {
            let record: ChemistryDefCsvFileRow = result.unwrap();

            let chemname = record.kit; //format!("{}",record.kit, record.chem);

            let mut bc_setup = CombinatorialBarcode16bp::new();

            let mut bc1 = map_round_bcs
                .get(&record.bc_file)
                .expect("Could not find barcode file for a chemistry")
                .clone();
            bc1.quick_testpos = 0;
            bc1.all_test_pos.push(0);
            bc_setup.add_pool("bc1", bc1);

            //Below is in a bit of the wrong position, since information used in this class!

            //How much to trim
            bc_setup.trim_bcread_len = record.trim1 as usize;

            //UMI position, if any
            bc_setup.umi_from = record.umi_start as usize;
            bc_setup.umi_to = record.umi_end as usize;

            chemlist.insert(chemname, bc_setup);
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
