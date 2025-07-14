use crate::barcode::CombinatorialBarcode8bp;
use crate::barcode::CombinatorialBarcodePart8bp;
use crate::barcode::Chemistry;
use seq_io::fastq::Reader as FastqReader;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

use seq_io::fastq::Record as FastqRecord;

use flate2::read::GzDecoder;

use std::collections::HashMap;
use std::io::Read;
use std::io::Cursor;



#[derive(Clone)]
pub struct ParseBioChemistry3 {
    barcode: CombinatorialBarcode8bp
}

impl Chemistry for ParseBioChemistry3 {




    ///////////////////////////////
    /// Prepare a chemistry by e.g. fine-tuning parameters or binding barcode position
    fn prepare(
        &mut self,
        _fastq_file_r1: &mut FastqReader<Box<dyn std::io::Read>>,
        fastq_file_r2: &mut FastqReader<Box<dyn std::io::Read>>
    ) -> anyhow::Result<()> {

        /*
        let a=str_to_barcode_8bp("ATCGGGGG");
        let b=str_to_barcode_8bp("TTCGGGNN");
        let score=HotEncodeATCGN::bitwise_hamming_distance_u32(a,b);
        println!("{}", score);
        panic!("asdasd");
         */


        println!("Loading parse barcodes");

        //Load the possible barcode systems
        let mut map_round_bcs = ParseBioChemistry3::read_barcodes_pb(
            Cursor::new(include_bytes!("chemistry_def.csv"))
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
            let record = fastq_file_r2.next().unwrap();
            let record = record.expect("Error reading record for checking barcode position; input file too short");

            for (chem_name, bcs) in &map_round_bcs {

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
            println!("Chemistry: {}\tNormalized score: {:.4}", chem_name, this_frac);
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

        //Detect barcode, which for parse is in R2
        let total_distance_cutoff = 4;
        let part_distance_cutoff = 1;
        let (isok, bc, _match_score) = self.barcode.detect_barcode(
            r2_seq,
            false,
            total_distance_cutoff,
            part_distance_cutoff
        );

        //println!("Total score {}", match_score);
        //if match_score>0 {
        //    println!("{}\t{}", match_score, String::from_utf8_lossy(r2_seq));
        //}

        if isok {

            let r1_from=0;
            let r1_to=r1_seq.len();
            
            //R2 need to have the first part with barcodes removed. Figure out total size!
            //TODO search for the truseq adapter that may appear toward the end
            let r2_from = self.barcode.trim_bcread_len;
            let r2_to = r2_seq.len();

            //Get UMI position
            let umi_from = self.barcode.umi_from;
            let umi_to = self.barcode.umi_to;

            (true, bc, ReadPair{
                r1: r1_seq[r1_from..r1_to].to_vec(), 
                r2: r2_seq[r2_from..r2_to].to_vec(), 
                q1: r1_qual[r1_from..r1_to].to_vec(), 
                q2: r2_qual[r2_from..r2_to].to_vec(), 
                umi: r2_seq[umi_from..umi_to].to_vec()})


        } else {
            //Just return the sequence as-is
            (false, "".to_string(), ReadPair{r1: r1_seq.to_vec(), r2: r2_seq.to_vec(), q1: r1_qual.to_vec(), q2: r2_qual.to_vec(), umi: vec![].to_vec()})
        }

    } 

}


impl ParseBioChemistry3 {

    ///////////////////////////////
    /// Create chemistry. Detect barcodes later
    pub fn new() -> ParseBioChemistry3 {
        ParseBioChemistry3 {
            barcode: CombinatorialBarcode8bp::new()
        }
    }

    ///////////////////////////////
    /// Load separate barcode positions. These must be aggregated into full chemistries later
    pub fn load_all_separate_bcs() -> HashMap<String, CombinatorialBarcodePart8bp> {

        let mut map_round_bcs = HashMap::new();

        map_round_bcs.insert(
            "R3_v3".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_R3_v3.csv.gz"))))
        );

        map_round_bcs.insert(
            "n141_R1_v3_6".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n141_R1_v3_6.csv.gz"))))
        );

        map_round_bcs.insert(
            "n198_v5".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n198_v5.csv.gz"))))
        );

        map_round_bcs.insert(
            "n24_v4".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n24_v4.csv.gz"))))
        );

        map_round_bcs.insert(
            "n299_R1_v3_6".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n299_R1_v3_6.csv.gz"))))
        );

        map_round_bcs.insert(
            "n37_R1_v3_6".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n37_R1_v3_6.csv.gz"))))
        );

        map_round_bcs.insert(
            "n99_v5".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_n99_v5.csv.gz"))))
        );

        map_round_bcs.insert(
            "v1".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_v1.csv.gz"))))
        );

        map_round_bcs.insert(
            "v2".to_string(), 
            ParseBioChemistry3::read_onepos_barcodes_pb(GzDecoder::new(Cursor::new(include_bytes!("bc_data_v2.csv.gz"))))
        );

        map_round_bcs
    }






    ///////////////////////////////
    /// Read all barcodes for one round
    pub fn read_onepos_barcodes_pb(
        src: impl Read
    ) -> CombinatorialBarcodePart8bp {

        let mut cb = CombinatorialBarcodePart8bp::new();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(src);
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                format!("{}", record.bci).as_str(),
                record.sequence.as_str()
            );
        }

        cb
    }


    ///////////////////////////////
    /// Read all parse bio chemistries
    /// 
    /// See kits.py in splitpipe. this chemistry was omitted as files were missing:
    /// mRmP	v3	8x12,16x12	n299_R1_v3_6	r2_megaPlus	r3_megaPlus	x	384
    /// 
    pub fn read_barcodes_pb(
        src: impl Read
    ) -> HashMap<String,CombinatorialBarcode8bp> {

        //Get barcodes for each position
        let map_round_bcs = ParseBioChemistry3::load_all_separate_bcs();

        //For each chemistry, build a barcode setup
        let mut chemlist = HashMap::new();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(src);
        for result in reader.deserialize() {
            let record: ChemistryDefCsvFileRow = result.unwrap();

            let chemname = format!("{} {}",record.kit, record.chem);

            let mut bc_setup = CombinatorialBarcode8bp::new();

            let mut bc1 = map_round_bcs.get(&record.bc1).expect("Could not find file for bc1").clone();
            bc1.quick_testpos = record.bc1pos;
            bc1.all_test_pos.push(record.bc1pos);
            bc_setup.add_pool(
                "bc1",
                bc1
            );

            let mut bc2=map_round_bcs.get(&record.bc2).expect("Could not find file for bc2").clone();
            bc2.quick_testpos = record.bc2pos;
            bc2.all_test_pos.push(record.bc2pos);
            bc_setup.add_pool(
                "bc2",
                bc2
            );

            let mut bc3=map_round_bcs.get(&record.bc3).expect("Could not find file for bc3").clone();
            bc3.quick_testpos = record.bc3pos;
            bc3.all_test_pos.push(record.bc3pos);
            bc_setup.add_pool(
                "bc3",
                bc3
            );


            //Below is in a bit of the wrong position, since information used in this class!

            //How much to trim
            bc_setup.trim_bcread_len = record.trim2;

            //UMI position, if any
            bc_setup.umi_from = 0;
            bc_setup.umi_to = record.umilen;

            chemlist.insert(chemname,bc_setup);
        }

        chemlist
    }




} 


///////////////////////////////
/// File format:
/// 
/// 
/// bci,sequence,uid,well,stype
/// 1,AACGTGAT,pbs_1000,A1,L
/// 
/// stype is L, S, T, R, X#
/// 
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct BarcodeCsvFileRow {
    bci: u64,
    sequence: String,
    uid: String,
    well: String,
    stype: String
}



///////////////////////////////
/// Note that there is a barcode BC4 in the index of the library. The current code assumes that the library has been demultiplexed by bcl2fastq, such that this
/// index has been handled already
/// 
/// https://support.parsebiosciences.com/hc/en-us/articles/14846676930452-What-are-the-run-configuration-and-sequencing-requirements-for-WT-libraries 
/// 
/// Parse WT2  ---- link seems wrong, missing UMI. added below
/// P5  BC4  R1  cDNA  BC1_8bp  L1_22bp  BC2_8bp  L2_30bp  BC3_8bp  UMI_10bp  R2  BC4  P7
/// 
/// L1 appears to be ATCCACGTGCTTGAGACTGTGG           22bp
/// L2 appears to be GTGGCCGATGTTTCGCATCGGCGTACGACT   30bp     10+8+8+8+30+22 = 86bp
/// 
/// 
/// Parse WT3
/// P5  UDI  R1  cDNA  BC1_8bp  L1_12bp  BC2_8bp  L2_12bp  BC3_8bp  UMI_10bp  R2  UDI  P7
/// 
/// L1 appears to be TCCAACCACCTC  12bp
/// L2 appears to be ATGA*GGGTCAG  12bp
/// 
/// Barcodes are as-is, no need to reverse complement
/// 
/// 
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct ChemistryDefCsvFileRow {
    kit: String,
    chem: String,	
    plate_dims: String,	
    bc1: String,	
    bc2: String,	
    bc3: String,	
    ktype: String,	
    ialias: usize,
    umilen: usize,
    trim2: usize,

    bc1pos: usize,
    bc2pos: usize,
    bc3pos: usize,
}

