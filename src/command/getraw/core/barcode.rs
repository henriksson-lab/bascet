use anyhow::bail;

//use log::info;
use log::debug;
//use log::warn;
//use log::trace;


use std::collections::HashMap;

use bio::alignment::Alignment;
use bio::pattern_matching::myers::Myers;

use std::path::PathBuf;

use seq_io::fasta::Record as FastaRecord;
use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};

use itertools::Itertools;



#[derive(Clone, Debug)]
pub struct Barcode {
    pub name: String,
    pub sequence: String,
    pub pattern: Myers<u64>, //BitVector computed once and for all .. or move outside as it mutates??
}
impl Barcode {

    pub fn new(
        name: &str,
        sequence: &str,
    ) -> Barcode {
        Barcode {
            name: name.to_string(),
            sequence: sequence.to_string(),
            pattern: Myers::<u64>::new(sequence.as_bytes()),
        }
    }


    // Note: Mutatable because it modifies the Myers precalculated matrix
    //// returns: name, start, score
    pub fn seek_one( 
        &mut self,
        record: &[u8],
        max_distance: u8,
    ) -> Option<(&String, usize, i32)> {  
        // use Myers' algorithm to find the barcodes in a read
        // Ref: Myers, G. (1999). A fast bit-vector algorithm for approximate string
        // matching based on dynamic programming. Journal of the ACM (JACM) 46, 395–415.
        //let mut hits: Vec<(&String, Vec<u8>, usize, i32)> = Vec::new();
        let mut aln = Alignment::default();
        let mut matches = self.pattern.find_all_lazy(record, max_distance);

        // Return the best hit, if any
        let min_key = matches.by_ref().min_by_key(|&(_, dist)| dist);
        
        if let Some((best_end,_)) = min_key {
            // Calculate the alignment
            matches.alignment_at(best_end, &mut aln);

            Some((
                &self.name,
                aln.ystart,
                aln.score,
            ))
        } else {
            None
        }

    }

}







#[derive(Clone, Debug)]
pub struct CombinatorialBarcoding {

    pub names: Vec<String>,
    pub map_name_to_index: HashMap<String,usize>,
    pub pools: Vec<BarcodePool>

}
impl CombinatorialBarcoding {

    pub fn new() -> CombinatorialBarcoding {

        CombinatorialBarcoding{
            names: vec![],
            map_name_to_index: HashMap::new(),
            pools: vec![]
        }
    }

    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

    pub fn add_bc(
        &mut self,
        name: &str,
        poolname: &str,
        sequence: &str
    )  {

        //Create new pool if needed
        //let &mut pool = player_stats.entry(poolname).or_insert(BarcodePool::new());

        if !(self.map_name_to_index.contains_key(poolname)) {
            let mut pool = BarcodePool::new();
            pool.bc_length = sequence.len();

            let pool_index = self.pools.len();
            self.map_name_to_index.insert(poolname.to_string(), pool_index);
            self.pools.push(pool);
        }

        let pool_index = self.map_name_to_index.get(poolname).expect("bc index fail");
        let pool: &mut BarcodePool = self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }



    //From histogram, decide where to start. This can fail if no barcode fitted at all
    fn pick_startpos(
        &mut self
    ) -> anyhow::Result<()> {
        for p in &mut self.pools {
            p.pick_startpos()?;
        }
        Ok(())
    }


    fn scan_startpos(
        &mut self,
        seq: &[u8]
    ) {
        for p in &mut self.pools {
            p.scan_startpos(seq);
        }
    }


    pub fn find_probable_barcode_boundaries(
        &mut self,
        mut fastq_file: FastqReader<Box<dyn std::io::Read>>,
        n_reads: u32,
    ) -> anyhow::Result<()> {

        // Generate histogram of probable barcode start through iterating over the first n reads
        //let mut all_hits: Vec<(u32, usize, usize, i32)> = Vec::new();
        for _ in 0..n_reads {
            let record = fastq_file.next().unwrap();
            let record = record.expect("Error reading record for checking barcode position; input file too short");

            self.scan_startpos(&record.seq());
        }

        self.pick_startpos();

        Ok(())
    }


    pub fn detect_barcode(
        &mut self,
        seq: &[u8]
    ) -> Vec<String> {
        let mut full_bc: Vec<String> = Vec::with_capacity(self.num_pools());
        //full_bc.push("foo".to_string()); 
        for p in &mut self.pools {

            //full_bc.push("foo".to_string());

            let one_bc = p.detect_barcode(seq);
            if let Some((this_bc, _score)) = one_bc {
                full_bc.push(this_bc);
            }
        }
        
        full_bc
    }


}



#[derive(Clone, Debug)]
pub struct BarcodePool {

    pub barcode_list: Vec<Barcode>,
    pub seq2barcode: HashMap<String,usize>,
    pub bc_length: usize,

    pub quick_testpos: usize,
    pub all_test_pos:Vec<usize>,

    pub histogram_startpos:Vec<usize>
}
impl BarcodePool {


    fn new() -> BarcodePool {
        BarcodePool {
            barcode_list: vec![],
            seq2barcode: HashMap::new(),
            bc_length: 0,
            quick_testpos: 0,
            all_test_pos: vec![],

            histogram_startpos: vec![]
        }
    }

    pub fn add_bc(
        &mut self,
        bcname: &str,
        sequence: &str
    ){
        let bc = Barcode::new(bcname, sequence);
        self.seq2barcode.insert(sequence.to_string().clone(), self.barcode_list.len());
        self.barcode_list.push(bc);
    }


    // Find where this barcode might be located in a read.
    // Stores it internal histogram
    fn scan_startpos(
        &mut self,
        seq: &[u8]
    ) {

        //Find candidate hits
        let mut all_hits: Vec<(usize, i32)> = Vec::new();  //start, score
        for barcode in self.barcode_list.iter_mut() {
            let hits = barcode.seek_one(seq, 1); //// returns: name, sequence, start, score
            if let Some((_name, start, score)) = hits {
                all_hits.push((start, score));
            }
        }

        //Return first hit that is the best one
        let all_hits = all_hits.iter().min_set_by_key(|&(_, dist)| dist);
        if all_hits.len() > 0 {
            for (start, _score) in all_hits {
                self.histogram_startpos.push(*start);
            }
        }
    }


    
    //From histogram, decide where to start. This can fail if no barcode fitted at all
    fn pick_startpos(
        &mut self
    ) -> anyhow::Result<()> {

        if self.histogram_startpos.is_empty() {
            bail!("Barcode pool is not detected in reads");
        }
    
        //Find the most common value
        let mut histogram = self.histogram_startpos.iter().counts();
        let (&&most_common_pos,&most_common_count) = histogram.iter().max_by_key(|&(_, dist)| dist).expect("no entry in histogram");
        //let most_common_pos = **most_common_pos;

        //Keep any positions within a cutoff from the most common place
        let cutoff = (most_common_count as f64) * 0.8;
        histogram.retain(|_pos, cnt| (*cnt as f64) > cutoff);
        

        //Pick first and last expected positions
        let first_pos = **histogram.keys().min_by_key(|pos| ***pos).expect("there should be a min position");
        let last_pos = **histogram.keys().max_by_key(|pos| ***pos).expect("there should be a max position");
        
        //todo ensure last pos is not beyond last read length  -- later

        self.quick_testpos = most_common_pos;
        self.all_test_pos.extend(first_pos..last_pos);

        println!("Picked first pos {}",self.quick_testpos);
        println!("scanning from {} to {} ",first_pos, last_pos);
        println!("bc length {} ",self.bc_length);

        //Histogram no longer needed
        self.histogram_startpos.clear();
        Ok(())
    }


    pub fn detect_barcode(
        &mut self,
        seq: &[u8]
    ) -> Option<(String, i32)> { //barcode name, score


        //perform optimistic search first!
        let optimistic_seq = &seq[self.quick_testpos..(self.quick_testpos+self.bc_length)];
        let optimistic_seq = String::from_utf8(optimistic_seq.to_vec()).expect("weird bc");   // seems evil

        if let Some(&i) = self.seq2barcode.get(&optimistic_seq) {
            let bc = self.barcode_list.get(i).expect("wtf");
            return Some((bc.name.clone(),0));
        } else {
            debug!("not a precise match {:?}",optimistic_seq);
        }

        //--------------- todo; maybe scan the primary range first? can order vector for this to happen
        //Find candidate hits. Scan each barcode, in all positions 
        let mut all_hits: Vec<(String, i32)> = Vec::new();  //barcode name, start, score
        for barcode in self.barcode_list.iter_mut() {
            let hits = barcode.seek_one(seq, 1); //// returns: barcode name, sequence, start, score
            if let Some((name, _start, score)) = hits {
                if score==0 {
                    //If we find a perfect hit then return early, and only this one
                    return Some((name.clone(), score));
                } else {
                    //Keep hit for later comparison
                    all_hits.push((name.clone(), score));
                }
            }
        }

        //Return the first hit that is the best one
        let all_hits = all_hits.iter().min_set_by_key(|&(_name, score)| score);
        if let Some(&f)=all_hits.first() {
            Some(f.clone())
        } else {
            None
        }
    }

}














#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct Row {
    pos: String,
    well: String,
    seq: String,
}



pub fn read_barcodes(
    _barcode_file: &Option<PathBuf> ///////////////// todo add support
) -> CombinatorialBarcoding {

    let mut cb: CombinatorialBarcoding = CombinatorialBarcoding::new();

    let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
    let c = String::from_utf8(atrandi_bcs.to_vec()).unwrap();

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(c.as_bytes());
    for result in reader.deserialize() {
        let record: Row = result.unwrap();

        cb.add_bc(
            record.well.as_str(),
            record.pos.as_str(),
            record.seq.as_str()
        );
    }

    if cb.num_pools()==0 {
        println!("Warning: empty barcodes file");
    }
    cb
}

