use anyhow::bail;
use log::debug;
use std::collections::HashMap;
use std::io::Read;

use bio::alignment::Alignment;
use bio::pattern_matching::myers::Myers;

use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};

use itertools::Itertools;

use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;

///////////////////////////////
/// Detector of barcode given Myers algorithm
#[derive(Clone, Debug)]
pub struct MyersBarcode {
    pub name: String,
    pub sequence: String,
    pub pattern: Myers<u64>, //this structure needs mutation during search
}
impl MyersBarcode {
    pub fn new(name: &str, sequence: &str) -> MyersBarcode {
        MyersBarcode {
            name: name.to_string(),
            sequence: sequence.to_string(),
            pattern: Myers::<u64>::new(sequence.as_bytes()),
        }
    }

    ///////////////////////////////
    /// Seek first barcode hit
    /// Note: Mutatable because it modifies the Myers precalculated matrix
    /// returns: name, start, score
    pub fn seek_one(&mut self, record: &[u8], max_distance: u8) -> Option<(&String, usize, i32)> {
        // use Myers' algorithm to find the barcodes in a read
        // Ref: Myers, G. (1999). A fast bit-vector algorithm for approximate string
        // matching based on dynamic programming. Journal of the ACM (JACM) 46, 395–415.
        let mut aln = Alignment::default();
        let mut matches = self.pattern.find_all_lazy(record, max_distance);

        // Return the best hit, if any
        let min_key = matches.by_ref().min_by_key(|&(_, dist)| dist);

        if let Some((best_end, _)) = min_key {
            // Calculate the alignment
            matches.alignment_at(best_end, &mut aln);

            Some((&self.name, aln.ystart, aln.score))
        } else {
            None
        }
    }
}

///////////////////////////////
/// A set of barcode positions and sequences, making up a total combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcode {
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    //Maps name of pool to index in array (used using building only)
    map_name_to_index: HashMap<String, usize>,
=======

    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String,usize>,
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs

    //Each barcode set in the combination
    pools: Vec<CombinatorialBarcodePart>,

    //How much to trim from this read
    pub trim_bcread_len: usize,
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
=======

    //Location of the UMI
    pub umi_from: usize,
    pub umi_to: usize,

>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
}
impl CombinatorialBarcode {
    pub fn new() -> CombinatorialBarcode {
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
        CombinatorialBarcode {
            map_name_to_index: HashMap::new(),
            pools: vec![],
            trim_bcread_len: 0,
=======

        CombinatorialBarcode {
            map_poolname_to_index: HashMap::new(),
            pools: vec![],
            trim_bcread_len: 0,
            umi_from: 0,
            umi_to: 0
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        }
    }

    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    pub fn add_bc(&mut self, name: &str, poolname: &str, sequence: &str) {
        //Create new pool if needed
        if !(self.map_name_to_index.contains_key(poolname)) {
            let mut pool = CombinatorialBarcodePart::new();
            pool.bc_length = sequence.len();

            let pool_index = self.pools.len();
            self.map_name_to_index
                .insert(poolname.to_string(), pool_index);
            self.pools.push(pool);
        }

        let pool_index = self.map_name_to_index.get(poolname).expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart =
            self.pools.get_mut(*pool_index).expect("get pool fail");
=======

    pub fn add_pool(
        &mut self,
        poolname: &str,
        pool: CombinatorialBarcodePart
    ) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index.insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    
    pub fn add_bc(
        &mut self,
        name: &str,
        poolname: &str,
        sequence: &str
    )  {

        //Create new pool if needed
        if !(self.map_poolname_to_index.contains_key(poolname)) {
            self.add_pool(poolname, CombinatorialBarcodePart::new());
        }

        let pool_index = self.map_poolname_to_index.get(poolname).expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart = self.pools.get_mut(*pool_index).expect("get pool fail");
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        pool.add_bc(name, sequence);
    }

    ///////////////////////////////
    /// From histogram, decide where to start. This can fail if no barcode fitted at all
    fn pick_startpos(&mut self) -> anyhow::Result<()> {
        for p in &mut self.pools {
            p.pick_startpos()?;
        }
        Ok(())
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    fn scan_startpos(&mut self, seq: &[u8]) {
=======
    ///////////////////////////////
    /// For each round of barcode, check location along all read
    pub fn scan_oneread_barcode_boundaries(
        &mut self,
        seq: &[u8]
    ) {
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        for p in &mut self.pools {
            p.scan_oneread_barcode_boundaries(seq);
        }
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    pub fn find_probable_barcode_boundaries(
        /////////////////////////////////////////// TODO: sort barcodes such that innermost BC is searched first. thus we can give up early possibly
=======




    pub fn scan_reads_barcode_boundaries( 
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        &mut self,
        fastq_file: &mut FastqReader<Box<impl std::io::Read + ?Sized>>,
        n_reads: u32,
    ) -> anyhow::Result<()> {
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
        // Generate histogram of probable barcode start through iterating over the first n reads
        //let mut all_hits: Vec<(u32, usize, usize, i32)> = Vec::new();
        for _ in 0..n_reads {
            let record = fastq_file.next().unwrap();
            let record = record
                .expect("Error reading record for checking barcode position; input file too short");
            self.scan_startpos(&record.seq());
=======

        //Generate histogram of probable barcode start through iterating over the first n reads
        for _ in 0..n_reads {
            let record = fastq_file.next().unwrap();
            let record = record.expect("Error reading record for checking barcode position; input file too short");
            self.scan_oneread_barcode_boundaries(&record.seq());
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        }

        Ok(())
    }


    ///////////////////////////////
    /// How many possible barcode matches where there during detection?
    pub fn count_detect_barcode_matches(
        &mut self
    )  -> usize {

        // Sum matches from each barcode round
        let mut cnt: usize = 0;
        for p in &mut self.pools {
            cnt += p.count_detect_barcode_matches();
        }
        cnt
    }


    pub fn decide_barcode_boundaries( 
        &mut self
    ) -> anyhow::Result<()> {

        //Pick the locations of all barcodes
        _ = self.pick_startpos();

        //Figure out how much we need to trim. Set it based on the last BC position.
        //This assumes no adapters after the last BC (could provide!)
        let mut trim_bcread_len: usize = 0;
        for p in &mut self.pools {
            let possible_end = p.quick_testpos + p.bc_length;
            if possible_end > trim_bcread_len {
                trim_bcread_len = possible_end;
            }
        }
        self.trim_bcread_len = trim_bcread_len;
        println!(
            "Detected amount to trim from barcode read: {}",
            trim_bcread_len
        );

        Ok(())
    }

    ///////////////////////////////
    /// Given a read, try figure out where the barcodes are located
    /// TODO: sort barcodes such that innermost BC is searched first. thus we can give up early possibly
    pub fn find_probable_barcode_boundaries( 
        &mut self,
        fastq_file: &mut FastqReader<Box<impl std::io::Read + ?Sized>>,
        n_reads: u32,
    ) -> anyhow::Result<()> {

        self.scan_reads_barcode_boundaries(fastq_file, n_reads)?;
        self.decide_barcode_boundaries()?;

        Ok(())
    }

    ///////////////////////////////
    /// Detect barcode only
    /// 
    /// TODO extract UMI
    /// 
    #[inline(always)]
    pub fn detect_barcode(
        &mut self,
        seq: &[u8],
        abort_early: bool,
        total_distance_cutoff: i32,
        part_distance_cutoff: i32,
    ) -> (bool, CellID) {
        let mut full_bc: Vec<String> = Vec::with_capacity(self.num_pools());
        let mut total_score = 0;

        //println!("------");

        //Loop across each barcode round
        for p in &mut self.pools {
            let one_bc = p.detect_barcode(seq, part_distance_cutoff as u8);
            if let Some((this_bc, score)) = one_bc {
                full_bc.push(this_bc);
                total_score = total_score + score;
            } else if abort_early {
                //println!("------ abort, part_distance_cutoff");

                //If we cannot decode a barcode, abort early. This saves a good % of time
                return (false, bcvec_to_string(&full_bc));
            }

            //println!("{} {}", p.quick_testpos, total_score);

            // early return if mismatch too high. This saves a good % of time
            if total_score > total_distance_cutoff {
                //println!("------ abort, total_distance_cutoff");
                return (false, bcvec_to_string(&full_bc));
            }
        }
        if !abort_early && full_bc.len() != self.pools.len() {
            //Barcode was incomplete. This can only happen if early abortion not set.
            //Adding it as a condition to help compiler remove this test when the function is inlined
            //println!("------ abort, incomplete");
            return (false, bcvec_to_string(&full_bc));
        }

        (true, bcvec_to_string(&full_bc))
    }

    ///////////////////////////////
    /// Detect barcode, and trim if ok
    #[inline(always)]
    pub fn detect_barcode_and_trim(
        &mut self,
        bc_seq: &[u8],
        bc_qual: &[u8],
        other_seq: &[u8],
        other_qual: &[u8],
        total_bc_distance_cutoff: i32,
        local_bc_distance_cutoff: i32,
    ) -> (bool, CellID, ReadPair) {
        let mut full_bc: Vec<String> = Vec::with_capacity(self.num_pools());
        let mut total_score = 0;
        for p in &mut self.pools {
            let one_bc = p.detect_barcode(bc_seq, local_bc_distance_cutoff as u8);
            if let Some((this_bc, score)) = one_bc {
                full_bc.push(this_bc);
                total_score = total_score + score;
            } else {
                //If we cannot decode a barcode, abort early. This saves a good % of time
                //No trimming performed
                return (
                    false,
                    bcvec_to_string(&full_bc),
                    ReadPair {
                        r1: bc_seq.to_vec(),
                        r2: other_seq.to_vec(),
                        q1: bc_qual.to_vec(),
                        q2: other_qual.to_vec(),
                        umi: vec![].to_vec(),
                    },
                );
            }

            if total_score > total_bc_distance_cutoff {
                // early return if mismatch too high. This saves a good % of time
                return (
                    false,
                    bcvec_to_string(&full_bc),
                    ReadPair {
                        r1: bc_seq.to_vec(),
                        r2: other_seq.to_vec(),
                        q1: bc_qual.to_vec(),
                        q2: other_qual.to_vec(),
                        umi: vec![].to_vec(),
                    },
                );
            }
        }

        //We got a full barcode. Trim barcode read next
        //TODO: need to also trim other read, if it overlaps the BC read and go into the adapters.
        //could simply scan for fragment after BCs in other read? could use the fancy data structure over last BC if we wanted
        return (
            true,
            bcvec_to_string(&full_bc),
            ReadPair {
                r1: bc_seq[self.trim_bcread_len..].to_vec(),
                r2: other_seq.to_vec(),
                q1: bc_qual[self.trim_bcread_len..].to_vec(),
                q2: other_qual.to_vec(),
                umi: vec![].to_vec(),
            },
        );
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
=======


    
    ///////////////////////////////
    /// Read list of barcodes from a TSV file
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode {
        let mut cb: CombinatorialBarcode = CombinatorialBarcode::new();

        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(src);
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                record.well.as_str(),
                record.pos.as_str(),
                record.seq.as_str(),
            );
        }

        if cb.num_pools() == 0 {
            println!("Warning: empty barcodes file");
        }
        cb
    }
}

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
=======

///////////////////////////////
/// Convert list of barcode names to cellID
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
fn bcvec_to_string(cell_id: &Vec<String>) -> CellID {
    //Note: : and - are not allowed in cell IDs. this because of the possible use of tabix
    //should support some type of uuencodeing
    cell_id.join("_")
}

///////////////////////////////
/// One barcode position, in a combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcodePart {
    pub barcode_list: Vec<MyersBarcode>,
    pub seq2barcode: HashMap<String, usize>,
    pub bc_length: usize,

    pub quick_testpos: usize,
    pub all_test_pos: Vec<usize>,

    pub histogram_startpos: Vec<usize>,
}
impl CombinatorialBarcodePart {
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    fn new() -> CombinatorialBarcodePart {
=======

    pub fn new() -> CombinatorialBarcodePart {
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        CombinatorialBarcodePart {
            barcode_list: vec![],
            seq2barcode: HashMap::new(),
            bc_length: 0,
            quick_testpos: 0,
            all_test_pos: vec![],

            histogram_startpos: vec![],
        }
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    pub fn add_bc(&mut self, bcname: &str, sequence: &str) {
=======
    ///////////////////////////////
    /// Add a barcode to this round
    pub fn add_bc(
        &mut self,
        bcname: &str,
        sequence: &str
    ){
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        let bc = MyersBarcode::new(bcname, sequence);
        self.seq2barcode
            .insert(sequence.to_string().clone(), self.barcode_list.len());
        self.barcode_list.push(bc);
        self.bc_length = sequence.len();
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    // Find where this barcode might be located in a read.
    // Stores it internal histogram
    fn scan_startpos(&mut self, seq: &[u8]) {
=======
    ///////////////////////////////
    /// Find where this barcode might be located in a read.
    /// Stores location in the internal histogram
    fn scan_oneread_barcode_boundaries(
        &mut self,
        seq: &[u8]
    ) {

>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        //Find candidate hits
        let mut all_hits: Vec<(usize, i32)> = Vec::new(); //start, score
        for barcode in self.barcode_list.iter_mut() {
            let hits = barcode.seek_one(seq, 1); //// returns: name, sequence, start, score
            if let Some((_name, start, score)) = hits {
                all_hits.push((start, score));
            }
        }

        //Take the first hit that is the best one, and add to startpos histogram
        let all_hits = all_hits.iter().min_set_by_key(|&(_, dist)| dist);
        if all_hits.len() > 0 {
            for (start, _score) in all_hits {
                self.histogram_startpos.push(*start);
                //println!("Found match {}", self.histogram_startpos.len());
                
            }
        }
    }

<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
    //From histogram, decide where to start. This can fail if no barcode fitted at all
    fn pick_startpos(&mut self) -> anyhow::Result<()> {
=======

    

    ///////////////////////////////
    /// Get how many matches were found
    pub fn count_detect_barcode_matches(
        &mut self
    )  -> usize {
        println!("mathces bottom {}", self.histogram_startpos.len());
        self.histogram_startpos.len()
    }

    ///////////////////////////////
    /// From histogram, decide where to start. This can fail if no barcode fitted at all.
    /// 
    /// Note that the histogram is cleared after this function call
    /// 
    fn pick_startpos(
        &mut self
    ) -> anyhow::Result<()> {

>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs
        if self.histogram_startpos.is_empty() {
            bail!("Barcode pool is not detected in reads");
        }

        //Find the most common value
        let mut histogram = self.histogram_startpos.iter().counts();
        let (&&most_common_pos, &most_common_count) = histogram
            .iter()
            .max_by_key(|&(_, dist)| dist)
            .expect("no entry in histogram");

        //Keep any positions within a cutoff from the most common place
        let cutoff = (most_common_count as f64) * 0.8;
        histogram.retain(|_pos, cnt| (*cnt as f64) > cutoff);

        //Pick first and last expected positions
        let first_pos = **histogram
            .keys()
            .min_by_key(|pos| ***pos)
            .expect("there should be a min position");
        let last_pos = **histogram
            .keys()
            .max_by_key(|pos| ***pos)
            .expect("there should be a max position");

        //todo ensure last pos is not beyond last read length  -- later

        self.quick_testpos = most_common_pos;
        self.all_test_pos.extend(first_pos..last_pos);

        println!("scanning from starting positions {} to {}, first testing position {}. The barcode is of length {}",first_pos, last_pos, self.quick_testpos, self.bc_length);

        //Histogram no longer needed
        self.histogram_startpos.clear();
        Ok(())
    }

    pub fn detect_barcode(
        &mut self,
        seq: &[u8],
        max_distance: u8, // was 1
    ) -> Option<(String, i32)> {
        //barcode name, score

        //perform optimistic search first!
<<<<<<< HEAD:src/barcode/combinatorial_barcode.rs
        let optimistic_seq = &seq[self.quick_testpos..(self.quick_testpos + self.bc_length)];
        let optimistic_seq = String::from_utf8(optimistic_seq.to_vec()).expect("weird bc"); // seems evil
=======
        let optimistic_seq = &seq[self.quick_testpos..(self.quick_testpos+self.bc_length)];
        let optimistic_seq = unsafe {
            //This has to be fast. Pray that we don't get weird strings as input
            String::from_utf8_unchecked(optimistic_seq.to_vec())


            //We can work with plain u8 if we get rid of meyers algorithm

        };
>>>>>>> main:src/barcode/combinatorial_barcode_anysize.rs

        if let Some(&i) = self.seq2barcode.get(&optimistic_seq) {
            let bc = self.barcode_list.get(i).expect("wtf");
            return Some((bc.name.clone(), 0));
        } else {
            debug!("not a precise match {:?}", optimistic_seq);
        }


        //Simply scan 


        //return None;
        ////////////////////////////////// Performance test. below stinks


        //--------------- todo; maybe scan the primary range first? can order vector for this to happen
        //Find candidate hits. Scan each barcode, in all positions
        let mut all_hits: Vec<(String, i32)> = Vec::new(); //barcode name, start, score
        for barcode in self.barcode_list.iter_mut() {
            let hits = barcode.seek_one(seq, max_distance); //// returns: barcode name, sequence, start, score
            if let Some((name, _start, score)) = hits {
                if score == 0 {
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
        if let Some(&f) = all_hits.first() {
            Some(f.clone())
        } else {
            None
        }
    }
}

///////////////////////////////
/// For serialization: one row in a barcode CSV definition file
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct BarcodeCsvFileRow {
    pos: String,
    well: String,
    seq: String,
}
