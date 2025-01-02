use linya::Bar;
use log::info;
use log::debug;
use log::warn;
use log::trace;

use std::collections::HashMap;
use std::collections::HashSet;

use bio::alignment::Alignment;
use bio::pattern_matching::myers::Myers;

use super::io;
use std::path::PathBuf;

use seq_io::fasta::{Reader as FastaReader, Record as FastaRecord};
use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};

use itertools::Itertools;



#[derive(Clone, Debug)]
pub struct Barcode {
    //pub index: usize,
    pub name: String,
    pub pool: u32,
    pub sequence: Vec<u8>,
    pub pattern: Myers<u64>, //BitVector computed once and for all .. or move outside as it mutates??
}
impl Barcode {

    pub fn new(
        //index: usize,
        name: &str,
        pool: u32,
        sequence: &[u8],
    ) -> Barcode {
        Barcode {
            //index: index,
            name: name.to_string(),
            pool: pool,
            sequence: sequence.to_vec(),
            pattern: Myers::<u64>::new(sequence),
        }
    }



    // Get score (if any) of best match of barcode to sequence
    pub fn seek( ////////////// why mutable??
        &mut self,
//        &self,
        record: &[u8],
        max_distance: u8,
    ) -> Vec<(&String, u32, Vec<u8>, usize, usize, i32)> {
        // use Myers' algorithm to find the barcodes in a read
        // Ref: Myers, G. (1999). A fast bit-vector algorithm for approximate string
        // matching based on dynamic programming. Journal of the ACM (JACM) 46, 395–415.
        let mut hits: Vec<(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();
        let mut aln = Alignment::default();
        let mut matches = self.pattern.find_all_lazy(record, max_distance);  //^^^^^^^^^^^^ `self` is a `&` reference, so the data it refers to cannot be borrowed as mutable
        let maybe_matches = matches.by_ref().min_set_by_key(|&(_, dist)| dist);
        if maybe_matches.len() > 0 {
            for (best_end, _) in maybe_matches {
                matches.alignment_at(best_end, &mut aln);
                hits.push((
                    &self.name,
                    self.pool,
                    self.sequence.to_owned(),
                    aln.ystart,
                    aln.yend,
                    aln.score,
                ));
            }
        }
        hits
    }
}


/* 

this is never used?

pub fn read_barcodes(barcode_files: &Vec<PathBuf>) -> Vec<Barcode> {
    let mut barcodes: Vec<Barcode> = Vec::new();
    for barcode_file in barcode_files {
        let mut reader = io::open_fasta(barcode_file); // all barcodes should be in tsv files
                                                   // open barcode file
                                                   // tsv with the following columns (optional in parantheses):
                                                   // pos	(well)	seq
                                                   // let mut reader = File::open(barcode_file).unwrap();
                                                   // buffer and iterator
        let mut n_barcodes: usize = 0;
        while let Some(record) = reader.next() {
            let record = record.expect("Error reading record");
            let b = Barcode {
                index: n_barcodes,
                name: record.id().unwrap().to_string(),
                pool: 0,
                sequence: record.seq().to_vec(),
                pattern: Myers::<u64>::new(record.seq().to_vec()),
            };
            barcodes.push(b);
            n_barcodes += 1;
        }
    }
    // TODO check the edit distance between barcodes
    info!(
        "Found {} barcodes in specified barcode files",
        barcodes.iter().count()
    );
    barcodes
}

*/






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

    pub fn add_bc(
        &mut self,
        name: &str,
        poolname: &str,
        sequence: &[u8]
    )  {

        //Create new pool if needed
        //let &mut pool = player_stats.entry(poolname).or_insert(BarcodePool::new());

        if !(self.map_name_to_index.contains_key(poolname)) {
            let pool = BarcodePool::new();
            let pool_index = self.pools.len();
            self.map_name_to_index.insert(poolname.to_string(), pool_index);
            self.pools.push(pool);
        }

        let pool_index = self.map_name_to_index.get(poolname).expect("bc index fail");
        let pool: &mut BarcodePool = self.pools.get_mut(*pool_index).expect("get pool fail");

        let bc = Barcode::new(name, 666, sequence);
        pool.barcode_list.push(bc);

    }

}



#[derive(Clone, Debug)]
pub struct BarcodePool {

    pub barcode_list: Vec<Barcode>,
    pub seq2barcode: HashMap<String,u8>,
    pub bc_length: usize,
    pub quick_testpos: u8,
}
impl BarcodePool {


    fn new() -> BarcodePool {
        BarcodePool {
            barcode_list: vec![],
            seq2barcode: HashMap::new(),
            bc_length: 0,
            quick_testpos: 0
        }
    }

}







pub fn find_probable_barcode_boundaries(
    mut fastq_file: FastqReader<Box<dyn std::io::Read>>,
    barcodes: &mut Vec<Barcode>,
    pools: &HashSet<u32>,
    n_reads: u32,
) -> Vec<(u32, usize, usize)> {


    // Vec<(pool, start, stop)>
    let mut starts: Vec<(u32, usize, usize)> = Vec::new();
    // find most probable barcode start through iterating over the first n reads
    let mut all_hits: Vec<(u32, usize, usize, i32)> = Vec::new();
    for _ in 0..n_reads {
        let record = fastq_file.next().unwrap();
        let record = record.expect("Error reading record");
        for barcode in barcodes.iter_mut() {
            let mut hits = barcode.seek(&record.seq(), 1);
            // only keep pool, start, stop, score from hits
            let hits_filtered = hits.iter_mut().map(|x| (x.1, x.3, x.4, x.5));
            all_hits.extend(hits_filtered);
        }
    }

    let limit = (0.9 * n_reads as f32).floor() as usize;

    // now find the most likely possible starts and ends for each pool
    for pool in pools.iter() {
        let pool_hits_for_start = all_hits.iter().filter(|x| pool == &x.0);
        let pool_hits_for_end = all_hits.iter().filter(|x| pool == &x.0);
        // now the start and stop for that pool hit
        let possible_starts = pool_hits_for_start
            .map(|x| x.1)
            .counts()
            .into_iter()
            .filter(|&(_, v)| v > limit)
            .collect::<HashMap<_, _>>();
        let possible_ends = pool_hits_for_end
            .map(|x| x.2)
            .counts()
            .into_iter()
            .filter(|&(_, v)| v > limit)
            .collect::<HashMap<_, _>>();
        trace!(
            "Possible start positions for pool {:?}: {:?}",
            pool,
            possible_starts
        );
        trace!(
            "Possible end positions for pool {:?}: {:?}",
            pool,
            possible_ends
        );
        let smallest_start = match possible_starts.is_empty() {
            true => {
                warn!(
                    "No possible start positions found on the first {} reads",
                    n_reads
                );
                warn!("The barcode detection will be performed on the whole read");
                1 as usize
            }
            false => *possible_starts.keys().min().unwrap(),
        };
        let biggest_end = match possible_ends.is_empty() {
            true => {
                warn!(
                    "No possible start positions found on the first {} reads",
                    n_reads
                );
                warn!("The barcode detection will be performed on the whole read");
                1 as usize // TODO have read length here
            }
            false => *possible_ends.keys().max().unwrap(),
        };
        debug!(
            "Pool {:?} - Most probable start and end position for barcodes: {} - {}",
            pool, smallest_start, biggest_end
        );
        starts.push((*pool, smallest_start, biggest_end));
    }
    starts
}














#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct Row {
    pos: u32,
    well: String,
    seq: String,
}



pub fn validate_barcode_inputs(
    barcode_file: &Option<PathBuf>
) -> Vec<Barcode> {


    let mut barcodes: Vec<Barcode> = Vec::new();




    let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
    let c = String::from_utf8(atrandi_bcs.to_vec()).unwrap();

    //read_barcodes_file(&atrandi_bcs.as_ref(), &mut barcodes);

    let mut n_barcodes = 0; //TODO: why is this needed later?

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(c.as_bytes());
    for result in reader.deserialize() {
        let record: Row = result.unwrap();

        let b = Barcode::new(
                //n_barcodes,
                record.well.as_str(),
                record.pos,
                record.seq.as_bytes()
        );    

        barcodes.push(b);
        n_barcodes += 1;
    }

    if n_barcodes==0 {
        println!("Warning: empty barcodes file");
    }
    //TODO support reading of new files too

/* 

    // takes either presets or barcode files and returns a vector of Barcodes
    // TODO while presets are being implemented, barcode files support is currently disabled
    match preset {
        Some(preset) => {
            debug!("loading barcode preset: {:?}", preset);
            // TODO RESOLVE PRESET FILEPATH
            // not easy to include data in rust binary?
            // let's give the path for now
            // can include downloading in the future? of include the data in the binary?
            let opened = match File::open(&preset) {
                Ok(file) => file,
                Err(_) => {
                    error!("Could not open preset file {}", &preset.display());
                    process::exit(1)
                }
            };
            let mut n_barcodes = 0;

            
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_reader(opened);
            for result in reader.deserialize() {
                let record: Row = result.unwrap();
                let b = Barcode {
                    index: n_barcodes,
                    name: record.well,
                    pool: record.pos,
                    sequence: record.seq.as_bytes().to_vec(),
                    pattern: Myers::<u64>::new(record.seq.as_bytes().to_vec()),
                };
                barcodes.push(b);
                n_barcodes += 1;
            }
            
            if(n_barcodes==0){
                println!("Warning: empty barcodes file");
            }

            read_barcodes_file(&opened, &mut barcodes);

        }
        None => {
            // load the barcodes here
            println!("loading barcodes: {:?}", barcode_files);
        }
    }
*/


    barcodes
}



