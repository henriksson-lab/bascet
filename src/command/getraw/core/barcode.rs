use log::info;

use bio::alignment::Alignment;
use bio::pattern_matching::myers::Myers;

use super::io;
use std::path::PathBuf;

use seq_io::fasta::{Reader as FastaReader, Record as FastaRecord};

use itertools::Itertools;



#[derive(Clone, Debug)]
pub struct Barcode {
    pub index: usize,
    pub name: String,
    pub pool: u32,
    pub sequence: Vec<u8>,
    pub pattern: Myers<u64>,
}
impl Barcode {

    pub fn new(
        index: usize,
        name: &str,
        pool: u32,
        sequence: &[u8],
    ) -> Barcode {
        Barcode {
            index: index,
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