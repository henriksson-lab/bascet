use KMER_Select::simulate;
use std::path::Path;
use seq_io::fasta::{Reader, Record};

fn main() {
    let path = Path::new("data/all.fa");
    let mut reader = Reader::from_path(path).unwrap();

    let out_path = Path::new("simulated/reads.fasta");
    // reads = simulate::reads(&reader, out_path);
    println!("mean sequence length of {} records: {:.1} bp", n, sum as f32 / n as f32);
}