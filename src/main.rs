use std::path::Path;
use KMER_Select::simulate::ReadsSimulator;

fn main() {
    let in_path = Path::new("data/all.fa");
    let out_path = Path::new("simulated/reads.fasta");
    let sim = ReadsSimulator {
        p_read_open: 0.025,
        read_length: 150,
    };
    let _ = sim.simulate_with(in_path, out_path);
}
