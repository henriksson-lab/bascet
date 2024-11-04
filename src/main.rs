use std::path::Path;
use std::process::Command;
use KMER_Select::simulate;

fn main() {
    let path_in = Path::new("data/all.fa");
    let path_out = Path::new("simulated/reads_variable_fidelity.fastq");
    let sim = simulate::Reads::<150>::new(path_in, 0.001, 0.0001);
    let _ = sim.simulate(path_out);

    let path_out = Path::new("simulated/reads_low_fidelity.fastq");
    let sim = simulate::Reads::<150>::new(path_in, 0.0001, 0.00001);
    let _ = sim.simulate(path_out);

    let path_out = Path::new("simulated/reads_high_fidelity.fastq");
    let sim = simulate::Reads::<150>::new(path_in, 0.01, 0.001);
    let _ = sim.simulate(path_out);
}
