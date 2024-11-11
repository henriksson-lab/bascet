use bio::io::{fasta, fastq};
use std::fs::{File, FileType};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::process::Command;
use KMER_Select::simulate::{self, ISS};

fn main() {
    let path_ref = Path::new("data/all.fa");
    let mut path_out: &Path;

    path_out = Path::new("simulated/1M");
    ISS::simulate(&path_ref, &path_out, 10, 1000000);

    path_out = Path::new("simulated/100K");
    ISS::simulate(&path_ref, &path_out, 10, 100000);

    path_out = Path::new("simulated/10K");
    ISS::simulate(&path_ref, &path_out, 10, 10000);

    path_out = Path::new("simulated/1K");
    ISS::simulate(&path_ref, &path_out, 10, 1000);

    // let ref_reader = fasta::Reader::new(ref_bufreader);
    // for record_opt in ref_reader.records() {
    //     let record = record_opt.unwrap();
    //     let record_name = record.id().replace(".", "_");

    //     let ref_file = File::create(refs_in.join(&record_name).with_extension("fastq")).unwrap();
    //     let ref_bufwriter: BufWriter<File> = BufWriter::new(ref_file);
    //     let mut ref_bufwriter = fastq::Writer::new(ref_bufwriter);
    //     let _ = ref_bufwriter.write(
    //         &record_name,
    //         record.desc(),
    //         &record.seq(),
    //         &vec![33 + 40; record.seq().len()],
    //     );
    // }
    // let mut queries = vec![];
    // for entry in refs_in.read_dir().expect("read_dir call failed") {
    //     if let Ok(entry) = entry {
    //         if entry.file_name() == "all.fa" {
    //             continue;
    //         }
    //         if let Some(path) = entry.path().to_str() {
    //             queries.push(String::from(path));
    //         }
    //     }
    // }
    // for entry in dir_out.read_dir().expect("read_dir call failed") {
    //     if let Ok(entry) = entry {
    //         if let Some(path) = entry.path().to_str() {
    //             queries.push(String::from(path));
    //         }
    //     }
    // }
    // let sketch_out = Command::new("mash")
    //     .arg("sketch")
    //     .arg("-o")
    //     .arg(dir_out.to_str().unwrap())
    //     .args(&queries)
    //     .output()
    //     .expect("Failed to execute mash sketch command");

    // if sketch_out.status.success() {
    //     println!("Simulated reads sketched successfully.");
    // } else {
    //     eprintln!("Command failed to execute.");
    //     eprintln!("stderr: {}", String::from_utf8_lossy(&sketch_out.stderr));
    // }

    // let pairwise_file_out = dir_out.with_extension("dst");
    // let pairwise_file = File::create(&pairwise_file_out)
    //     .expect(&format!("Unable to write to file: {pairwise_file_out:?}."));
    // let mut pairwise_file_bufwriter = BufWriter::new(pairwise_file);
    // let _ = write!(
    //     pairwise_file_bufwriter,
    //     "Reference-ID\tQuery-ID\tMash-distance\tP-value\tMatching-hashes\n"
    // );

    // for query in queries {
    //     let output = Command::new("mash")
    //         .arg("dist")
    //         .arg(query)
    //         .arg(dir_out.with_extension("msh"))
    //         .output()
    //         .expect("Failed to execute mash dist command");

    //     if output.status.success() {
    //         let _ = write!(
    //             pairwise_file_bufwriter,
    //             "{}",
    //             String::from_utf8_lossy(&output.stdout)
    //         );
    //     } else {
    //         eprintln!("Command failed to execute.");
    //         eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    //     }
    // }
    // let _ = pairwise_file_bufwriter.flush();

    // let path_out = Path::new("simulated/reads_low_fidelity.fastq");
    // let sim = simulate::Reads::<150>::new(path_in, 0.0001, 0.00001);
    // let _ = sim.simulate(path_out);

    // let path_out = Path::new("simulated/reads_high_fidelity.fastq");
    // let sim = simulate::Reads::<150>::new(path_in, 0.01, 0.001);
    // let _ = sim.simulate(path_out);
}
