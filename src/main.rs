use bio::io::{fasta, fastq};
use bio::stats::combinatorics::combinations;
use itertools::Itertools;
use linya::Progress;
use std::cmp::max;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, FileType, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::{DirEntry, WalkDir};
use KMER_Select::simulate::{self, ISS};

fn is_fastq(entry: &DirEntry) -> bool {
    if let Some(ext) = entry.path().extension() {
        return ext == "fastq";
    }
    return false;
}
const COUNT_SHIFT: i32 = 95;
const KMER_SHIFT: i32 = 33;
const KMER_BITS: i32 = COUNT_SHIFT - KMER_SHIFT;
const COUNT_BITS: i32 = 32;

fn extract_count(key: i128) -> u32 {
    let positive_key = -key;
    ((positive_key >> COUNT_SHIFT) & 0xFFFFFFFF) as u32
}

fn extract_kmer(key: i128) -> u64 {
    let positive_key = -key;
    ((positive_key >> KMER_SHIFT) & ((1i128 << 62) - 1)) as u64
}


fn debug_key(key: i128) {
    let unsigned_key = key as u128;
    println!("Original:     {:0128b}", key);
    println!("After shift:  {:064b}", unsigned_key >> KMER_SHIFT);
    println!("Final kmer:   {:064b}", extract_kmer(key));
    println!("Final count:  {:032b}", extract_count(key));
}
fn main() {
    let path_ref = Path::new("data/all.fa");
    let path_out: &Path;

    path_out = Path::new("simulated/1K");
    // ISS::simulate(&path_ref, &path_out, 10, 1000);

    let concat_path = path_out.join("1K").with_extension("fastq");
    let mut concat_file = File::create(&concat_path).unwrap();

    let walker = WalkDir::new(path_out).into_iter();
    let mut cats: Vec<String> = Vec::new();
    for entry in walker {
        if let Ok(entry) = entry {
            if is_fastq(&entry) {
                cats.push(String::from(entry.path().to_str().unwrap()));
            }
        }
    }
    for cat_file in &cats {
        let cat_out = Command::new("cat")
            .arg(cat_file)
            .output()
            .expect("Failed to execute cat");

        concat_file
            .write_all(&cat_out.stdout)
            .expect("Failed to write to output file");
    }

    let kmc_path = &path_out.join("1K");
    let _ = Command::new("kmc")
        .arg("-cs4294967295")
        .arg("-k31")
        .arg(&concat_path)
        .arg(&kmc_path)
        .arg("data/temp")
        .output()
        .expect("Failed to execute kmc command");

    let kmc_path_reduced = path_out.join("kmc_reduced");
    let _ = Command::new("kmc_tools")
        .arg("transform")
        .arg(&kmc_path)
        .arg("reduce")
        .arg(&kmc_path_reduced)
        .output()
        .expect("Failed to execute kmc command");

    let kmc_path_dump = path_out.join("kmc_dump").with_extension("txt");
    let _ = Command::new("kmc_tools")
        .arg("transform")
        .arg(&kmc_path_reduced)
        .arg("dump")
        .arg("-s")
        .arg(&kmc_path_dump)
        .output()
        .expect("Failed to execute kmc command");
    println!("Processing dump file");
    let ref_file = File::open(kmc_path_dump).unwrap();
    let reader = BufReader::new(ref_file);

    let n_smallest = 2000;
    let mut min_heap: BinaryHeap<i128> = BinaryHeap::new();

    for line in reader.lines().flatten() {
        let str: Vec<&str> = line.split_ascii_whitespace().collect();
        let kmer: u64 = str[0].trim().chars().fold(0, |acc, e| {
            (acc << 2)
                + match e {
                    'A' => 0,
                    'T' => 1,
                    'G' => 2,
                    'C' => 3,
                    _ => panic!("Unknown nucleotide"),
                }
        });
        let count: u32 = str[1].trim().parse().expect("Failed to parse count");
        let key = -(((count as i128) << 95) | ((kmer as i128) << 33));
        // println!("{count} => {}", extract_count(key));
        min_heap.push(key);
        if min_heap.len() > n_smallest {
            min_heap.pop();
        }
    }
    // Only keep the (2*k) kmer representation :)
    let ref_features: Vec<u64> = min_heap
        .into_iter()
        .map(|k| extract_kmer(k))
        .collect();

    let feature_file = File::create(&path_out.join("features").with_extension("csv")).unwrap();
    let mut feature_writer = BufWriter::new(feature_file);
    let _ = writeln!(feature_writer, "Query,{}", ref_features.iter().join(","));

    let walker = WalkDir::new(path_out).into_iter();
    let mut compare: Vec<(PathBuf, PathBuf)> = Vec::new();
    for entry in walker {
        if let Ok(entry) = entry {
            if entry.path() == path_out {
                continue;
            }
            if entry.metadata().unwrap().is_dir() {
                compare.push((
                    entry.path().join("Reads_R1").with_extension("fastq"),
                    entry.path().join("Reads_R2").with_extension("fastq"),
                ));
            }
        }
    }
    let mut progress = Progress::new();
    let bar = progress.bar(compare.len(), "Processing Pairwise Feature Matrices");
    let mut idx = 0;
    progress.set_and_draw(&bar, idx);
    for pair in compare {
        let p = pair.0;
        let q = pair.1;
        let out_path = p.parent().unwrap();
        let cat_path = out_path.join("concat").with_extension("fastq");
        let kmc_path = out_path.join("kmc");
        let _ = File::create(&cat_path);

        let p_file = File::open(&p).unwrap();
        let q_file = File::open(&q).unwrap();
        let mut p_reader = BufReader::new(p_file);
        let mut q_reader = BufReader::new(q_file);

        let cat_file = File::create(&cat_path).unwrap();
        let mut cat_writer = BufWriter::new(&cat_file);
        let p_size = p_reader.get_ref().metadata().unwrap().len();
        let q_size = q_reader.get_ref().metadata().unwrap().len();
        let buffer_size = max(p_size, q_size) as usize;
        let mut buffer = Vec::with_capacity(buffer_size);

        p_reader.read_to_end(&mut buffer).unwrap();
        cat_writer.write_all(&buffer).unwrap();
        buffer.clear();

        q_reader.read_to_end(&mut buffer).unwrap();
        cat_writer.write_all(&buffer).unwrap();
        buffer.clear();

        let _ = cat_writer.flush();

        // let _ = Command::new("kmc")
        //     .arg("-cs4294967295")
        //     .arg("-k31")
        //     .arg(&cat_path)
        //     .arg(&kmc_path)
        //     .arg("data/temp")
        //     .output()
        //     .expect("Failed to execute kmc command");

        let kmc_path_reduced = out_path.join("kmc_reduced");
        // let _ = Command::new("kmc_tools")
        //     .arg("transform")
        //     .arg(&kmc_path)
        //     .arg("reduce")
        //     .arg(&kmc_path_reduced)
        //     .output()
        //     .expect("Failed to execute kmc command");

        let kmc_path_dump = out_path.join("kmc_dump").with_extension("txt");
        // let _ = Command::new("kmc_tools")
        //     .arg("transform")
        //     .arg(&kmc_path_reduced)
        //     .arg("dump")
        //     .arg("-s")
        //     .arg(&kmc_path_dump)
        //     .output()
        //     .expect("Failed to execute kmc command");

        let query_file = File::open(kmc_path_dump).unwrap();
        let query_reader = BufReader::new(query_file);

        let mut min_heap: BinaryHeap<i128> = BinaryHeap::new();

        for line in query_reader.lines().flatten() {
            let str: Vec<&str> = line.split_ascii_whitespace().collect();
            let kmer: u64 = str[0].trim().chars().fold(0, |acc, e| {
                (acc << 2)
                    + match e {
                        'A' => 0,
                        'T' => 1,
                        'G' => 2,
                        'C' => 3,
                        _ => panic!("Unknown nucleotide"),
                    }
            });
            let count: u32 = str[1].trim().parse().expect("Failed to parse count");
            let key =  -(((count as i128) << 95) | ((kmer as i128) << 33));
            min_heap.push(key);
            if min_heap.len() > n_smallest {
                min_heap.pop();
            }
        }
        // When processing query k-mers:
        let (count_vec, kmer_vec): (Vec<u32>, Vec<u64>) = min_heap
        .iter()
            .map(|&key| {
                let count = extract_count(key);
                let kmer = extract_kmer(key);
                // Add debug print
                // println!("Original: {}, Extracted kmer: {}", key, kmer);
                (count, kmer as u64)
            })
            .unzip();
        
        let mut line: Vec<String> = Vec::with_capacity(n_smallest + 1);
        line.push(format!("{}", &cat_path.to_str().unwrap()));
        for feature in &ref_features {
            let feature_in_query = kmer_vec.iter().position(|&k| k == *feature);
            if let Some(index) = feature_in_query {
                line.push(format!("{}", count_vec[index]));
                println!("Found match! Ref k-mer: {}, Query count: {}", feature, count_vec[index]);
                continue;
            }
            line.push(format!("{}", 0));
            if kmer_vec.len() > 0 {
                // println!("No match for ref k-mer: {}, First query k-mer: {}", feature, kmer_vec[0]);
            }
        }

        let _ = writeln!(feature_writer, "{}", line.join(","));
        idx += 1;
        progress.set_and_draw(&bar, idx);
    }
    // path_out = Path::new("simulated/10K");
    // ISS::simulate(&path_ref, &path_out, 10, 10000);

    // path_out = Path::new("simulated/1K");
    // ISS::simulate(&path_ref, &path_out, 10, 1000);

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
