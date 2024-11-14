use bio::io::{fasta, fastq};
use bio::stats::combinatorics::combinations;
use itertools::Itertools;
use linya::Progress;
use std::cmp::{max, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, FileType, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Command};
use walkdir::{DirEntry, WalkDir};
use KMER_Select::kmer::{self, Codec, EncodedKMER};
use KMER_Select::simulate::ISS;
use KMER_Select::utils;

fn main() {
    const KMER_SIZE: usize = 31;
    const KMC_KMER_SIZE_ARG: &str = concat!("-ks", stringify!(KMER_SIZE));
    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();

    let path_ref = Path::new("data/all.fa");
    let path_out: &Path;

    path_out = Path::new("simulated/1K");
    // ISS::simulate(&path_ref, &path_out, 10, 1000);
    let path_concatenated = ISS::collect_dir(&path_out).unwrap();

    let kmc_path = &path_out.join("1K");
    let _ = Command::new("kmc")
        .arg("-cs4294967295")
        .arg(KMC_KMER_SIZE_ARG)
        .arg(&path_concatenated)
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

    let n_smallest = 500;
    let n_largest = 500;
    let mut min_heap: BinaryHeap<Reverse<u128>> = BinaryHeap::with_capacity(n_smallest + 1);
    let mut max_heap: BinaryHeap<u128> = BinaryHeap::with_capacity(n_largest + 1);

    for line in reader.lines() {
        let line = line.unwrap();
        let mut iter = line.split_ascii_whitespace().map(|e| e.trim());

        let (str_kmer, str_count) = match (iter.next(), iter.next()) {
            (Some(kmer), Some(count)) => (kmer, count),
            (_, _) => panic!("Line must have at least two elements"),
        };
        let count = str_count.parse::<u32>().unwrap();
        let encoded = unsafe { CODEC.encode(str_kmer, count).into_bits() };

        min_heap.push(Reverse(encoded));
        max_heap.push(encoded);

        if min_heap.len() > n_smallest {
            min_heap.pop();
        }
        if max_heap.len() > n_largest {
            max_heap.pop();
        }
    }
    // Only keep the (2*k) kmer representations, counts are irrellevant here
    let mut ref_features: Vec<u128> = Vec::with_capacity(n_smallest + n_largest);
    ref_features.extend(
        min_heap
            .into_iter()
            .map(|rc| EncodedKMER::from_bits(rc.0).kmer()),
    );
    ref_features.extend(
        max_heap
            .into_iter()
            .map(|c| EncodedKMER::from_bits(c).kmer()),
    );

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

        let _ = utils::concat_files_two(&p, &q, &cat_path);

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

        let n_smallest = 500;
        let n_largest = 500;
        let mut min_heap: BinaryHeap<Reverse<u128>> = BinaryHeap::with_capacity(n_smallest + 1);
        let mut max_heap: BinaryHeap<u128> = BinaryHeap::with_capacity(n_largest + 1);

        for line in query_reader.lines() {
            let line = line.unwrap();
            let mut iter = line.split_ascii_whitespace().map(|e| e.trim());

            let (str_kmer, str_count) = match (iter.next(), iter.next()) {
                (Some(kmer), Some(count)) => (kmer, count),
                (_, _) => panic!("Line must have at least two elements"),
            };
            let count = str_count.parse::<u32>().unwrap();
            let encoded = unsafe { CODEC.encode(str_kmer, count).into_bits() };

            min_heap.push(Reverse(encoded));
            max_heap.push(encoded);

            if min_heap.len() > n_smallest {
                min_heap.pop();
            }
            if max_heap.len() > n_largest {
                max_heap.pop();
            }
        }
        let mut query_features: Vec<EncodedKMER> = Vec::with_capacity(n_smallest + n_largest);
        query_features.extend(
            min_heap
                .into_iter()
                .map(|rc| EncodedKMER::from_bits(rc.0)),
        );
        query_features.extend(
            max_heap
                .into_iter()
                .map(|c| EncodedKMER::from_bits(c)),
        );

        let mut line: Vec<String> = Vec::with_capacity(n_smallest + 1);
        line.push(format!("{}", &cat_path.to_str().unwrap()));

        for feature in &query_features {
            let kmer = feature.kmer();
            let count = feature.count();

            let feature_in_query = ref_features.binary_search(&kmer);

            if let Ok(index) = feature_in_query {
                line.push(format!("{}", count));
                println!(
                    "Found match! Ref k-mer: {}, Query count: {}",
                    kmer, count
                );
                continue;
            }
            line.push(format!("{}", 0));
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
