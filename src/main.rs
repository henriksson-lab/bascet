use bio::io::fasta::Reader;
use bio::io::fastq::Writer;
use bio::io::{fasta, fastq};
use core::str;
use fs2::FileExt;
use itertools::Itertools;
use linya::Progress;
use memmap2::MmapOptions;
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rayon::{prelude::*, ThreadPoolBuilder};
use region::page;
use rustc_hash::FxHasher;
use std::cell::{RefCell, UnsafeCell};
use std::cmp::{max, min, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::env;
use std::fs::{self, File, FileType, OpenOptions};
use std::hash::BuildHasherDefault;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::num::NonZero;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::{exit, Command};
use std::sync::{Arc, Mutex};
use std::{ptr, thread, u8};
use walkdir::{DirEntry, WalkDir};
use KMER_Select::bounded_heap::{prelude::*, BoundedMaxHeap, BoundedMinHeap};
use KMER_Select::kmc::{self, Dump, ThreadState};
use KMER_Select::kmer::{self, Codec, EncodedKMER};
use KMER_Select::simulate::ISSRunner;
use KMER_Select::utils;

fn main() {
    const KMER_SIZE: usize = 31;
    const THREADS: usize = 12;
    const WORKER_THREADS: usize = THREADS - 1;
    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();

    let kmc_kmer_size_arg: &str = &format!("-k{}", KMER_SIZE);
    println!("{kmc_kmer_size_arg}");

    let path_out = Path::new("simulated/1K");

    let kmc_path = &path_out.join("kmc");
    let kmc_path_dump = path_out.join("kmc_dump").with_extension("txt");

    let path_concatenated = ISSRunner::collect_dir(&path_out).unwrap();
    let kmc = Command::new("kmc")
        .arg("-cs4294967295")
        .arg("-ci2")
        .arg(kmc_kmer_size_arg)
        .arg(&path_concatenated)
        .arg(&kmc_path)
        .arg("data/temp")
        .output()
        .expect("Failed to execute kmc command");

    let _ = Command::new("kmc_tools")
        .arg("transform")
        .arg(&kmc_path)
        .arg("dump")
        .arg(&kmc_path_dump)
        .output()
        .expect("Failed to execute kmc command");

    println!("Processing dump file");

    let ref_file = File::open(kmc_path_dump).unwrap();
    let _ = ref_file.lock_shared();

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(THREADS)
        .build()
        .unwrap();

    let dump_start = std::time::Instant::now();
    let config = kmc::Config {
        seed: 0,
        threads: THREADS,
        chunk_size: 524288,
        nlo_results: 50_000,
        nhi_results: 2_000,
    };

    let thread_states: Vec<ThreadState<SmallRng>> = (0..WORKER_THREADS)
        .map(|_| {
            ThreadState::<SmallRng>::from_entropy(
                config.nlo_results,
                config.nhi_results,
                config.chunk_size,
            )
        })
        .collect();

    let kmc_parser: Dump<KMER_SIZE> = Dump::new(config.clone());
    let (min_heap, max_heap) = kmc_parser
        .featurise::<SmallRng>(ref_file, &thread_states, &thread_pool)
        .unwrap();

    let ref_features: Vec<u128> = min_heap
        .iter()
        .chain(max_heap.iter())
        .map(|c| EncodedKMER::from_bits(*c).kmer())
        .collect();

    println!("Dump file time: {:.8}s", dump_start.elapsed().as_secs_f64());
    println!("Features found: {}", ref_features.len());

    let feature_file = File::create(&path_out.join("features").with_extension("csv")).unwrap();
    let mut feature_writer = BufWriter::new(feature_file);
    let _ = writeln!(
        feature_writer,
        "Query,{}",
        ref_features
            .iter()
            .map(|kc| unsafe { CODEC.decode(*kc) })
            .join(",")
    );

    // Process comparison files
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
    let config = kmc::Config {
        seed: 0,
        threads: THREADS,
        chunk_size: 524288,
        nlo_results: 500_000,
        nhi_results:  20_000,
    };
    let mut thread_states: Vec<ThreadState<SmallRng>> = (0..WORKER_THREADS)
        .map(|_| {
            ThreadState::<SmallRng>::from_entropy(
                config.nlo_results,
                config.nhi_results,
                config.chunk_size,
            )
        })
        .collect();
    let kmc_parser: Dump<KMER_SIZE> = Dump::new(config.clone());

    let start = std::time::Instant::now();
    let mut progress = Progress::new();
    let bar = progress.bar(compare.len(), "Building Pairwise Feature Matrix");
    let mut idx = 0;
    progress.set_and_draw(&bar, idx);

    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            config.nlo_results + config.nhi_results,
            BuildHasherDefault::default(),
        );

    for pair in compare {
        let p = pair.0;
        let out_path = p.parent().unwrap();
        let cat_path = out_path.join("concat").with_extension("fastq");
        let kmc_path = out_path.join("kmc");
        let kmc_path_dump = out_path.join("kmc_dump").with_extension("txt");

        let query_file = File::open(&kmc_path_dump).unwrap();
        let _ = query_file.lock_shared();

        let find_start = std::time::Instant::now();

        // Reset thread states before processing each file
        thread_states.iter_mut().for_each(|state| state.reset());
        query_features.clear();
        let (query_min_heap, query_max_heap) = kmc_parser
            .featurise::<SmallRng>(query_file, &thread_states, &thread_pool)
            .unwrap();

        // Populate query features HashMap
        for heap_item in query_min_heap.iter().chain(query_max_heap.iter()) {
            let encoded = EncodedKMER::from_bits(*heap_item);
            query_features.insert(encoded.kmer(), encoded.count() as u16);
        }

        // Write results
        let mut line = vec![cat_path.to_str().unwrap().to_string()];
        for feature in &ref_features {
            line.push(
                query_features
                    .get(feature)
                    .map_or(String::from("0"), |count| count.to_string()),
            );
        }

        let _ = writeln!(feature_writer, "{}", line.join(","));
        idx += 1;
        progress.set_and_draw(&bar, idx);
    }

    // Process reference files
    let path_ref = Path::new("data/temp");
    let walker = WalkDir::new(path_ref).into_iter();

    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            config.nlo_results + config.nhi_results,
            BuildHasherDefault::default(),
        );

    for entry in walker {
        if let Ok(entry) = entry {
            let entry_path = entry.path();
            if entry_path.is_dir() || entry_path.extension() != Some("fasta".as_ref()) {
                continue;
            }

            // Convert FASTA to FASTQ
            let read_handle = File::open(entry_path).unwrap();
            let write_handle = File::create(entry_path.with_extension("fastq")).unwrap();
            let bufreader = BufReader::new(&read_handle);
            let bufwriter = BufWriter::new(&write_handle);
            let fasta_reader = Reader::from_bufread(bufreader);
            let mut fastq_writer = Writer::from_bufwriter(bufwriter);

            for record in fasta_reader.records() {
                if let Ok(record) = record {
                    let _ = fastq_writer.write(
                        record.id(),
                        record.desc(),
                        record.seq(),
                        &vec![54; record.seq().len()],
                    );
                }
            }

            let out_path = entry_path.parent().unwrap();
            let kmc_path_dump = out_path.join("kmc_dump");

            let query_file = File::open(&kmc_path_dump).unwrap();
            let _ = query_file.lock_shared();

            // Reset thread states before processing each file
            thread_states.iter_mut().for_each(|state| state.reset());
            query_features.clear();

            let (query_min_heap, query_max_heap) = kmc_parser
                .featurise::<SmallRng>(query_file, &thread_states, &thread_pool)
                .unwrap();

            // Populate query features HashMap
            for heap_item in query_min_heap.iter().chain(query_max_heap.iter()) {
                let encoded = EncodedKMER::from_bits(*heap_item);
                query_features.insert(encoded.kmer(), encoded.count() as u16);
            }

            // Write results
            let mut line = vec![entry_path.to_str().unwrap().to_string()];
            for feature in &ref_features {
                line.push(
                    query_features
                        .get(feature)
                        .map_or(String::from("0"), |count| count.to_string()),
                );
            }

            let _ = writeln!(feature_writer, "{}", line.join(","));
        }
    }

    let single_duration = start.elapsed().as_secs_f64();
    println!("took {single_duration}");
}
