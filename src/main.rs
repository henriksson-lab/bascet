use bio::io::fasta::Reader;
use bio::io::fastq::Writer;
use bio::io::{fasta, fastq};
use KMER_Select::kmc::{self, ThreadState};
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
use KMER_Select::kmer::{self, Codec, EncodedKMER};
use KMER_Select::simulate::ISSRunner;
use KMER_Select::utils;

fn main() {
    const KMER_SIZE: usize = 31;
    const KMER_COUNT_CHARS: usize = 11;
    const THREADS: usize = 12;
    const WORKER_THREADS: usize = THREADS - 1;

    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();
    let range = Uniform::new_inclusive(u16::MIN, u16::MAX);

    let kmc_kmer_size_arg: &str = &format!("-k{}", KMER_SIZE);
    let mut rng = SmallRng::from_entropy();
    println!("{kmc_kmer_size_arg}");

    let path_out: &Path;

    path_out = Path::new("simulated/1K");
    // ISS::simulate(&path_ref, &path_out, 10, 1000);

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
    // lock file write access so that the behaviour of mmep is safe-ish
    let _ = ref_file.lock_shared();

    // HACK: 4'294'967'295 is the largest kmer counter possible, so its count of digits + 1 for safety + the KMER_SIZE
    let overlap_window = KMER_SIZE + KMER_COUNT_CHARS + 1;

    let n_smallest = 50_000;
    let n_largest = 1_000;


    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(THREADS)
        .build()
        .unwrap();

    // +1 because the floating point is truncated -> rounded down
    let n_smallest_thread_local = (n_smallest / WORKER_THREADS) + 1;
    let n_largest_thread_local = (n_largest / WORKER_THREADS) + 1;

    let thread_states: Vec<ThreadState> = (0..WORKER_THREADS)
        .map(|_| ThreadState {
            rng: UnsafeCell::new(SmallRng::from_entropy()),
            min_heap: UnsafeCell::new(BoundedMinHeap::with_capacity(n_smallest_thread_local)),
            max_heap: UnsafeCell::new(BoundedMaxHeap::with_capacity(n_largest_thread_local)),
        }).collect();
    
    let dump_start = std::time::Instant::now();
    
    let kmc_parser = kmc::Dump::<KMER_SIZE>::new();
    let extracted_features = kmc_parser.featurise(ref_file, &thread_states, &thread_pool).unwrap();
    let ref_features: Vec<u128> = extracted_features.iter().map(|c| EncodedKMER::from_bits(*c).kmer() ).collect();
    println!(
        "Dump file time: {:.2}s",
        dump_start.elapsed().as_secs_f64()
    );

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
    let start = std::time::Instant::now();
    let mut progress = Progress::new();
    let bar = progress.bar(compare.len(), "Building Pairwise Feature Matrix");
    let mut idx = 0;
    progress.set_and_draw(&bar, idx);

    let local_n_smallest = n_smallest * 10;
    let local_n_largest = n_largest * 10;
    let mut min_heap: BoundedMinHeap<u128> = BoundedMinHeap::with_capacity(n_smallest);
    let mut max_heap: BoundedMaxHeap<u128> = BoundedMaxHeap::with_capacity(n_largest);
    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            local_n_smallest + local_n_largest,
            BuildHasherDefault::default(),
        );
    let mut hash_took = 0.0;
    let mut find_took = 0.0;
    for pair in compare {
        let p = pair.0;
        let q = pair.1;

        let out_path = p.parent().unwrap();
        let cat_path = out_path.join("concat").with_extension("fastq");
        let kmc_path = out_path.join("kmc");
        let kmc_path_dump = out_path.join("kmc_dump").with_extension("txt");

        // let _ = utils::concat_files_two(&p, &q, &cat_path);
        // let _ = Command::new("kmc")
        //     .arg("-cs4294967295")
        //     .arg(kmc_kmer_size_arg)
        //     .arg(&cat_path)
        //     .arg(&kmc_path)
        //     .arg("data/temp")
        //     .output()
        //     .expect("Failed to execute kmc command");

        // let _ = Command::new("kmc_tools")
        //     .arg("transform")
        //     .arg(&kmc_path)
        //     .arg("dump")
        //     .arg(&kmc_path_dump)
        //     .output()
        //     .expect("Failed to execute kmc command");

        let query_file = File::open(kmc_path_dump).unwrap();
        let query_reader = BufReader::new(query_file);

        min_heap.clear();
        max_heap.clear();
        let find_start = std::time::Instant::now();
        for line in query_reader.lines() {
            let line = line.unwrap();
            let mut iter = line.split_ascii_whitespace().map(|e| e.trim());

            let (str_kmer, str_count) = match (iter.next(), iter.next()) {
                (Some(kmer), Some(count)) => (kmer, count),
                (_, _) => panic!("Line must have at least two elements"),
            };
            let count = str_count.parse::<u16>().unwrap();
            let encoded = unsafe {
                CODEC
                    .encode_str(str_kmer, count, &mut rng, range)
                    .into_bits()
            };

            let _ = min_heap.push(encoded);
            let _ = max_heap.push(encoded);
        }
        find_took += find_start.elapsed().as_secs_f64();
        // let test = EncodedKMER::from_bits(min_heap.peek().unwrap().0.clone());
        // println!("{} count {} kmer: {}", test.kmer(), test.count(), unsafe { CODEC.decode(test.into_bits()) });
        let hash_start = std::time::Instant::now();
        query_features.clear();
        for c in min_heap.iter() {
            let encoded = EncodedKMER::from_bits(*c);
            query_features.insert(encoded.kmer(), encoded.count() as u16);
        }
        for c in max_heap.iter() {
            let encoded = EncodedKMER::from_bits(*c);
            query_features.insert(encoded.kmer(), encoded.count() as u16);
        }
        hash_took += hash_start.elapsed().as_secs_f64();

        let mut line: Vec<String> = Vec::with_capacity(n_smallest + n_largest + 1);
        line.push(format!("{}", &cat_path.to_str().unwrap()));

        for feature in &ref_features {
            let feature_in_query = query_features.get(feature);
            if let Some(count) = feature_in_query {
                let kmer = unsafe { CODEC.decode(*feature) };
                line.push(format!("{}", count));
                // println!("Found match! Ref k-mer: {}, Query count: {}", kmer, count);
                continue;
            }
            line.push(format!("{}", 0));
        }

        let _ = writeln!(feature_writer, "{}", line.join(","));
        idx += 1;
        progress.set_and_draw(&bar, idx);
    }

    println!("Kmer finding took {find_took}s");
    println!("Hashing took {hash_took}s");

    let path_ref = Path::new("data/temp");
    let walker = WalkDir::new(path_ref).into_iter();

    let mut min_heap: BoundedMinHeap<u128> = BoundedMinHeap::with_capacity(n_smallest);
    let mut max_heap: BoundedMaxHeap<u128> = BoundedMaxHeap::with_capacity(n_largest);
    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            local_n_smallest + local_n_largest,
            BuildHasherDefault::default(),
        );
    for entry in walker {
        if let Ok(entry) = entry {
            let entry_path = entry.path();
            if entry_path.is_dir() || entry_path.extension() != Some("fasta".as_ref()) {
                continue;
            }

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
            let in_path = entry_path.with_extension("fastq");
            let out_path = entry_path.parent().unwrap();
            let kmc_path = out_path.join("kmc");
            let kmc_path_dump = out_path.join("kmc_dump");

            // let _ = Command::new("kmc")
            //     .arg("-cs4294967295")
            //     .arg(kmc_kmer_size_arg)
            //     .arg(&in_path)
            //     .arg(&kmc_path)
            //     .arg("data/temp")
            //     .output()
            //     .expect("Failed to execute kmc command");

            // let _ = Command::new("kmc_tools")
            //     .arg("transform")
            //     .arg(&kmc_path)
            //     .arg("dump")
            //     .arg(&kmc_path_dump)
            //     .output()
            //     .expect("Failed to execute kmc command");

            let query_file = File::open(kmc_path_dump).unwrap();
            let query_reader = BufReader::new(query_file);

            min_heap.clear();
            max_heap.clear();
            for line in query_reader.lines() {
                let line = line.unwrap();
                let mut iter = line.split_ascii_whitespace().map(|e| e.trim());

                let (str_kmer, str_count) = match (iter.next(), iter.next()) {
                    (Some(kmer), Some(count)) => (kmer, count),
                    (_, _) => panic!("Line must have at least two elements"),
                };
                let count = str_count.parse::<u16>().unwrap();
                let encoded = unsafe {
                    CODEC
                        .encode_str(str_kmer, count, &mut rng, range)
                        .into_bits()
                };

                let _ = min_heap.push(encoded);
                let _ = max_heap.push(encoded);
            }
            // let test = EncodedKMER::from_bits(min_heap.peek().unwrap().0.clone());
            // println!("{} count {} kmer: {}", test.kmer(), test.count(), unsafe { CODEC.decode(test.into_bits()) });
            query_features.clear();
            for c in min_heap.iter() {
                let encoded = EncodedKMER::from_bits(*c);
                query_features.insert(encoded.kmer(), encoded.count() as u16);
            }
            for c in max_heap.iter() {
                let encoded = EncodedKMER::from_bits(*c);
                query_features.insert(encoded.kmer(), encoded.count() as u16);
            }

            let mut line: Vec<String> = Vec::with_capacity(n_smallest + n_largest + 1);
            line.push(format!("{}", &entry_path.to_str().unwrap()));

            for feature in &ref_features {
                let feature_in_query = query_features.get(feature);
                if let Some(count) = feature_in_query {
                    let kmer = unsafe { CODEC.decode(*feature) };
                    line.push(format!("{}", count));
                    // println!("Found match! Ref k-mer: {}, Query count: {}", kmer, count);
                    continue;
                }
                line.push(format!("{}", 0));
            }

            let _ = writeln!(feature_writer, "{}", line.join(","));
        }
    }
    let single_duration = start.elapsed().as_secs_f64();
    println!("took {single_duration}");
}
