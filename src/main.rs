use bio::io::fasta::Reader;
use bio::io::fastq::Writer;
use bio::io::{fasta, fastq};
use fs2::FileExt;
use itertools::Itertools;
use linya::Progress;
use memmap2::MmapOptions;
use rand::distributions::Uniform;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rustc_hash::FxHasher;
use std::cmp::{max, min, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::fs::{self, File, FileType, OpenOptions};
use std::hash::BuildHasherDefault;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Command};
use std::u8;
use walkdir::{DirEntry, WalkDir};
use KMER_Select::bounded_heap::{prelude::*, BoundedMaxHeap, BoundedMinHeap};
use KMER_Select::kmer::{self, Codec, EncodedKMER};
use KMER_Select::simulate::ISSRunner;
use KMER_Select::utils;
use rayon::prelude::*;

fn main() {
    const KMER_SIZE: usize = 31;
    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();
    let mut rng = SmallRng::from_entropy();
    let range = Uniform::new_inclusive(u16::MIN, u16::MAX);

    let kmc_kmer_size_arg: &str = &format!("-k{}", KMER_SIZE);
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

    // HACK: 4'294'967'295 is the largest kmer counter possible, so its count of digits + 1
    let overlap_window = 11;

    let n_threads: usize = std::thread::available_parallelism()
        .unwrap_or(std::num::NonZero::new(1).unwrap())
        .get() as usize;
    let chunk_size: usize = 4096;
    let cursor_max: usize = ref_file.metadata().unwrap().len() as usize;
    let mut cursor: usize = 0;

    while cursor < cursor_max {
        let desired_page_size = chunk_size * n_threads;
        let desired_page_size_with_offset = desired_page_size + overlap_window;
        let remaining_file_size = cursor_max - cursor;
        let read_size = min(desired_page_size_with_offset, remaining_file_size);
        
        let mmap = unsafe {
            MmapOptions::new()
                .offset(cursor as u64)
                .len(read_size)
                .map(&ref_file)
        }
        .unwrap();

        let chunk_size = read_size / n_threads;
        mmap.par_chunks(chunk_size)
            .enumerate()
            .for_each(|(chunk_idx, chunk)| {
                println!("{chunk:?}")
            });

        cursor += desired_page_size_with_offset;
    }

    let _ = ref_file.unlock();
    // let reader = BufReader::new(&ref_file);

    // let n_smallest = 50000;
    // let n_largest = 1000;
    // let mut min_heap: BoundedMinHeap<u128> = BoundedMinHeap::with_capacity(n_smallest);
    // let mut max_heap: BoundedMaxHeap<u128> = BoundedMaxHeap::with_capacity(n_largest);

    // let total_start = std::time::Instant::now();
    // let mut parse_time = std::time::Duration::ZERO;
    // let mut encode_time = std::time::Duration::ZERO;
    // let mut heap_time = std::time::Duration::ZERO;

    // for line in reader.lines() {
    //     let line = line.unwrap();

    //     let parse_start = std::time::Instant::now();
    //     let mut iter = line.split_ascii_whitespace().map(|e| e.trim());
    //     let (str_kmer, str_count) = match (iter.next(), iter.next()) {
    //         (Some(kmer), Some(count)) => (kmer, count),
    //         (_, _) => panic!("Line must have at least two elements"),
    //     };
    //     let count = str_count.parse::<u16>().unwrap();
    //     parse_time += parse_start.elapsed();

    //     let encode_start = std::time::Instant::now();
    //     let encoded = unsafe { CODEC.encode(str_kmer, count, &mut rng, range).into_bits() };
    //     encode_time += encode_start.elapsed();

    //     let heap_start = std::time::Instant::now();
    //     let _ = min_heap.push(encoded);
    //     let _ = max_heap.push(encoded);
    //     heap_time += heap_start.elapsed();
    // }
    //

    let total_time = total_start.elapsed();
    println!("Total time: {:.2}s", total_time.as_secs_f64());
    println!("Parsing: {:.2}s", parse_time.as_secs_f64());
    println!("Encoding: {:.2}s", encode_time.as_secs_f64());
    println!("Heap ops: {:.2}s", heap_time.as_secs_f64());
    println!("Minheap features: {}", min_heap.len());
    // Only keep the (2*k) kmer representations, counts are irrellevant here
    let mut ref_features: Vec<u128> = Vec::with_capacity(n_smallest + n_largest + 1);
    ref_features.extend(
        min_heap
            .iter()
            .take(n_smallest)
            .map(|c| EncodedKMER::from_bits(*c).kmer()),
    );
    ref_features.extend(
        max_heap
            .iter()
            .take(n_largest)
            .map(|c| EncodedKMER::from_bits(*c).kmer()),
    );

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
            let encoded = unsafe { CODEC.encode(str_kmer, count, &mut rng, range).into_bits() };

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
            query_features.insert(encoded.kmer(), encoded.count());
        }
        for c in max_heap.iter() {
            let encoded = EncodedKMER::from_bits(*c);
            query_features.insert(encoded.kmer(), encoded.count());
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
                let encoded = unsafe { CODEC.encode(str_kmer, count, &mut rng, range).into_bits() };

                let _ = min_heap.push(encoded);
                let _ = max_heap.push(encoded);
            }
            // let test = EncodedKMER::from_bits(min_heap.peek().unwrap().0.clone());
            // println!("{} count {} kmer: {}", test.kmer(), test.count(), unsafe { CODEC.decode(test.into_bits()) });
            query_features.clear();
            for c in min_heap.iter() {
                let encoded = EncodedKMER::from_bits(*c);
                query_features.insert(encoded.kmer(), encoded.count());
            }
            for c in max_heap.iter() {
                let encoded = EncodedKMER::from_bits(*c);
                query_features.insert(encoded.kmer(), encoded.count());
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
