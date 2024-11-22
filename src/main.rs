use bio::io::fasta::Reader;
use bio::io::fastq::Writer;
use core::str;
use fs2::FileExt;
use itertools::Itertools;
use linya::Progress;
use rand::rngs::SmallRng;
use rustc_hash::FxHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;
use KMER_Select::kmc::{self, Dump, ThreadState, Worker};
use KMER_Select::kmer::{Codec, EncodedKMER};
use KMER_Select::simulate::ISSRunner;

const KMER_SIZE: usize = 31;
const THREADS: usize = 12;
const WORKER_THREADS: usize = THREADS - 1;
const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();

fn process_kmc_file(path_out: &Path) -> (PathBuf, PathBuf) {
    let kmc_path = path_out.join("kmc");
    let kmc_path_dump = path_out.join("kmc_dump").with_extension("txt");
    let path_concatenated = ISSRunner::collect_dir(&path_out).unwrap();

    let kmc_kmer_size_arg = format!("-k{}", KMER_SIZE);
    let _ = Command::new("kmc")
        .arg("-cs4294967295")
        .arg(&kmc_kmer_size_arg)
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

    (kmc_path, kmc_path_dump)
}

fn create_feature_writer(path_out: &Path, ref_features: &[u128]) -> BufWriter<File> {
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
    feature_writer
}

fn extract_features(
    file: File,
    workers: &[Worker<SmallRng>],
    config: &kmc::Config,
) -> (Vec<u128>, Vec<u128>) {
    let kmc_parser: Dump<KMER_SIZE> = Dump::new(*config);
    let (min_heap, max_heap) = kmc_parser.featurise(file, workers).unwrap();

    let min_features: Vec<u128> = min_heap
        .iter()
        .map(|c| EncodedKMER::from_bits(*c).kmer())
        .collect();
    let max_features: Vec<u128> = max_heap
        .iter()
        .map(|c| EncodedKMER::from_bits(*c).kmer())
        .collect();

    (min_features, max_features)
}

fn process_query(
    query_file: File,
    query_workers: &[Worker<SmallRng>],
    query_parser: &Dump<KMER_SIZE>,
    query_features: &mut HashMap<u128, u16, BuildHasherDefault<FxHasher>>,
) {
    let (min_heap, max_heap) = query_parser.featurise(query_file, query_workers).unwrap();

    for heap_item in min_heap.iter().chain(max_heap.iter()) {
        let encoded = EncodedKMER::from_bits(*heap_item);
        query_features.insert(encoded.kmer(), encoded.count());
    }
}

fn write_feature_line(
    writer: &mut BufWriter<File>,
    path: &Path,
    features: &HashMap<u128, u16, BuildHasherDefault<FxHasher>>,
    ref_features: &[u128],
) {
    let label = path.parent().unwrap().file_name().unwrap();
    let mut line = vec![label.to_str().unwrap().to_string()];
    for feature in ref_features {
        line.push(
            features
                .get(feature)
                .map_or(String::from("0"), |count| count.to_string()),
        );
    }
    let _ = writeln!(writer, "{}", line.join(","));
}
fn convert_fasta_to_fastq(fasta_path: &Path) {
    let read_handle = File::open(fasta_path).unwrap();
    let write_handle = File::create(fasta_path.with_extension("fastq")).unwrap();
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
}

fn main() {
    const CHUNK_SIZE: usize = 524288;
    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();
    let path_out = Path::new("simulated/1K");
    let (kmc_path, kmc_path_dump) = process_kmc_file(path_out);

    println!("Processing dump file");
    let ref_file = File::open(kmc_path_dump).unwrap();
    let _lock = ref_file.lock_shared();

    let init_config = kmc::Config {
        seed: 0,
        threads: THREADS,
        chunk_size: CHUNK_SIZE,
        nlo_results: 50_000,
        nhi_results: 10_000,
    };

    // Create persistent workers
    let workers: Vec<Worker<SmallRng>> = (0..WORKER_THREADS)
        .map(|thread_idx| {
            Worker::new(
                thread_idx,
                ThreadState::<SmallRng>::from_entropy(
                    init_config.nlo_results,
                    init_config.nhi_results,
                    init_config.chunk_size,
                ),
            )
        })
        .collect();

    let dump_start = std::time::Instant::now();
    let (min_features, max_features) = extract_features(ref_file, &workers, &init_config);
    let ref_features: Vec<u128> = min_features.into_iter().chain(max_features).collect();
    // let _ = println!(
    //     "Query,{}",
    //     ref_features.iter().map(|c| unsafe { CODEC.decode(*c) } ).join(",")
    // );
    println!("Dump file time: {:.3}s", dump_start.elapsed().as_secs_f64());
    println!("Features found: {}", ref_features.len());

    let mut feature_writer = create_feature_writer(path_out, &ref_features);

    // Process comparison files
    let compare: Vec<(PathBuf, PathBuf)> = WalkDir::new(path_out)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.path() == path_out || !entry.metadata().ok()?.is_dir() {
                return None;
            }
            Some((
                entry.path().join("Reads_R1").with_extension("fastq"),
                entry.path().join("Reads_R2").with_extension("fastq"),
            ))
        })
        .collect();

    let query_config = kmc::Config {
        seed: 0,
        threads: WORKER_THREADS,
        chunk_size: CHUNK_SIZE,
        nlo_results: init_config.nlo_results * 10,
        nhi_results: init_config.nhi_results * 10,
    };

    // Create new workers with updated config
    drop(workers);
    let query_workers: Vec<Worker<SmallRng>> = (0..WORKER_THREADS)
        .map(|thread_idx| {
            Worker::new(
                thread_idx,
                ThreadState::<SmallRng>::from_entropy(
                    query_config.nlo_results,
                    query_config.nhi_results,
                    query_config.chunk_size,
                ),
            )
        })
        .collect();

    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            query_config.nlo_results + query_config.nhi_results,
            BuildHasherDefault::default(),
        );

    let query_parser = Dump::<KMER_SIZE>::new(query_config);

    let mut progress = Progress::new();
    let bar = progress.bar(compare.len(), "Building Pairwise Feature Matrix");
    let mut idx = 0;
    progress.set_and_draw(&bar, idx);

    for pair in compare {
        let out_path = pair.0.parent().unwrap();
        let kmc_path_dump = out_path.join("kmc_dump").with_extension("txt");

        query_workers.iter().for_each(|worker| worker.state.reset());
        query_features.clear();

        let query_file = File::open(&kmc_path_dump).unwrap();
        let _lock = query_file.lock_shared();

        process_query(
            query_file,
            &query_workers,
            &query_parser,
            &mut query_features,
        );
        println!("Features found: {}", query_features.len());
        write_feature_line(&mut feature_writer, &pair.0, &query_features, &ref_features);

        idx += 1;
        progress.set_and_draw(&bar, idx);
    }

    // Process reference files
    let path_ref = Path::new("data/temp");
    let ref_files: Vec<PathBuf> = WalkDir::new(path_ref)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() || path.extension() != Some("fasta".as_ref()) {
                return None;
            }
            Some(path.to_path_buf())
        })
        .collect();

    let ref_bar = progress.bar(ref_files.len(), "Processing Reference Files");
    let mut ref_idx = 0;
    progress.set_and_draw(&ref_bar, ref_idx);

    for entry_path in ref_files {
        convert_fasta_to_fastq(&entry_path);

        let out_path = entry_path.parent().unwrap();
        let kmc_path_dump = out_path.join("kmc_dump");

        query_workers.iter().for_each(|worker| worker.state.reset());
        query_features.clear();

        let query_file = File::open(&kmc_path_dump).unwrap();
        let _lock = query_file.lock_shared();

        process_query(
            query_file,
            &query_workers,
            &query_parser,
            &mut query_features,
        );
        write_feature_line(
            &mut feature_writer,
            &entry_path,
            &query_features,
            &ref_features,
        );

        ref_idx += 1;
        progress.set_and_draw(&ref_bar, ref_idx);
    }
}
