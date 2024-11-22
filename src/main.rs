use bio::io::fasta::Reader;
use bio::io::fastq::Writer;
use threadpool::ThreadPool;
use std::sync::Arc;
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
use KMER_Select::kmc::{self, Dump, ThreadState};
use KMER_Select::kmer::{Codec, EncodedKMER};
use KMER_Select::simulate::ISSRunner;
use std::time::Instant;

const KMER_SIZE: usize = 31;
const THREADS: usize = 12;
const WORKER_THREADS: usize = THREADS - 1;
const NLO_RESULTS: usize = 50_000;
const NHI_RESULTS: usize = 0;
const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();

struct ProcessResult {
    kmc_path: PathBuf,
    kmc_path_dump: PathBuf,
    processing_time: f64,
}

fn process_kmc_file(path_out: &Path) -> ProcessResult {
    let start = Instant::now();
    
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

    ProcessResult {
        kmc_path,
        kmc_path_dump,
        processing_time: start.elapsed().as_secs_f64(),
    }
}

struct FeatureWriterResult {
    writer: BufWriter<File>,
    creation_time: f64,
}

fn create_feature_writer(path_out: &Path, ref_features: &[u128]) -> FeatureWriterResult {
    let start = Instant::now();
    
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
    
    FeatureWriterResult {
        writer: feature_writer,
        creation_time: start.elapsed().as_secs_f64(),
    }
}

struct ExtractFeaturesResult {
    min_features: Vec<u128>,
    max_features: Vec<u128>,
    extraction_time: f64,
}

fn extract_features(
    file: File,
    thread_states: &[Arc<ThreadState<SmallRng>>],
    thread_pool: &ThreadPool,
    config: &kmc::Config,
) -> ExtractFeaturesResult {
    let start = Instant::now();
    
    let kmc_parser: Dump<KMER_SIZE> = Dump::new(*config);
    let (min_heap, max_heap) = kmc_parser.featurise(file, thread_pool, thread_states).unwrap();

    let min_features: Vec<u128> = min_heap
        .iter()
        .map(|c| EncodedKMER::from_bits(*c).kmer())
        .collect();
    let max_features: Vec<u128> = max_heap
        .iter()
        .map(|c| EncodedKMER::from_bits(*c).kmer())
        .collect();

    ExtractFeaturesResult {
        min_features,
        max_features,
        extraction_time: start.elapsed().as_secs_f64(),
    }
}

fn process_query(
    query_file: File,
    thread_states: &[Arc<ThreadState<SmallRng>>],
    thread_pool: &ThreadPool,
    query_parser: &Dump<KMER_SIZE>,
    query_features: &mut HashMap<u128, u16, BuildHasherDefault<FxHasher>>,
) {
    let (min_heap, max_heap) = query_parser.featurise(query_file, thread_pool, thread_states).unwrap();

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
    let total_start = Instant::now();
    println!("🧬 Starting KMer Analysis");
    println!("  → Configuration: {} threads, {}bp kmers", THREADS, KMER_SIZE);
    
    const CHUNK_SIZE: usize = 524288;
    const CODEC: Codec<KMER_SIZE> = Codec::<KMER_SIZE>::new();
    let path_out = Path::new("simulated/1K");
    
    // Step 1: KMC Processing
    println!("\n[1/4] Starting KMC processing...");
    println!("  → Collecting directory contents...");
    println!("  → Running KMC command...");
    println!("  → Processing KMC dump...");
    let kmc_result = process_kmc_file(path_out);
    println!("✓ KMC processing completed in {:.2}s", kmc_result.processing_time);

    // Step 2: Dump File Processing
    println!("\n[2/4] Processing dump file...");
    let ref_file = File::open(&kmc_result.kmc_path_dump).unwrap();
    let _lock = ref_file.lock_shared();

    let init_config = kmc::Config::new(THREADS, CHUNK_SIZE, NLO_RESULTS, NHI_RESULTS);
    let thread_pool = ThreadPool::new(WORKER_THREADS);

    println!("  → Initializing thread states...");
    let thread_states: Vec<Arc<ThreadState<SmallRng>>> = (0..WORKER_THREADS)
        .map(|_| {
            Arc::new(ThreadState::<SmallRng>::from_entropy(
                (init_config.nlo_results / init_config.work_threads) + 1,
                (init_config.nhi_results / init_config.work_threads) + 1,
                init_config.chunk_size,
            ))
        })
        .collect();

    println!("  → Extracting features using {} threads...", THREADS);
    let feature_result = extract_features(ref_file, &thread_states, &thread_pool, &init_config);
    println!("✓ Feature extraction completed in {:.2}s", feature_result.extraction_time);

    let ref_features: Vec<u128> = feature_result.min_features.into_iter()
        .chain(feature_result.max_features)
        .collect();
    println!("  → Total features identified: {}", ref_features.len());

    // Step 3: Feature Writer Creation
    println!("\n[3/4] Creating feature output file...");
    let feature_writer_result = create_feature_writer(path_out, &ref_features);
    println!("✓ Feature file created in {:.2}s", feature_writer_result.creation_time);
    let mut feature_writer = feature_writer_result.writer;

    // Step 4: Process Comparison Files
    println!("\n[4/4] Processing comparison files...");
    let compare_start = Instant::now();
    
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
    
    // Store the count before consuming the vector
    let compare_count = compare.len();

    let query_config = kmc::Config::new(THREADS, CHUNK_SIZE, NLO_RESULTS * 10, NHI_RESULTS * 10);
    let mut progress = Progress::new();
    let bar = progress.bar(compare.len(), "Processing comparison files");
    let mut idx = 0;

    // Create new thread states with updated config
    let query_states: Vec<Arc<ThreadState<SmallRng>>> = (0..WORKER_THREADS)
        .map(|_| {
            Arc::new(ThreadState::<SmallRng>::from_entropy(
                (init_config.nlo_results / init_config.work_threads) + 1,
                (init_config.nhi_results / init_config.work_threads) + 1,
                query_config.chunk_size,
            ))
        })
        .collect();

    let mut query_features: HashMap<u128, u16, BuildHasherDefault<FxHasher>> =
        HashMap::with_capacity_and_hasher(
            query_config.nlo_results + query_config.nhi_results,
            BuildHasherDefault::default(),
        );

    let query_parser = Dump::<KMER_SIZE>::new(query_config);

    for pair in compare {
        let out_path = pair.0.parent().unwrap();
        let kmc_path_dump = out_path.join("kmc_dump").with_extension("txt");

        query_states.iter().for_each(|state| state.reset());
        query_features.clear();

        let query_file = File::open(&kmc_path_dump).unwrap();
        let _lock = query_file.lock_shared();

        process_query(
            query_file,
            &query_states,
            &thread_pool,
            &query_parser,
            &mut query_features,
        );
        write_feature_line(&mut feature_writer, &pair.0, &query_features, &ref_features);

        idx += 1;
        progress.set_and_draw(&bar, idx);
    }

    // Process reference files section
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
    let ref_count = ref_files.len();

    println!("\nProcessing reference files...");
    let ref_bar = progress.bar(ref_files.len(), "Processing reference files");
    let mut ref_idx = 0;

    for entry_path in ref_files {
        convert_fasta_to_fastq(&entry_path);

        let out_path = entry_path.parent().unwrap();
        let kmc_path_dump = out_path.join("kmc_dump");

        query_states.iter().for_each(|state| state.reset());
        query_features.clear();

        let query_file = File::open(&kmc_path_dump).unwrap();
        let _lock = query_file.lock_shared();

        process_query(
            query_file,
            &query_states,
            &thread_pool,
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

    let total_time = total_start.elapsed();
    println!("\n✨ Analysis complete!");
    println!("  → Total processing time: {:.4}s", total_time.as_secs_f64());
    println!("  → Features processed: {}", ref_features.len());
    println!("  → Files analyzed: {}", compare_count + ref_count);
    println!("  → Average time per file: {:.4}s", 
        (total_time.as_secs_f64() - compare_start.elapsed().as_secs_f64()) / (compare_count + ref_count) as f64);
}