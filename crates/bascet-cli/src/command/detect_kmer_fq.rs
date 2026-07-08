use anyhow::{Context, Result, anyhow, bail};
use bascet_core::DEFAULT_SIZEOF_ARENA;
use bascet_core::{
    attr::{meta::*, sequence::*},
    *,
};
use bytesize::ByteSize;
use clap::Args;
use crossbeam::channel::{Receiver, Sender, bounded};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tracing::info;

use crate::fileformat::new_anndata::SparseMatrixAnnDataBuilder;
use crate::fileformat::{DetectedFileformat, detect_shard_format};
use crate::utils::{atomic_temp_path, publish_atomic_output};

pub const DEFAULT_PATH_TEMP: &str = "temp";
const DEFAULT_SIZEOF_STREAM_BUFFER: ByteSize = ByteSize::mib(512);
const BATCH_SEQS: usize = 8192;
const WORKER_QUEUE_DEPTH: usize = 4;

/// Commandline option: Check FASTQ for occurences of given list of KMERs
#[derive(Args)]
pub struct DetectKmerFqCMD {
    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Threads
    #[arg(short = '@', long = "threads", value_parser= clap::value_parser!(usize), default_value = "10")]
    pub num_threads_total: usize,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    // Input feature file (text file, one kmer per line)
    #[arg(short = 'f', value_parser = clap::value_parser!(PathBuf))]
    pub path_features: PathBuf,

    #[arg(long = "num-threads-read", default_value_t = 1)]
    pub num_threads_read: usize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
        default_value_t = DEFAULT_SIZEOF_STREAM_BUFFER,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: ByteSize,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,
}
impl DetectKmerFqCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        let params = DetectKmerFq {
            path_tmp: self.path_tmp.clone(),
            path_input: self.path_in.clone(),
            path_output: self.path_out.clone(),
            path_features: self.path_features.clone(),
            num_threads_total: self.num_threads_total,
            num_threads_read: self.num_threads_read,
            sizeof_stream_buffer: self.sizeof_stream_buffer,
            sizeof_stream_arena: self.sizeof_stream_arena,
        };

        DetectKmerFq::run(&Arc::new(params))?;

        info!("Query has finished succesfully");
        Ok(())
    }
}

/// Algorithm: Check FASTQ for occurences of given list of KMERs
pub struct DetectKmerFq {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
    pub path_features: std::path::PathBuf,
    pub num_threads_total: usize,
    pub num_threads_read: usize,
    pub sizeof_stream_buffer: ByteSize,
    pub sizeof_stream_arena: ByteSize,
}
impl DetectKmerFq {
    /// Run the algorithm
    pub fn run(params: &Arc<DetectKmerFq>) -> anyhow::Result<()> {
        if detect_shard_format(&params.path_input) != DetectedFileformat::TIRP {
            bail!(
                "detect-kmer-fq now uses bounded streaming and currently supports TIRP input only"
            );
        }

        //Prepare matrix that we will store into
        let mut mm = SparseMatrixAnnDataBuilder::new();

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            //anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            info!("Using tempdir {}", params.path_tmp.display());
            if fs::create_dir_all(&params.path_tmp).is_err() {
                panic!("Failed to create temporary directory");
            };
        }

        //Below reads list of features to include. Set up a map: KMER => column in matrix.
        //Also figure out what kmer size to use.
        //Ensure order of KMER in dictionary is the same as the order of columns in matrix
        let mut features_reference: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        let file_features_ref = File::open(&params.path_features).unwrap();
        let bufreader_features_ref = BufReader::new(&file_features_ref);
        let mut kmer_size = 0;

        let mut all_features: Vec<Vec<u8>> = Vec::new();
        for rline in bufreader_features_ref.lines() {
            let feature = rline.unwrap();
            all_features.push(feature.as_bytes().to_vec());
        }
        all_features.sort();

        //Allocate positions in matrix for each feature
        for feature in all_features {
            //Detect kmer size. should be the same for all entries, not checked
            kmer_size = feature.len();

            //Get feature index
            let sfeature = String::from_utf8_lossy(feature.as_slice());
            let feature_index = mm.get_or_create_feature(&sfeature.to_string().as_bytes());
            features_reference.insert(feature, feature_index);
        }

        if kmer_size == 0 {
            anyhow::bail!("Feature file has no features");
        } else {
            info!(
                "Read {} features. Detected kmer-length of {}",
                features_reference.len(),
                kmer_size
            );
        }

        let workers = params.num_threads_total.max(1);
        let features_reference = Arc::new(features_reference);
        let (tx_merge, rx_merge) = bounded::<Partial>(workers * 4);
        let mut senders: Vec<Sender<Msg>> = Vec::with_capacity(workers);
        let mut worker_handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx_worker, rx_worker) = bounded::<Msg>(WORKER_QUEUE_DEPTH);
            senders.push(tx_worker);
            let tx_merge = tx_merge.clone();
            let features_reference = Arc::clone(&features_reference);
            worker_handles.push(thread::spawn(move || {
                worker_loop(rx_worker, tx_merge, features_reference, kmer_size)
            }));
        }
        drop(tx_merge);

        let path_output = params.path_output.clone();
        let merger = thread::spawn(move || merger_loop(rx_merge, workers, mm, path_output));

        let reader_path = params.path_input.clone();
        let num_threads_read = params.num_threads_read;
        let sizeof_stream_arena = params.sizeof_stream_arena;
        let sizeof_stream_buffer = params.sizeof_stream_buffer;
        let reader = thread::spawn(move || {
            reader_loop(
                reader_path,
                num_threads_read,
                sizeof_stream_arena,
                sizeof_stream_buffer,
                senders,
            )
        });

        reader
            .join()
            .map_err(|_| anyhow!("detect-kmer-fq reader thread panicked"))??;
        for handle in worker_handles {
            handle
                .join()
                .map_err(|_| anyhow!("detect-kmer-fq worker thread panicked"))?;
        }
        merger
            .join()
            .map_err(|_| anyhow!("detect-kmer-fq merger thread panicked"))??;

        Ok(())
    }
}

enum Msg {
    Batch(Vec<Vec<u8>>),
    EndCell { idx: u32, name: String },
}

type FeatureCounts = BTreeMap<u32, u32>;
type Partial = (u32, String, FeatureCounts);

fn reader_loop(
    path_in: PathBuf,
    num_threads_read: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    senders: Vec<Sender<Msg>>,
) -> Result<()> {
    let num_threads = bounded_integer::BoundedU64::new(num_threads_read.max(1) as u64)
        .context("invalid read thread count")?;
    let decoder: bascet_io::BBGZDecoder = bascet_io::codec::BBGZDecoder::builder()
        .with_path(&path_in)
        .countof_threads(num_threads)
        .build();
    let parser = bascet_io::parse::Tirp::builder().build();
    let mut stream = bascet_core::Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();
    let mut query = stream.query::<bascet_io::tirp::Record>();

    let nworkers = senders.len();
    let mut next_worker = 0usize;
    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SEQS);
    let mut have_cell = false;
    let mut cur_id: Vec<u8> = Vec::new();
    let mut cur_name = String::new();
    let mut cur_idx: u32 = 0;
    let mut num_cells = 0u64;

    let flush = |batch: &mut Vec<Vec<u8>>, next_worker: &mut usize| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let out = std::mem::replace(batch, Vec::with_capacity(BATCH_SEQS));
        senders[*next_worker]
            .send(Msg::Batch(out))
            .map_err(|_| anyhow!("detect-kmer-fq worker channel closed early"))?;
        *next_worker = (*next_worker + 1) % nworkers;
        Ok(())
    };

    let end_cell =
        |batch: &mut Vec<Vec<u8>>, next_worker: &mut usize, idx: u32, name: &str| -> Result<()> {
            flush(batch, next_worker)?;
            for sender in &senders {
                sender
                    .send(Msg::EndCell {
                        idx,
                        name: name.to_string(),
                    })
                    .map_err(|_| anyhow!("detect-kmer-fq worker channel closed early"))?;
            }
            Ok(())
        };

    while let Some(record) = query
        .next_into::<bascet_io::tirp::Record>()
        .context("failed to read TIRP record")?
    {
        let record_id = *record.get_ref::<Id>();

        if !have_cell {
            have_cell = true;
            cur_id = record_id.to_vec();
            cur_name = cell_name(&cur_id)?;
            cur_idx = 0;
        } else if record_id != cur_id.as_slice() {
            end_cell(&mut batch, &mut next_worker, cur_idx, &cur_name)?;
            num_cells += 1;
            if num_cells % 1000 == 0 {
                info!("queued {} cells", num_cells);
            }
            cur_id = record_id.to_vec();
            cur_name = cell_name(&cur_id)?;
            cur_idx += 1;
        }

        batch.push((*record.get_ref::<R1>()).to_vec());
        batch.push((*record.get_ref::<R2>()).to_vec());
        if batch.len() >= BATCH_SEQS {
            flush(&mut batch, &mut next_worker)?;
        }
    }

    if have_cell {
        end_cell(&mut batch, &mut next_worker, cur_idx, &cur_name)?;
        num_cells += 1;
    }
    info!("queued final total of {} cells", num_cells);
    Ok(())
}

fn worker_loop(
    rx: Receiver<Msg>,
    tx_merge: Sender<Partial>,
    features_reference: Arc<BTreeMap<Vec<u8>, u32>>,
    kmer_size: usize,
) {
    info!("Starting KMER counter process");
    let mut features_count: FeatureCounts = BTreeMap::new();
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Batch(seqs) => {
                for seq in &seqs {
                    count_from_seq(&features_reference, &mut features_count, seq, kmer_size)
                        .unwrap();
                }
            }
            Msg::EndCell { idx, name } => {
                let done = std::mem::take(&mut features_count);
                if tx_merge.send((idx, name, done)).is_err() {
                    return;
                }
            }
        }
    }
    info!("Shutting down KMER counter");
}

fn merger_loop(
    rx: Receiver<Partial>,
    num_workers: usize,
    mut mm: SparseMatrixAnnDataBuilder,
    path_output: PathBuf,
) -> Result<()> {
    let mut pending: HashMap<u32, (String, FeatureCounts, usize)> = HashMap::new();
    let mut num_cells = 0u64;

    while let Ok((idx, name, part)) = rx.recv() {
        let entry = pending
            .entry(idx)
            .or_insert_with(|| (name, BTreeMap::new(), 0));
        for (feature_index, cnt) in part {
            *entry.1.entry(feature_index).or_default() += cnt;
        }
        entry.2 += 1;

        if entry.2 == num_workers {
            let (name, mut counts, _) = pending.remove(&idx).unwrap();
            let cell_index = mm.get_or_create_cell(name.as_bytes());
            mm.add_cell_counts_per_feature_index(cell_index, &mut counts);
            if cell_index % 10 == 0 {
                info!("Counted KMERs from cells: {}", cell_index);
            }
            num_cells += 1;
        }
    }

    info!("Storing count table to {}", path_output.display());
    let path_tmp = atomic_temp_path(&path_output);
    mm.save_to_anndata(&path_tmp)
        .context("failed to save detect-kmer-fq HDF5 file")?;
    publish_atomic_output(path_tmp, &path_output)?;
    info!("Stored KMER counts for final total of {} cells", num_cells);
    Ok(())
}

/// Get KMER counts from a sequence
fn count_from_seq(
    features_reference: &BTreeMap<Vec<u8>, u32>, //Map from feature to index
    features_count: &mut FeatureCounts,
    seq: &[u8],
    kmer_size: usize,
) -> anyhow::Result<()> {
    //Check for presence of chosen KMERs
    for kmer in seq.windows(kmer_size) {
        if let Some(feature_index) = features_reference.get(kmer) {
            *features_count.entry(*feature_index).or_default() += 1;
        }
    }

    let rc_seq = revcomp(seq);

    //Check for presence of chosen KMERs -- reverse complement
    for kmer in rc_seq.windows(kmer_size) {
        if let Some(feature_index) = features_reference.get(kmer) {
            *features_count.entry(*feature_index).or_default() += 1;
        }
    }

    Ok(())
}

/// Implementation is taken from https://doi.org/10.1101/082214
/// This function handles ATCG
fn revcomp(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|c| if c & 2 != 0 { c ^ 4 } else { c ^ 21 })
        .collect()
}

fn cell_name(cell_id: &[u8]) -> Result<String> {
    let name = String::from_utf8(cell_id.to_vec()).context("cell id in TIRP is not valid UTF-8")?;
    if name.is_empty() {
        bail!("empty cell id is not supported");
    }
    Ok(name)
}
