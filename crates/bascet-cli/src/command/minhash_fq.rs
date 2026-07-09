use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::thread;

use anyhow::{Context, Result, anyhow, bail};
use bascet_core::DEFAULT_SIZEOF_ARENA;
use bascet_core::{
    attr::{meta::*, sequence::*},
    *,
};
use bytesize::ByteSize;
use clap::Args;
use crossbeam::channel::{Receiver, Sender, bounded};
use std::collections::HashMap;
use tracing::info;
use zip::ZipWriter;

use crate::kmer::minhash::{MinhashCodec, MinhashKMER};
use crate::kmer::{BoundedHeap, BoundedMinHeap};
use crate::utils::{atomic_temp_path, publish_atomic_output};

/// Default stream decode buffer. Small on purpose: the minhash consumer is fast and
/// parallel, so there is no need for the large buffer the mapcell path used.
const DEFAULT_SIZEOF_STREAM_BUFFER: ByteSize = ByteSize::mib(512);

/// Number of sequences (r1/r2 halves) carried per batch handed to a worker.
const BATCH_SEQS: usize = 8192;

/// Bounded depth of each per-worker channel (backpressure on the reader).
const WORKER_QUEUE_DEPTH: usize = 4;

#[derive(Args)]
pub struct MinhashFqCMD {
    /// Input TIRP file (records must be sorted by cell, as Bascet TIRP always is).
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    /// Output zip file. One entry `<cell>/minhash.txt` per cell.
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    /// Number of minhash worker threads.
    #[arg(short = '@', long = "workers", default_value_t = 1)]
    pub workers: usize,

    /// Threads used by the TIRP BGZF decoder.
    #[arg(long = "num-threads-read", default_value_t = 1)]
    pub num_threads_read: usize,

    /// K-mer length (1..=32).
    #[arg(long = "kmer", default_value_t = 31)]
    pub kmer: usize,

    /// Number of minhashes (features) kept per cell.
    #[arg(long = "num-minhash", default_value_t = 1000)]
    pub num_minhash: usize,

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

impl MinhashFqCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        self.validate()?;
        info!(
            path_in = %self.path_in.display(),
            workers = self.workers,
            kmer = self.kmer,
            num_minhash = self.num_minhash,
            sizeof_stream_buffer = %self.sizeof_stream_buffer,
            "minhash-fq"
        );
        run_minhash_fq(
            self.path_in.clone(),
            self.path_out.clone(),
            self.workers,
            self.num_threads_read,
            self.kmer,
            self.num_minhash,
            self.sizeof_stream_arena,
            self.sizeof_stream_buffer,
        )
    }

    fn validate(&self) -> Result<()> {
        if self.workers == 0 {
            bail!("--workers must be > 0");
        }
        if self.num_threads_read == 0 {
            bail!("--num-threads-read must be > 0");
        }
        if self.kmer == 0 || self.kmer > 32 {
            bail!("--kmer must be between 1 and 32");
        }
        if self.num_minhash == 0 {
            bail!("--num-minhash must be > 0");
        }
        Ok(())
    }
}

/// A chunk of sequences (r1/r2 halves) belonging to a single cell, or a marker
/// that the reader has finished the current cell.
enum Msg {
    Batch(Vec<Vec<u8>>),
    EndCell { idx: u32, name: String },
}

/// A worker's minhash for one cell, on its way to the merger.
type Partial = (u32, String, BoundedMinHeap<MinhashKMER>);

fn run_minhash_fq(
    path_in: PathBuf,
    path_out: PathBuf,
    workers: usize,
    num_threads_read: usize,
    kmer: usize,
    num_minhash: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
) -> Result<()> {
    // Merger channel: workers emit exactly `workers` partials per cell.
    let (tx_merge, rx_merge) = bounded::<Partial>(workers * 4);

    // One channel per worker so the reader can fan an EndCell out to all of them.
    let mut senders: Vec<Sender<Msg>> = Vec::with_capacity(workers);
    let mut worker_handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx_w, rx_w) = bounded::<Msg>(WORKER_QUEUE_DEPTH);
        senders.push(tx_w);
        let tx_merge = tx_merge.clone();
        worker_handles.push(thread::spawn(move || {
            worker_loop(rx_w, tx_merge, kmer, num_minhash)
        }));
    }
    // Only the workers should keep the merger alive.
    drop(tx_merge);

    let merger = thread::spawn(move || merger_loop(rx_merge, workers, kmer, num_minhash, path_out));

    let reader = thread::spawn(move || {
        reader_loop(
            path_in,
            num_threads_read,
            sizeof_stream_arena,
            sizeof_stream_buffer,
            senders,
        )
    });

    let reader_res = reader
        .join()
        .map_err(|_| anyhow!("reader thread panicked"))?;
    for h in worker_handles {
        h.join().map_err(|_| anyhow!("worker thread panicked"))?;
    }
    let merger_res = merger
        .join()
        .map_err(|_| anyhow!("merger thread panicked"))?;

    reader_res?;
    merger_res?;
    Ok(())
}

/// Stream the cell-sorted TIRP, round-robin each cell's sequences across workers,
/// and mark cell boundaries. Holds at most one batch in memory — never a whole cell.
fn reader_loop(
    path_in: PathBuf,
    num_threads_read: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    senders: Vec<Sender<Msg>>,
) -> Result<()> {
    let num_threads = bounded_integer::BoundedU64::new(num_threads_read as u64)
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
    let mut num_cells: u64 = 0;

    // Flush the pending batch to the next worker in round-robin order.
    let flush = |batch: &mut Vec<Vec<u8>>, next_worker: &mut usize| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let out = std::mem::replace(batch, Vec::with_capacity(BATCH_SEQS));
        senders[*next_worker]
            .send(Msg::Batch(out))
            .map_err(|_| anyhow!("worker channel closed early"))?;
        *next_worker = (*next_worker + 1) % nworkers;
        Ok(())
    };

    // Finalise the current cell: flush its tail batch, then fan EndCell to all workers.
    let end_cell =
        |batch: &mut Vec<Vec<u8>>, next_worker: &mut usize, idx: u32, name: &str| -> Result<()> {
            flush(batch, next_worker)?;
            for s in &senders {
                s.send(Msg::EndCell {
                    idx,
                    name: name.to_string(),
                })
                .map_err(|_| anyhow!("worker channel closed early"))?;
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
    // senders dropped here -> workers observe disconnect and finish.
    Ok(())
}

/// Each worker keeps a single bounded minheap for the cell currently in flight.
fn worker_loop(rx: Receiver<Msg>, tx_merge: Sender<Partial>, kmer: usize, num_minhash: usize) {
    let mut heap: BoundedMinHeap<MinhashKMER> = BoundedMinHeap::with_capacity(num_minhash);
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Batch(seqs) => {
                for seq in &seqs {
                    for window in seq.windows(kmer) {
                        let _ = BoundedHeap::push(&mut heap, MinhashKMER::new(window));
                    }
                }
            }
            Msg::EndCell { idx, name } => {
                let done = std::mem::replace(&mut heap, BoundedMinHeap::with_capacity(num_minhash));
                if tx_merge.send((idx, name, done)).is_err() {
                    return;
                }
            }
        }
    }
}

/// Combine the per-worker partial heaps for each cell and write it to the zip as
/// soon as all workers have reported it. Only in-flight (incomplete) cells are held.
fn merger_loop(
    rx: Receiver<Partial>,
    num_workers: usize,
    kmer: usize,
    num_minhash: usize,
    path_out: PathBuf,
) -> Result<()> {
    let path_tmp = atomic_temp_path(&path_out);
    let file = File::create(&path_tmp)
        .with_context(|| format!("failed to create output zip {}", path_tmp.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(file));
    let options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    let codec = MinhashCodec::new(kmer);

    // idx -> (name, accumulated heap, #workers reported so far)
    let mut pending: HashMap<u32, (String, BoundedMinHeap<MinhashKMER>, usize)> = HashMap::new();
    let mut num_cells: u64 = 0;

    while let Ok((idx, name, part)) = rx.recv() {
        let entry = pending
            .entry(idx)
            .or_insert_with(|| (name, BoundedMinHeap::with_capacity(num_minhash), 0));
        for kmer_val in part.iter() {
            let _ = BoundedHeap::push(&mut entry.1, *kmer_val);
        }
        entry.2 += 1;

        if entry.2 == num_workers {
            let (name, mut heap, _) = pending.remove(&idx).unwrap();
            write_cell_minhash(&mut zip_writer, &options, &name, &mut heap, &codec)?;
            num_cells += 1;
            if num_cells % 1000 == 0 {
                info!("wrote minhash for {} cells", num_cells);
            }
        }
    }

    zip_writer.finish()?;
    publish_atomic_output(&path_tmp, &path_out)?;
    info!("wrote minhash for final total of {} cells", num_cells);
    Ok(())
}

/// Write one cell's minhash as sorted k-mer strings. Mirrors `MinHash::store_minhash_seq`
/// (minhash.rs) but streams into the zip instead of a standalone file.
fn write_cell_minhash(
    zip_writer: &mut ZipWriter<BufWriter<File>>,
    options: &zip::write::FileOptions<()>,
    name: &str,
    heap: &mut BoundedMinHeap<MinhashKMER>,
    codec: &MinhashCodec,
) -> Result<()> {
    let mut list: Vec<Vec<u8>> = Vec::with_capacity(heap.len());
    while let Some(h) = heap.pop_min() {
        list.push(h.decode(codec));
    }
    list.sort();

    zip_writer.start_file(format!("{}/minhash.txt", name), *options)?;
    for kmer_string in list {
        zip_writer.write_all(&kmer_string)?;
        zip_writer.write_all(b"\n")?;
    }
    Ok(())
}

fn cell_name(cell_id: &[u8]) -> Result<String> {
    let name = String::from_utf8(cell_id.to_vec()).context("cell id in TIRP is not valid UTF-8")?;
    if name.is_empty() {
        bail!("empty cell id is not supported");
    }
    if name.contains('/') || name.contains('\\') || name == "." || name == ".." {
        bail!("cell id {:?} cannot be used as a zip directory", name);
    }
    Ok(name)
}
