use anyhow::Result;
use bgzip::{write::BGZFMultiThreadWriter, Compression};
use clap::Args;
use gxhash::GxHasher;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    io::traits::*, log_critical, log_info, log_warning, support_which_stream, support_which_writer,
};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;

pub const DEFAULT_THREADS_READ: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 2;
pub const DEFAULT_THREADS_TOTAL: usize = 12;

support_which_stream! {
    ShardifyInput => ShardifyStream<T: BascetCell>
    for formats [tirp]
}
support_which_writer! {
    ShardifyOutput => ShardifyWriter<W: std::io::Write>
    for formats [tirp]
}
/// Commandline option: Take parsed reads and organize them as shards
#[derive(Args)]
pub struct ShardifyCMD {
    // Input bascets (comma separated; ok with PathBuf???)
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,

    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_TOTAL)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
}

impl ShardifyCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        let (collector_tx, collector_rx) =
            crossbeam::channel::bounded::<(usize, Option<ShardifyCell>)>(1000);

        let writer_pool = Arc::new(WriterPool::new(&self.path_out));

        let writer_pool_clone = Arc::clone(&writer_pool);
        let num_readers = self.path_in.len();
        let collector_handle = thread::spawn(move || {
            use std::collections::BTreeMap;

            let mut reader_minimums: Vec<Option<Vec<u8>>> = vec![None; num_readers];
            let mut pending_tokens: BTreeMap<Vec<u8>, (usize, ShardifyCell)> = BTreeMap::new();

            while let Ok((reader_id, token_opt)) = collector_rx.recv() {
                match token_opt {
                    Some(token) => {
                        let cell_bytes = token.get_cell().unwrap().to_vec();

                        reader_minimums[reader_id] = Some(cell_bytes.clone());

                        pending_tokens
                            .entry(cell_bytes)
                            .and_modify(|(count, _)| *count += 1)
                            .or_insert((1, token));

                        let global_min = reader_minimums
                            .iter()
                            .filter_map(|m| m.as_ref())
                            .min()
                            .cloned();

                        if let Some(min) = global_min {
                            let ready_tokens: Vec<_> = pending_tokens
                                .range(..min)
                                .map(|(k, _)| k.clone())
                                .collect();

                            for token_key in ready_tokens {
                                if let Some((_, token)) = pending_tokens.remove(&token_key) {
                                    let mut hasher = GxHasher::default();
                                    token.cell.hash(&mut hasher);
                                    let writer_index =
                                        hasher.finish() as usize % writer_pool_clone.writer_count();

                                    writer_pool_clone.write(writer_index, token);
                                }
                            }
                        }
                    }
                    None => {
                        reader_minimums[reader_id] = None;
                    }
                }
            }

            for (_, (_, token)) in pending_tokens {
                let mut hasher = GxHasher::default();
                token.cell.hash(&mut hasher);
                let writer_index = hasher.finish() as usize % writer_pool_clone.writer_count();

                writer_pool_clone.write(writer_index, token);
            }

            writer_pool_clone.shutdown();
        });

        let filter = self.include_cells.as_ref().map(|path| {
            let file = File::open(path).unwrap();
            let reader = BufReader::new(file);
            reader
                .lines()
                .map(|line| line.unwrap().into_bytes())
                .collect::<gxhash::HashSet<Vec<u8>>>()
        });
        println!("filter len: {}", filter.as_ref().unwrap().len());

        let stream_threads = self.threads_read / self.path_in.len();
        let stream_handles: Vec<_> = self
            .path_in
            .iter()
            .enumerate()
            .map(|(reader_id, input_path)| {
                let input_path = input_path.clone();
                let collector_tx = collector_tx.clone();
                let filter = filter.clone();
                let mut filtered = 0;
                let mut processed = 0;

                thread::spawn(move || {
                    let file = ShardifyInput::try_from_path(&input_path).unwrap();
                    let stream: ShardifyStream<ShardifyCell> =
                        ShardifyStream::try_from_input(file).unwrap().set_reader_threads(stream_threads);

                    for token in stream {
                        let token = token.unwrap();

                        if let Some(ref filter) = filter {
                            if !filter.contains(token.get_cell().unwrap()) {
                                continue;
                            }
                        }
                        collector_tx.send((reader_id, Some(token))).unwrap();
                    }
                    collector_tx.send((reader_id, None)).unwrap();
                })
            })
            .collect();

        for handle in stream_handles {
            handle.join().unwrap();
        }

        drop(collector_tx);
        collector_handle.join().unwrap();

        Ok(())
    }
}

#[derive(Debug)]
struct ShardifyCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetCell for ShardifyCell {
    type Builder = ShardifyCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }

    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.qualities)
    }

    fn get_umis(&self) -> Option<&[&[u8]]> {
        Some(&self.umis)
    }
}
struct ShardifyCellBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl ShardifyCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            qualities: Vec::new(),
            umis: Vec::new(),
            underlying: Vec::new(),
        }
    }
}

impl BascetCellBuilder for ShardifyCellBuilder {
    type Token = ShardifyCell;

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    #[inline(always)]
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        let aid = Arc::new(id);
        self = self.add_underlying(aid.clone()).add_cell_id_slice(&aid);
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        let aseq = Arc::new(seq);
        self = self.add_underlying(aseq.clone()).add_cell_id_slice(&aseq);
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by Arcs. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(static_slice);
        self
    }

    #[inline(always)]
    fn add_rp_slice(mut self, r1: &[u8], r2: &[u8]) -> Self {
        let r1_static_slice: &'static [u8] = unsafe { std::mem::transmute(r1) };
        let r2_static_slice: &'static [u8] = unsafe { std::mem::transmute(r2) };
        self.reads.push((r1_static_slice, r2_static_slice));
        self
    }
    #[inline(always)]
    fn add_qp_slice(mut self, q1: &[u8], q2: &[u8]) -> Self {
        let q1_static_slice: &'static [u8] = unsafe { std::mem::transmute(q1) };
        let q2_static_slice: &'static [u8] = unsafe { std::mem::transmute(q2) };
        self.qualities.push((q1_static_slice, q2_static_slice));
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push((static_slice, &[]));
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.qualities.push((static_slice, &[]));
        self
    }

    fn add_umi_slice(mut self, umi: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(umi) };
        self.umis.push(static_slice);
        self
    }

    #[inline(always)]
    fn add_underlying(mut self, buffer: Arc<Vec<u8>>) -> Self {
        self.underlying.push(buffer);
        self
    }

    #[inline(always)]
    fn build(self) -> ShardifyCell {
        ShardifyCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            qualities: self.qualities,
            umis: self.umis,
            _underlying: self.underlying,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for ShardifyStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}

struct WriterPool {
    writers: Vec<Arc<Mutex<WriterHandle>>>,
}

impl WriterPool {
    fn new(paths: &[PathBuf]) -> Self {
        let writers = paths
            .iter()
            .map(|path| Arc::new(Mutex::new(WriterHandle::new(path.clone()))))
            .collect();

        WriterPool { writers }
    }

    fn writer_count(&self) -> usize {
        self.writers.len()
    }

    fn write(&self, index: usize, token: ShardifyCell) {
        let mut writer = self.writers[index].lock().unwrap();
        writer.write(token);
    }

    fn shutdown(&self) {
        for writer in &self.writers {
            let mut writer = writer.lock().unwrap();
            writer.shutdown();
        }
    }
}

struct WriterHandle {
    tx: Option<crossbeam::channel::Sender<ShardifyCell>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl WriterHandle {
    fn new(path: PathBuf) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<ShardifyCell>();
        let handle = thread::spawn(move || {
            let file = File::create(&path).unwrap();
            let output_file = ShardifyOutput::try_from_path(&path).unwrap();
            let bgzf_writer: BGZFMultiThreadWriter<BufWriter<File>> = BGZFMultiThreadWriter::new(BufWriter::new(file), Compression::fast());
            let mut writer: ShardifyWriter<BGZFMultiThreadWriter<BufWriter<File>>> =
                ShardifyWriter::try_from_output(output_file)
                    .unwrap()
                    .set_writer(bgzf_writer);
            
            while let Ok(token) = rx.recv() {
                writer.write_cell(&token).unwrap();
            }
            let _ = writer.get_writer().unwrap().flush();
        });

        WriterHandle {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    fn write(&mut self, token: ShardifyCell) {
        if let Some(ref tx) = self.tx {
            tx.send(token).unwrap();
        }
    }

    fn shutdown(&mut self) {
        self.tx = None;
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}
