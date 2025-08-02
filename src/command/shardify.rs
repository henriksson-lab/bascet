use anyhow::Result;
use clap::Args;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    io::{traits::*, AutoBascetFile},
    log_critical, log_info, log_warning, support_which_stream, support_which_writer,
    utils::expand_and_resolve,
};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;

pub const DEFAULT_THREADS_READ: usize = 8;
pub const DEFAULT_THREADS_WORK: usize = 4;
pub const DEFAULT_THREADS_TOTAL: usize = 12;

support_which_stream! {
    ShardifyStream<T: BascetStreamToken>
    for formats [tirp]
}
support_which_writer! {
    ShardifyWriter<W: std::io::Write>
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
        log_info!("Starting Shardify";
            "input files" => self.path_in.len(),
            "output path" => ?self.path_out,
            "total threads" => self.threads_total,
            "read threads" => self.threads_read,
            "work threads" => self.threads_work,
        );

        // GOOD FIRST ISSUE:
        // Output files should also use the AutoFile system
        // let expanded_output = expand_and_resolve(&self.path_out)?;
        // if let Some(parent) = expanded_output.parent() {
        //     if !parent.exists() {
        //         log_critical!("Output directory does not exist"; "path" => ?parent);
        //     }
        // }
        // self.path_out = expanded_output;

        let processed_files = Arc::new(AtomicUsize::new(0));
        let total_cells_processed = Arc::new(AtomicUsize::new(0));
        let total_errors = Arc::new(AtomicUsize::new(0));

        let threads_per_stream = self.threads_read / self.path_in.len();

        let (tx, rx) = mpsc::channel::<ShardifyToken>();

        let path_out = self.path_out.clone();
        let writer_handle = thread::spawn(move || {
            use std::sync::mpsc;

            let mut senders = Vec::new();
            let mut writer_handles = Vec::new();

            for (_, path) in path_out.iter().enumerate() {
                let (writer_tx, writer_rx) = mpsc::channel::<ShardifyToken>();
                senders.push(writer_tx);

                let mut total = 0;

                let path = path.clone();
                let file = std::fs::File::create(&path).unwrap();

                let handle = thread::spawn(move || {
                    let output_file = match AutoBascetFile::try_from_path(&path) {
                        Ok(file) => file,
                        Err(e) => {
                            log_critical!("Failed to open input file, skipping"; "path" => ?path, "error" => %e);
                        }
                    };
                    let mut output_writer: ShardifyWriter<BufWriter<std::fs::File>> =
                        ShardifyWriter::try_from_file(output_file).unwrap();
                    output_writer = output_writer.set_writer(BufWriter::new(file));

                    while let Ok(token) = writer_rx.recv() {
                        output_writer.write_cell(token);
                        total += 1;
                        if total % 1000 == 0 {
                            println!("{}", total)
                        }
                    }
                });
                writer_handles.push(handle);
            }

            while let Ok(token) = rx.recv() {
                // Hash cell to determine output file
                let mut hasher = DefaultHasher::new();
                token.cell.hash(&mut hasher);
                let hash = hasher.finish();
                let writer_index = hash as usize % senders.len();

                senders[writer_index].send(token).unwrap();
            }

            drop(senders);
            for handle in writer_handles {
                handle.join().unwrap();
            }
        });

        let handles: Vec<_> = self.path_in.iter().map(|input| {
            let input = input.clone();
            let tx = tx.clone();
            let processed_files = Arc::clone(&processed_files);
            let total_cells_processed = Arc::clone(&total_cells_processed);
            let total_errors = Arc::clone(&total_errors);

            let file = std::fs::File::open(self.include_cells.as_ref().unwrap().as_path()).unwrap();
            let reader = BufReader::new(file);
            let hashset: gxhash::HashSet<Vec<u8>> = reader
                .lines()
                .collect::<Result<Vec<_>, _>>().unwrap()
                .into_iter()
                .map(|line| line.into_bytes())
                .collect();

            thread::spawn(move || {
                log_info!("Processing input file"; "path" => ?input);

                let file = match AutoBascetFile::try_from_path(&input) {
                    Ok(file) => file,
                    Err(e) => {
                        log_warning!("Failed to open input file, skipping"; "path" => ?input, "error" => %e);
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                let mut stream: ShardifyStream<ShardifyToken> = match ShardifyStream::try_from_file(file) {
                    Ok(stream) => stream.set_reader_threads(threads_per_stream),
                    Err(e) => {
                        log_warning!("Failed to create stream from file, skipping"; "path" => ?input, "error" => %e);
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                for token in stream {
                    if let Ok(token) = token {
                        if hashset.contains(token.get_cell().unwrap()) {
                            let _ = tx.send(token);
                        }
                    }
                }

                processed_files.fetch_add(1, Ordering::Relaxed);
            })
        }).collect();

        for handle in handles {
            handle.join().unwrap();
        }

        drop(tx);
        writer_handle.join().unwrap();

        let mut processed_files = processed_files.load(Ordering::Relaxed);
        let mut total_cells_processed = total_cells_processed.load(Ordering::Relaxed);
        let mut total_errors = total_errors.load(Ordering::Relaxed);

        Ok(())
    }
}

#[derive(Debug)]
struct ShardifyToken {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetStreamToken for ShardifyToken {
    type Builder = ShardifyTokenBuilder;
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
struct ShardifyTokenBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl ShardifyTokenBuilder {
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

impl BascetStreamTokenBuilder for ShardifyTokenBuilder {
    type Token = ShardifyToken;

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    #[inline(always)]
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        let aid = Arc::new(id);
        self.underlying.push(aid.clone());
        self.cell = Some(unsafe { std::mem::transmute(aid.as_slice()) });
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        let aseq = Arc::new(seq);
        self.underlying.push(aseq.clone());

        let static_slice: &'static [u8] = unsafe { std::mem::transmute(aseq.as_slice()) };
        self.reads.push((static_slice, &[]));
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
    fn build(self) -> ShardifyToken {
        ShardifyToken {
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
    T: BascetStreamToken,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
