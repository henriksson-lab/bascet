use crate::{
    command::determine_thread_counts_2,
    common,
    io::{traits::*, AutoBascetFile},
    kmer::kmc_counter::CountSketch,
    log_critical, log_info, log_warning, support_which_stream,
    utils::expand_and_resolve,
};

use clap::Args;
use enum_dispatch::enum_dispatch;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

support_which_stream! {
    AutoStream<T: BascetStreamToken>
    for formats [tirp, zip]
}

pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WORK: usize = 11;
pub const DEFAULT_THREADS_TOTAL: usize = 12;
pub const DEFAULT_COUNTSKETCH_SIZE: usize = 128;
pub const DEFAULT_KMER_SIZE: usize = 31;
pub const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 128;

#[derive(Args)]
pub struct CountsketchCMD {
    // Input bascets
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,
    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,
    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_TOTAL)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
    // CountSketch parameters
    #[arg(long = "sketch-size", value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_COUNTSKETCH_SIZE)]
    pub countsketch_size: usize,
    // K-mer size
    #[arg(short = 'k', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_KMER_SIZE)]
    pub kmer_size: usize,
    // Channel buffer size
    #[arg(long = "buffer-size", value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_CHANNEL_BUFFER_SIZE)]
    pub channel_buffer_size: usize,
}

impl CountsketchCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        log_info!("Starting CountSketch";
            "input files" => self.path_in.len(),
            "output path" => ?self.path_out,
            "total threads" => self.threads_total,
            "read threads" => self.threads_read,
            "work threads" => self.threads_work,
            "countsketch size" => self.countsketch_size,
            "kmer size" => self.kmer_size
        );

        let (n_readers, n_workers) = match determine_thread_counts_2(
            Some(self.threads_total),
            Some(self.threads_read),
            Some(self.threads_work),
        ) {
            Ok(counts) => {
                log_info!("Thread allocation successful"; "readers" => counts.0, "workers" => counts.1);
                counts
            }
            Err(e) => {
                log_critical!("Failed to determine thread counts"; "error" => %e);
            }
        };

        // GOOD FIRST ISSUE:
        // Output files should also use the AutoFile system
        let expanded_output = expand_and_resolve(&self.path_out)?;
        if let Some(parent) = expanded_output.parent() {
            if !parent.exists() {
                log_critical!("Output directory does not exist"; "path" => ?parent);
            }
        }
        self.path_out = expanded_output;

        let mut processed_files = 0;
        let mut total_cells_processed = 0;
        let mut total_errors = 0;

        for input in &self.path_in {
            log_info!("Processing input file"; "path" => ?input);

            if !input.exists() {
                log_warning!("Input file does not exist, skipping"; "path" => ?input);
                total_errors += 1;
                continue;
            }

            let file = match AutoBascetFile::try_from_path(input) {
                Ok(file) => file,
                Err(e) => {
                    log_warning!("Failed to open input file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let stream: AutoStream<StreamToken> = match AutoStream::try_from_file(file) {
                Ok(stream) => stream.set_reader_threads(n_readers),
                Err(e) => {
                    log_warning!("Failed to create stream from file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let output_path_buf = input.with_extension("countsketch.txt");
            let output_filename = match output_path_buf.file_name() {
                Some(name) => name,
                None => {
                    log_warning!("Failed to generate output filename, skipping"; "input" => ?input);
                    total_errors += 1;
                    continue;
                }
            };

            let output_path = self.path_out.join(output_filename);

            let output_file = match File::create(&output_path) {
                Ok(file) => file,
                Err(e) => {
                    log_critical!("Failed to create output file"; "path" => ?output_path, "error" => %e);
                }
            };

            let bufwriter = BufWriter::new(output_file);
            let bufwriter = Arc::new(Mutex::new(bufwriter));

            let worker_threadpool = threadpool::ThreadPool::new(n_workers);
            let (work_tx, work_rx) =
                crossbeam::channel::bounded::<Option<StreamToken>>(self.channel_buffer_size);

            for worker_id in 0..n_workers {
                let work_rx = work_rx.clone();

                let kmer_size = self.kmer_size;
                let mut countsketch = CountSketch::new(self.countsketch_size);

                let bufwriter = Arc::clone(&bufwriter);
                let mut buffer: Vec<u8> = Vec::new();

                worker_threadpool.execute(move || {
                    let mut cells_processed = 0;

                    while let Ok(Some(cell)) = work_rx.recv() {
                        countsketch.reset();

                        for read in cell.reads.iter() {
                            if read.len() >= kmer_size {
                                let kmers = read.windows(kmer_size);
                                for kmer in kmers {
                                    countsketch.add(kmer);
                                }
                            }
                        }

                        for read in cell.reads.iter() {
                            if read.len() >= kmer_size {
                                // GOOD FIRST ISSUE:
                                // writing into the memory the read originates would be unsafe behaviour 
                                // but is safe in this context since nothing will ever access that memory again
                                // would probably be a bit faster than collecting into a Vec
                                let rev_read: Vec<u8> = read
                                    .iter()
                                    .rev()
                                    .map(|&base| match base {
                                        b'A' => b'T',
                                        b'T' => b'A',
                                        b'G' => b'C',
                                        b'C' => b'G',
                                        _ => base,
                                    })
                                    .collect();

                                let kmers = rev_read.windows(kmer_size);
                                for kmer in kmers {
                                    countsketch.add(kmer);
                                }
                            }
                        }

                        buffer.clear();
                        buffer.extend_from_slice(cell.cell);
                        buffer.push(common::U8_CHAR_TAB);
                        buffer.extend_from_slice(cell.reads.len().to_string().as_bytes());

                        for value in countsketch.sketch.iter() {
                            buffer.push(common::U8_CHAR_TAB);
                            buffer.extend_from_slice(value.to_string().as_bytes());
                        }
                        buffer.push(common::U8_CHAR_NEWLINE);

                        // NOTE: lock free aproaches for writing did not perform much better
                        match bufwriter.lock() {
                            Ok(mut writer) => {
                                if let Err(e) = writer.write_all(&buffer) {
                                    log_warning!("Write error in worker thread"; "worker id" => worker_id, "error" => %e);
                                }
                            }
                            Err(_) => {
                                log_warning!("Buffer lock contention in worker"; "worker id" => worker_id);
                            }
                        }

                        cells_processed += 1;
                    }

                    log_info!("Worker thread completed"; "worker id" => worker_id, "cells processed" => cells_processed);
                });
            }

            let mut cells_parsed = 0;
            let mut parse_errors = 0;

            for cell_res in stream {
                let cell = match cell_res {
                    Ok(cell) => cell,
                    Err(e) => match e {
                        crate::runtime::Error::ParseError { .. } => {
                            log_warning!("Parse error"; "error" => %e);
                            parse_errors += 1;
                            continue;
                        }
                        _ => {
                            log_critical!("Stream error"; "error" => %e);
                        }
                    },
                };

                match work_tx.send(Some(cell)) {
                    Ok(_) => cells_parsed += 1,
                    Err(e) => {
                        log_critical!("Channel send failed"; "error" => %e);
                    }
                }

                if cells_parsed % 100 == 0 {
                    log_info!("Processing progress"; "cells parsed" => cells_parsed, "parse errors" => parse_errors);
                }
            }

            for worker_id in 0..n_workers {
                if let Err(e) = work_tx.send(None) {
                    log_warning!("Failed to send stop signal to worker"; "worker id" => worker_id, "error" => %e);
                }
            }

            worker_threadpool.join();

            match bufwriter.try_lock() {
                Ok(mut writer) => {
                    if let Err(e) = writer.flush() {
                        log_warning!("Failed to flush output buffer"; "error" => %e);
                    }
                }
                Err(e) => {
                    log_warning!("Could not acquire lock to flush buffer"; "error" => %e);
                }
            }

            processed_files += 1;
            total_cells_processed += cells_parsed;
            total_errors += parse_errors;

            log_info!("Completed file"; "output path" => ?output_path, "cells processed" => cells_parsed, "parse errors" => parse_errors);
        }

        log_info!("Processing complete"; "files processed" => processed_files, "total files" => self.path_in.len(), "total cells processed" => total_cells_processed, "total errors" => total_errors);

        if total_errors > 0 {
            log_warning!("Execution completed with errors"; "total errors" => total_errors);
        }

        Ok(())
    }
}

struct StreamToken {
    cell: &'static [u8],
    reads: Vec<&'static [u8]>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetStreamToken for StreamToken {
    type Builder = StreamTokenBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}

struct StreamTokenBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl StreamTokenBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            underlying: Vec::new(),
        }
    }
}

impl BascetStreamTokenBuilder for StreamTokenBuilder {
    type Token = StreamToken;

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
        self.reads.push(static_slice);
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
    fn add_seq_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push(static_slice);
        self
    }

    #[inline(always)]
    fn add_underlying(mut self, buffer: Arc<Vec<u8>>) -> Self {
        self.underlying.push(buffer);
        self
    }

    #[inline(always)]
    fn build(self) -> StreamToken {
        StreamToken {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            _underlying: self.underlying,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for AutoStream<T>
where
    T: BascetStreamToken,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
