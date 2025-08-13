use crate::{
    command::determine_thread_counts_2, io::traits::*, kmer::kmc_counter::CountSketch,
    log_critical, log_info, log_warning, support_which_stream, support_which_temp,
    support_which_writer,
};

use clap::Args;
use itertools::enumerate;
use std::{
    fs::File,
    io::BufWriter,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub const DEFAULT_THREADS_READ: usize = 8;
pub const DEFAULT_THREADS_WORK: usize = 4;
pub const DEFAULT_THREADS_TOTAL: usize = 12;
pub const DEFAULT_COUNTSKETCH_SIZE: usize = 128;
pub const DEFAULT_KMER_SIZE: usize = 31;
pub const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 128;

support_which_stream! {
    CountsketchInput => CountsketchStream<T: BascetCell>
    for formats [tirp, zip]
}
support_which_writer! {
    CountsketchOutput => CountsketchWriter<W: std::io::Write>
    for formats [csv]
}
#[derive(Args)]
pub struct CountsketchCMD {
    // Input bascets
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,
    // Output path
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_TOTAL)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
    // Countsketch parameters
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
        log_info!("Starting Countsketch";
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

        let mut processed_files = 0;
        let mut total_cells_processed = 0;
        let mut total_errors = 0;

        for (i, input) in enumerate(&self.path_in) {
            log_info!("Processing input file"; "path" => ?input);

            let file = match CountsketchInput::try_from_path(input) {
                Ok(file) => file,
                Err(e) => {
                    log_warning!("Failed to open input file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let stream: CountsketchStream<CountsketchCell> = match CountsketchStream::try_from_input(
                file,
            ) {
                Ok(stream) => stream,
                Err(e) => {
                    log_warning!("Failed to create stream from file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            }.set_reader_threads(n_readers);

            let output_path = self.path_out.join(format!("countsketch.{i}.csv"));
            let output_auto = match CountsketchOutput::try_from_path(&output_path) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to identify output file, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };
            let output_file = match File::create(output_auto.path()) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to create output file, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let writer =  match CountsketchWriter::try_from_output(output_auto) {
                Ok(writer) => writer,
                Err(e) => {
                    log_warning!("Failed to create output writer, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            }.set_writer(BufWriter::new(output_file));
            let writer = Arc::new(Mutex::new(writer));

            let worker_threadpool = threadpool::ThreadPool::new(n_workers);
            let (work_tx, work_rx) =
                crossbeam::channel::bounded::<Option<CountsketchCell>>(self.channel_buffer_size);

            for worker_id in 0..n_workers {
                let work_rx = work_rx.clone();

                let kmer_size = self.kmer_size;
                let mut countsketch = CountSketch::new(self.countsketch_size);

                let writer = Arc::clone(&writer);
                worker_threadpool.execute(move || {
                    let mut cells_processed = 0;

                    while let Ok(Some(cell)) = work_rx.recv() {
                        countsketch.reset();
                        let Some(reads) = cell.get_reads() else { continue };

                        for (r1, r2) in reads {
                            for kmer in r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                            for kmer in r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                            let mut rev_r1 = Vec::with_capacity(r1.len());
                            for &base in r1.iter().rev() {
                                rev_r1.push(match base {
                                    b'A' => b'T', b'T' => b'A',
                                    b'G' => b'C', b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }

                            let mut rev_r2 = Vec::with_capacity(r2.len());
                            for &base in r2.iter().rev() {
                                rev_r2.push(match base {
                                    b'A' => b'T', b'T' => b'A',
                                    b'G' => b'C', b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                        }

                        // NOTE: lock free aproaches for writing did not perform much better
                        match writer.lock() {
                            Ok(mut writer) => {
                                if let Err(e) = writer.write_countsketch(&cell, &countsketch) {
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

            processed_files += 1;
            total_cells_processed += cells_parsed;
            total_errors += parse_errors;

            log_info!("Completed file"; "output path" => ?output_path, "cells processed" => cells_parsed, "parse errors" => parse_errors);
        }

        log_info!("Countsketch complete"; "files processed" => processed_files, "files given as input" => self.path_in.len(), "total cells processed" => total_cells_processed, "total errors" => total_errors);

        if total_errors > 0 {
            log_warning!("Execution completed with errors");
        }

        Ok(())
    }
}

struct CountsketchCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetCell for CountsketchCell {
    type Builder = CountsketchCellBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }
}

struct CountsketchCellBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl CountsketchCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            underlying: Vec::new(),
        }
    }
}

impl BascetCellBuilder for CountsketchCellBuilder {
    type Token = CountsketchCell;

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
    fn add_sequence_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push((static_slice, &[]));
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
    fn add_underlying(mut self, buffer: Arc<Vec<u8>>) -> Self {
        self.underlying.push(buffer);
        self
    }

    #[inline(always)]
    fn build(self) -> CountsketchCell {
        CountsketchCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            _underlying: self.underlying,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for CountsketchStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
