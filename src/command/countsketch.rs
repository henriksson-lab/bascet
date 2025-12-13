use crate::{
    command::determine_thread_counts_2, common::PageBuffer, io::traits::*,
    kmer::kmc_counter::CountSketch, log_critical, log_info, log_warning, support_which_stream,
    support_which_writer, threading,
};

use bascet_core::*;
use bascet_io::{decode, parse, tirp};

use bounded_integer::{BoundedU64, BoundedUsize};
use bytesize::*;
use clap::Args;
use itertools::enumerate;
use rust_htslib::htslib;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

pub const DEFAULT_THREADS_READ: usize = 8;
pub const DEFAULT_THREADS_WORK: usize = 4;
pub const DEFAULT_THREADS_TOTAL: usize = 12;
pub const DEFAULT_COUNTSKETCH_SIZE: usize = 4096;
pub const DEFAULT_KMER_SIZE: usize = 31;

support_which_writer! {
    CountsketchOutput => CountsketchWriter<W: std::io::Write>
    for formats [tsv]
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
    // Stream buffer configuration
    #[arg(long = "buffer-size", value_parser = clap::value_parser!(u64), default_value_t = 1024)]
    pub buffer_size_mib: u64,
    #[arg(long = "page-size", value_parser = clap::value_parser!(u64), default_value_t = 8)]
    pub page_size_mib: u64,
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

            let decoder = decode::Bgzf::builder()
                .path(input)
                .num_threads(BoundedU64::new(n_readers as u64).unwrap())
                .build()
                .unwrap();
            let parser = parse::Tirp::builder().build().unwrap();

            let mut stream = Stream::builder()
                .with_decoder(decoder)
                .with_parser(parser)
                .countof_buffers(BoundedUsize::const_new::<1024>())
                .sizeof_arena(ByteSize::mib(self.page_size_mib))
                .sizeof_buffer(ByteSize::mib(self.buffer_size_mib))
                .build()
                .unwrap();

            let mut query = stream
                .query::<tirp::Cell>()
                .group_relaxed_with_context::<Id, Id, _>(|id: &&'static [u8], id_ctx: &&'static [u8]| {
                    match id.cmp(id_ctx) {
                        std::cmp::Ordering::Less => panic!("Unordered record list"),
                        std::cmp::Ordering::Equal => QueryResult::Keep,
                        std::cmp::Ordering::Greater => QueryResult::Emit,
                    }
                });

            let output_path = self.path_out.join(format!("countsketch.{i}.tsv"));
            let output_auto = match CountsketchOutput::try_from_path(&output_path) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to verify output file, skipping"; "path" => ?output_path, "error" => %e);
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

            let mut output_countsketch_writer = match CountsketchWriter::try_from_output(&output_auto) {
                Ok(writer) => writer,
                Err(e) => {
                    log_warning!("Failed to create output writer, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            }.set_writer(BufWriter::new(output_file));

            let worker_threadpool = threadpool::ThreadPool::new(n_workers);
            let (work_tx, work_rx) = crossbeam::channel::unbounded::<Option<tirp::Cell>>();
            let (write_tx, write_rx) =
                crossbeam::channel::unbounded::<Option<(tirp::Cell, CountSketch)>>();

            let _ = std::thread::spawn(move || {
                while let Ok(Some((cell, countsketch))) = write_rx.recv() {
                    if let Err(e) = output_countsketch_writer.write_comp_countsketch(&cell, &countsketch)
                    {
                        log_warning!("Failed to write countsketch"; "cell" => %String::from_utf8_lossy(cell.get_bytes::<Id>()), "error" => %e);
                    }
                }
                let _ = output_countsketch_writer.get_writer().unwrap().flush();
            });

            for _ in 0..n_workers {
                let work_rx = work_rx.clone();
                let write_tx = write_tx.clone();

                let kmer_size = self.kmer_size;
                let mut countsketch = CountSketch::new(self.countsketch_size);

                worker_threadpool.execute(move || {
                    while let Ok(Some(cell)) = work_rx.recv() {
                        let reads = cell.get_ref::<SequencePair>();
                        if reads.len() == 0 {
                            continue;
                        }

                        let mut rev_r1 = Vec::new();
                        let mut rev_r2 = Vec::new();

                        for (r1, r2) in reads {
                            for kmer in r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                            for kmer in r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }

                            rev_r1.clear();
                            rev_r1.reserve(r1.len());
                            for &base in r1.iter().rev() {
                                rev_r1.push(match base {
                                    b'A' => b'T',
                                    b'T' => b'A',
                                    b'G' => b'C',
                                    b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }

                            rev_r2.clear();
                            rev_r2.reserve(r2.len());
                            for &base in r2.iter().rev() {
                                rev_r2.push(match base {
                                    b'A' => b'T',
                                    b'T' => b'A',
                                    b'G' => b'C',
                                    b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                        }

                        let _ = write_tx.send(Some((cell, countsketch.clone())));
                        countsketch.reset();
                    }
                });
            }

            let (cells_parsed, parse_errors) = {
                let mut cells_parsed = 0;
                let mut parse_errors = 0;

                loop {
                    match query.next() {
                        Ok(Some(cell)) => {
                            match work_tx.send(Some(cell)) {
                                Ok(_) => cells_parsed += 1,
                                Err(e) => {
                                    log_critical!("Channel send failed"; "error" => %e);
                                }
                            }

                            if cells_parsed % 100 == 0 {
                                log_info!("Processing"; "cells parsed" => cells_parsed, "parse errors" => parse_errors);
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(_) => {
                            log_warning!("Parse error");
                            parse_errors += 1;
                        }
                    }
                }

                (cells_parsed, parse_errors)
            };

            for worker_id in 0..n_workers {
                if let Err(e) = work_tx.send(None) {
                    log_warning!("Failed to send stop signal to worker"; "worker id" => worker_id, "error" => %e);
                }
            }
            let _ = write_tx.send(None);

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
