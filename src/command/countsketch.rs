use crate::{
    common, io::{traits::*, AutoBascetFile}, kmer::kmc_counter::CountSketch, log_critical, log_info, log_warning, support_which_stream
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

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WORK: usize = 11;

#[derive(Args)]
pub struct CountsketchCMD {
    // Input bascets
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,
    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,
    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value = None)]
    threads_total: Option<usize>,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value = None)]
    threads_read: Option<usize>,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value = None)]
    threads_work: Option<usize>,
}

impl CountsketchCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let n_readers = self.threads_read.unwrap();
        let n_workers = self.threads_work.unwrap();

        for input in &self.path_in {
            log_info!("Processing"; "input" => ?input);
            let file = AutoBascetFile::try_from_path(input).unwrap();
            let stream: AutoStream<StreamToken> = AutoStream::try_from_file(file)
                .unwrap()
                .set_reader_threads(n_readers);

            let path = self
                .path_out
                .as_path()
                .join(input.with_extension("countsketch.txt").file_name().unwrap());

            let file = File::create(path).unwrap();
            let bufwriter = BufWriter::new(file);
            let bufwriter = Arc::new(Mutex::new(bufwriter));

            let worker_threadpool = threadpool::ThreadPool::new(n_workers);
            let (wtx, wrx) = crossbeam::channel::bounded::<Option<StreamToken>>(128);

            for _ in 0..n_workers {
                let rx = wrx.clone();
                let bufwriter = bufwriter.clone();
                let mut countsketch = CountSketch::new(128);
                let mut buffer: Vec<u8> = Vec::new();

                worker_threadpool.execute(move || {
                    while let Ok(Some(token)) = rx.recv() {
                        countsketch.reset();

                        // let mut total = 0;
                        for read in token.reads.iter() {
                            let kmers = read.windows(31);
                            // total += kmers.len();
                            for kmer in kmers {
                                countsketch.add(kmer);
                            }
                            let rread: Vec<u8> = read
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

                            let kmers = rread.windows(31);
                            // total += kmers.len();
                            for kmer in kmers {
                                countsketch.add(kmer);
                            }
                        }

                        buffer.clear();
                        buffer.extend_from_slice(token.cell);
                        buffer.push(common::U8_CHAR_TAB);
                        buffer.extend_from_slice(token.reads.len().to_string().as_bytes());

                        for value in countsketch.sketch.iter() {
                            buffer.push(common::U8_CHAR_TAB);
                            buffer.extend_from_slice(value.to_string().as_bytes());
                        }
                        buffer.push(common::U8_CHAR_NEWLINE);

                        // NOTE: lock free aproaches for writing did not perform much better
                        if let Ok(mut bufwriter) = bufwriter.try_lock() {
                            let _ = bufwriter.write_all(&buffer);
                        }
                    }
                });
            }

            // Feed tokens to workers
            for token_res in stream {
                let token = match token_res {
                    Ok(token) => token,
                    Err(e) => match e {
                        crate::runtime::Error::ParseError { .. } => {
                            log_warning!("{e}");

                            continue;
                        },
                        _ => unreachable!()
                    },
                };
                let _ = wtx.send(Some(token));
            }

            // Signal workers to stop
            for _ in 0..n_workers {
                let _ = wtx.send(None);
            }

            worker_threadpool.join();
        }
        Ok(())
    }
}

struct StreamToken {
    cell: &'static [u8],
    reads: Vec<&'static [u8]>,

    underlying: Vec<Arc<Vec<u8>>>,
}
impl<'slice> BascetStreamToken for StreamToken {
    type Builder = StreamTokenBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}

struct StreamTokenBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>, // Keep buffer alive
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
            underlying: self.underlying,
        }
    }
}

impl<T> Iterator for AutoStream<T>
where
    T: BascetStreamToken,
{
    type Item = Result<T, crate::runtime::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
