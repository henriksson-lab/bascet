use crate::{
    common,
    io::{traits::*, AutoBascetFile},
    kmer::kmc_counter::CountSketch,
    support_which_stream,
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
    for formats [tirp]
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
        for input in &self.path_in {
            let file = AutoBascetFile::try_from_path(input).unwrap();
            let mut stream: AutoStream<StreamToken> = AutoStream::try_from_file(file)
                .unwrap()
                .set_reader_threads(8);

            let num_threads = rayon::current_num_threads();
            let countsketch: Vec<Arc<Mutex<CountSketch>>> = (0..num_threads)
                .map(|_| Arc::new(Mutex::new(CountSketch::new(100))))
                .collect();

            let path = self
                .path_out
                .as_path()
                .join(input.with_extension("countsketch.txt").file_name().unwrap());

            let file = File::create(path).unwrap();
            let buf_writer = BufWriter::new(file);
            let buf_writer = Arc::new(Mutex::new(buf_writer));

            while let Ok(Some(token)) = stream.next_cell() {
                let countsketch = countsketch.clone();
                let buf_writer = buf_writer.clone();

                rayon::spawn(move || {
                    let thread_idx = rayon::current_thread_index().unwrap();
                    let mut countsketch = countsketch[thread_idx].lock().unwrap();
                    countsketch.reset();

                    let mut total = 0;
                    for read in token.reads.iter() {
                        let kmers = read.windows(31);
                        total += kmers.len();
                        for kmer in kmers {
                            countsketch.add(kmer);
                        }
                    }

                    if let Ok(mut buf_writer) = buf_writer.try_lock() {
                        let _ = buf_writer.write_all(token.cell);
                        let _ = buf_writer.write_all(&[common::U8_CHAR_TAB]);
                        let _ = buf_writer.write_all(total.to_string().as_bytes());

                        for value in countsketch.sketch.iter() {
                            let _ = buf_writer.write_all(&[common::U8_CHAR_TAB]);
                            let _ = buf_writer.write_all(value.to_string().as_bytes());
                        }
                        let _ = buf_writer.write_all(&[common::U8_CHAR_NEWLINE]);
                    }
                });
            }
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

    fn cell_id(mut self, id: Vec<u8>) -> Self {
        todo!()
        // NOTE: Should work something like this?
        // let aid = Arc::new(id);
        // self = self.add_underlying(aid);
        // self.cell = Some(&(aid.clone()));
        // self
    }

    fn add_cell_slice(mut self, slice: &[u8]) -> Self {
        // Convert to 'static lifetime using the same approach as build()
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(static_slice);
        self
    }

    fn add_read_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push(static_slice);
        self
    }
    fn add_underlying(mut self, buffer: Arc<Vec<u8>>) -> Self {
        self.underlying.push(buffer);
        self
    }

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
    type Item = anyhow::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
