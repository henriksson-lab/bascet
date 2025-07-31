use anyhow::Result;
use clap::Args;
use std::{io::Write, path::PathBuf, sync::Arc};

use crate::{
    io::{traits::*, AutoBascetFile},
    log_critical, log_info, log_warning, support_which_stream, support_which_writer,
    utils::expand_and_resolve,
};

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

        let mut processed_files = 0;
        let mut total_cells_processed = 0;
        let mut total_errors = 0;

        for input in &self.path_in {
            log_info!("Processing input file"; "path" => ?input);

            let file = match AutoBascetFile::try_from_path(input) {
                Ok(file) => file,
                Err(e) => {
                    log_warning!("Failed to open input file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let mut stream: ShardifyStream<ShardifyToken> = match ShardifyStream::try_from_file(
                file,
            ) {
                Ok(stream) => stream,
                Err(e) => {
                    log_warning!("Failed to create stream from file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            for _ in stream {}
        }

        Ok(())
    }
}

#[derive(Debug)]
struct ShardifyToken {
    cell: &'static [u8],
    reads: Vec<&'static [u8]>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetStreamToken for ShardifyToken {
    type Builder = ShardifyTokenBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}

struct ShardifyTokenBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl ShardifyTokenBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
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
    fn build(self) -> ShardifyToken {
        ShardifyToken {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
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
