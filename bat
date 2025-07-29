use crate::{
    command::determine_thread_counts_2,
    io::{traits::*, AutoBascetFile},
    support_which_stream,
};
use clap::Args;
use enum_dispatch::enum_dispatch;
use std::{path::PathBuf, sync::Arc};

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
    /// Run the commandline option
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        println!("Running Countsketch");
        let (threads_read, threads_write) =
            determine_thread_counts_2(self.threads_total, self.threads_read, self.threads_work)?;

        for input in &self.path_in {
            let file = AutoBascetFile::try_from_path(input).unwrap();
            let stream: AutoStream<StreamToken> =
                AutoStream::try_from_file(file).unwrap();
        }

        Ok(())
    }
}

struct StreamToken<'this> {
    cell: &'this [u8],
    reads: Vec<&'this [u8]>,

    underlying: Arc<Vec<u8>>,
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
