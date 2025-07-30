use anyhow::Result;
use clap::Args;
use log::{debug, info};
use std::collections::HashSet;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use std::io::{BufWriter, Write};

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use crate::fileformat::read_cell_list_file;
use crate::fileformat::shard;
use crate::fileformat::tirp;
use crate::fileformat::CellID;
use crate::fileformat::ReadPair;
use crate::fileformat::ReadPairReader;
use crate::fileformat::ShardCellDictionary;
use crate::fileformat::TirpBascetShardReader;

type ListReadPair = Arc<Vec<ReadPair>>;
type MergedListReadWithBarcode = Arc<(CellID, Vec<ListReadPair>)>;

pub const DEFAULT_PATH_TEMP: &str = "temp";

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
}
impl ShardifyCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        //test
        Ok(())
    }
}