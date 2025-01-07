use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use clap::Args;

use crate::command::shardify::ShardifyParams;
use crate::command::shardify::Shardify;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;


#[derive(Args)]
pub struct ShardifyCMD {
    // Input bascets (comma separated; ok with PathBuf???)
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,


    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,

}



impl ShardifyCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        let include_cells: Option<Vec<String>> = Some(Vec::new());

        let params = ShardifyParams {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),

            include_cells: include_cells
        };

        let _ = Shardify::run(Arc::new(params)).expect("shardify failed");

        println!("Mapcell has finished!");
        Ok(())
    }
}

