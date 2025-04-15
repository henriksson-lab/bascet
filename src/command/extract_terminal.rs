use anyhow::Result;
use clap::Args;
use std::fs::File;
use std::{
    io::{BufReader, BufWriter},
    path::PathBuf,
};
use zip::ZipArchive;
use log::debug;


pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct ExtractCMD {
    #[arg(short = 'i', value_parser)]  /// Zip-file name. Note that this command takes a shard, not a full bascet (can support later!) -- this is for speed
    pub path_in: PathBuf,

}
impl ExtractCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        //This is an interactive terminal to navigate Bascet-ZIP content




        Ok(())
    }
}
