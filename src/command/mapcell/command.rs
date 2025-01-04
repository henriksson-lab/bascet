use std::path::PathBuf;
use anyhow::Result;
use clap::Args;

use super::constants;
use super::core::params;
use super::core::core::MapCell;

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = constants::MAPCELL_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,


    //The script to run
    #[arg(short = 's', value_parser = clap::value_parser!(PathBuf))]
    pub path_script: PathBuf,

    // built-in software: can read into binary and write script to run at need

    // help feature to show built-in software

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_WRITE)]
    threads_write: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_WORK)]
    threads_work: usize,
}



impl Command {
    pub fn try_execute(&mut self) -> Result<()> {

        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
            path_script: self.path_script.clone(),

            threads_read: self.threads_read,
            threads_write: self.threads_write,
            threads_work: self.threads_work,
        };

        let _ = MapCell::run(params_io);

        println!("Mapcell has finished!");
        Ok(())
    }
}
