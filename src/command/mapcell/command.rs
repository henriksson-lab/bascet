use std::path::PathBuf;
use anyhow::Result;
use clap::Args;

use super::constants;
use super::core::params;
use super::core::core::MapCell;

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: Option<PathBuf>,
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = constants::MAPCELL_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: Option<PathBuf>,


    //The script to run
    #[arg(short = 's', value_parser = clap::value_parser!(PathBuf))]
    pub path_script: Option<PathBuf>,

    #[arg(long = "show-presets")]
    pub show_presets: bool,

    #[arg(long = "keep-files")]
    pub keep_files: bool,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_WRITE)]
    threads_write: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = constants::MAPCELL_DEFAULT_THREADS_WORK)]
    threads_work: usize,
}



impl Command {
    pub fn try_execute(&mut self) -> Result<()> {


        if self.show_presets {
            let names = super::core::core::get_preset_script_names();
            println!("Available preset scripts: {:?}", names);
            return Ok(());
        }

        let params_io = params::IO {
            path_in: self.path_in.as_ref().expect("Input file was not provided").clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.as_ref().expect("Output file was not provided").clone(),
            path_script: self.path_script.as_ref().expect("Script file was not provided").clone(),

            threads_read: self.threads_read,
            threads_write: self.threads_write,
            threads_work: self.threads_work,

            keep_files: self.keep_files            
        };

        let _ = MapCell::run(params_io).expect("mapcell failed");

        println!("Mapcell has finished!");
        Ok(())
    }
}

