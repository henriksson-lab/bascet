use anyhow::Result;
use anyhow::bail;

use clap::Args;
use std::{
    fs::File,
    path::PathBuf,
    sync::Arc,
    thread,
};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;


use crate::command::BuildKMERdatabase;
use crate::command::BuildKMERdatabaseParams;




#[derive(Args)]
pub struct BuildKMERdatabaseCMD {


    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,



    //Thread settings
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WRITE)]
    threads_write: usize,
    
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,

    
}
impl BuildKMERdatabaseCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        

        let params_io = BuildKMERdatabaseParams {

            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),            
            threads_work: self.threads_work,
        };

        let _ = BuildKMERdatabase::run(
            &Arc::new(params_io)
        );

        Ok(())
    }




}





