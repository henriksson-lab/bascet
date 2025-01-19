use anyhow::Result;
//use anyhow::bail;

use clap::Args;
use std::{
    path::PathBuf,
    sync::Arc,
};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;


use crate::command::Kraken;
use crate::command::KrakenParams;

//use crate::fileformat::read_cell_list_file;



#[derive(Args)]
pub struct KrakenCMD {


    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    
}
impl KrakenCMD {
    pub fn try_execute(&mut self) -> Result<()> {



        let params = KrakenParams {
            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),   
        };

        let _ = Kraken::run(
            &Arc::new(params)
        );

        log::info!("Kraken has finished succesfully");
        Ok(())
    }




}





