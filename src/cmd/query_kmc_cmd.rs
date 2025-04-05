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


use crate::command::QueryKmc;
use crate::command::QueryKmcParams;

//use crate::fileformat::read_cell_list_file;



#[derive(Args)]
pub struct QueryKmcCMD {


    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    // Input feature file (text file, one kmer per line)
    #[arg(short = 'f', value_parser = clap::value_parser!(PathBuf))]  
    pub path_features: PathBuf,


        
    //Thread settings
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,

    
}
impl QueryKmcCMD {
    pub fn try_execute(&mut self) -> Result<()> {



        let params = QueryKmcParams {
            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),   
            path_features: self.path_features.clone(), 
         
            threads_work: self.threads_work,
        };

        let _ = QueryKmc::run(
            &Arc::new(params)
        );

        log::info!("Query has finished succesfully");
        Ok(())
    }




}





