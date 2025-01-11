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


use crate::command::Query;
use crate::command::QueryParams;



//pub static DEFAULT_SEED_RANDOM: std::sync::LazyLock<u64> =
//    std::sync::LazyLock::new(|| rand::random::<u64>());


#[derive(Args)]
pub struct QueryCMD {


    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    #[arg(long, value_parser)]  
    pub path_features: PathBuf,

    /* 
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_MIN)]
    pub features_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_MAX)]
    pub features_nmax: usize,

    #[arg(long, value_parser = clap::value_parser!(u64), default_value_t = *DEFAULT_SEED_RANDOM)]
    pub seed: u64,
*/

    //Thread settings
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,

    
}
impl QueryCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        

        let params = QueryParams {

            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),   
            path_features: self.path_features.clone(), 
         
            threads_work: self.threads_work,
        };

        let _ = Query::run(
            &Arc::new(params)
        );

        println!("Query has finished succesfully");
        Ok(())
    }




}





