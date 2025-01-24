use anyhow::Result;
use clap::Args;
use std::path::PathBuf;
use crate::command::countchrom::CountChrom;
use crate::command::countchrom::CountGenomeParams;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS: usize = 5;


#[derive(Args)]
pub struct CountChromCMD {
    #[arg(short = 'i', value_parser)]  /// BAM or CRAM file; sorted, indexed? unless cell_id's given, no need for sorting
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]  /// Full path to file to store in
    pub path_out: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)] //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    // Number of threads

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS)]
    num_threads: usize,
    
    
}

impl CountChromCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        //TODO Can check that input file is sorted via header

        CountChrom::run(&CountGenomeParams {
            path_in: self.path_in.clone(),
         //   path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
            num_threads: self.num_threads
        }).unwrap();

        log::info!("CountChrom has finished succesfully");
        Ok(())
    }
}
