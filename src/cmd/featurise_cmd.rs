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


use crate::command::Featurise;
use crate::command::FeaturiseParams;

use crate::fileformat::read_cell_list_file;




#[derive(Args)]
pub struct FeaturiseCMD {


    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,


    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,

    //Thread settings
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WRITE)]
    threads_write: usize,
    
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,

    
}
impl FeaturiseCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        
        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };
        

        let params = FeaturiseParams {

            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),  
            include_cells: include_cells.clone(),          
            threads_work: self.threads_work,
        };

        let _ = Featurise::run(
            &Arc::new(params)
        );

        println!("Featurise has finished succesfully");
        Ok(())
    }




}





