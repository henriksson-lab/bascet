use anyhow::Result;
use clap::Args;
use std::sync::Arc;
use std::path::PathBuf;

use crate::command::transform::TransformFile;
use crate::command::transform::TransformFileParams;
use crate::fileformat::read_cell_list_file;


#[derive(Args)]
pub struct TransformCmd {
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,
    
}
impl TransformCmd {
    pub fn try_execute(&mut self) -> Result<()> {

        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };

        //Set up parameters and run the function
        let params = TransformFileParams {
            path_in: self.path_in.clone(),
            //path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),

            include_cells: include_cells
        };
        
        let _ = TransformFile::run(&Arc::new(params)).expect("tofastq failed");

        println!("ToFastq has finished!");
        Ok(())
    }
}




