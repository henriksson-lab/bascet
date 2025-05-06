use std::sync::Arc;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::io::BufRead;
use std::collections::HashSet;
use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

use crate::fileformat::CellID;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::fileformat::shard::ShardCellDictionary;
use crate::fileformat::read_cell_list_file;



pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;






#[derive(Args)]
pub struct CountsketchCMD {
    // Input bascets
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]  //
    pub path_in: Vec<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,
    
}
impl CountsketchCMD {

    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        
        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };
        

        let params = CountsketchMat {
            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),  
            include_cells: include_cells.clone(),          
        };

        let _ = CountsketchMat::run(
            &Arc::new(params)
        );

        log::info!("countsketch_mat has finished succesfully");
        Ok(())
    }
}








pub struct CountsketchMat {
    pub path_input: Vec<std::path::PathBuf>,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub include_cells: Option<Vec<CellID>>,
}
impl CountsketchMat {

    /// Run the algorithm
    pub fn run(
        params: &Arc<CountsketchMat>
    ) -> anyhow::Result<()> {

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            println!("Using tempdir {}", params.path_tmp.display());
            if fs::create_dir_all(&params.path_tmp).is_err() {
                panic!("Failed to create temporary directory");
            };  
        }


        //Detect which cells to gather
        let list_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {
            let mut list_cells: Vec<String> = Vec::new();
            for path_input in &params.path_input {
                let mut file_input = ZipBascetShardReader::new(&path_input).expect("Failed to open input file");
                let mut cells_for_file= file_input.get_cell_ids().expect("Failed to get content listing for input file");
                list_cells.append(&mut cells_for_file);
            }
            list_cells            
        };
        println!("Preparing to process {} cells", list_cells.len());

        //Open file for output
        let f=File::create(&params.path_output).expect("Could not open CS matrix file for writing");
        let mut bw=BufWriter::new(f);


        //Keep cells as a hash for quick lookup
        let mut hash_list_cells:HashSet<String> = HashSet::new();
        for cellid in list_cells {
            hash_list_cells.insert(cellid.clone());
        }

        //Process each input file
        let mut cur_file_id = 0;
        for path_input in &params.path_input {
            let mut file_input = ZipBascetShardReader::new(&path_input).
                expect("Failed to open input file");

            //In this particular file, which cells to extract?
            let cells_for_file= file_input.get_cell_ids().
                expect("Failed to get content listing for input file");
            let cells_for_file = cells_for_file.iter().
                filter(|&s| hash_list_cells.contains(s)).
                collect::<Vec<&String>>();

            // Get reads for each cell
            for cell_id in cells_for_file {

                if cur_file_id%1000 == 0 {
                    println!("Processing file {}, cell {}", path_input.display(), cur_file_id);
                }

                //Need to check if cell is present, as if multiple input files, the cell might not be in this particular file
                if file_input.has_cell(&cell_id) {
                    //Check if a minhash is present for this cell, otherwise exclude it.
                    //If processing multiple input files, there is a good chance the cell will not be there.
                    //Support streaming of sorts? subset to cells in this file?
                    file_input.set_current_cell(&cell_id);
                    let list_files = file_input.get_files_for_cell().expect("Could not get list of files for cell"); //////////// TODO may fail if we scan all files in each input file
                    let f1="countsketch.txt".to_string();
                    if list_files.contains(&f1) {

                        let path_f1 = params.path_tmp.join(format!("cell_{}.countsketch.txt", cur_file_id).to_string());
                        file_input.extract_as(&f1, &path_f1).unwrap();

                        //Print which cell we are in
                        write!(bw, "{}", cell_id).unwrap();

                        //Get the content and add to list
                        let file = File::open(&path_f1)?;
                        let lines = std::io::BufReader::new(file).lines();
                        for line in lines {
                            let line = line.unwrap();
                            write!(bw, "\t{}", line).unwrap();
                        }
                        writeln!(bw, "").unwrap();

                        //Delete file when done with it
                        std::fs::remove_file(&path_f1)?;
                    } 
                    cur_file_id = cur_file_id + 1;
                }
            }
        }
        println!("Obtained CS from {} cells", cur_file_id);

        //Delete temp folder
        fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }


    
}
