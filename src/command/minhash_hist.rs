use std::sync::Arc;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::io::BufRead;
use std::collections::HashMap;
use std::collections::HashSet;
use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

use crate::fileformat::CellID;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::fileformat::shard::ShardCellDictionary;
use crate::fileformat::read_cell_list_file;



pub const DEFAULT_PATH_TEMP: &str = "temp";



/// Commandline option: Generate a histogram of minhashes
#[derive(Args)]
pub struct MinhashHistCMD {
    // Input bascet or gascet
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
impl MinhashHistCMD {

    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        
        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };
        

        let params = MinhashHist {
            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),  
            include_cells: include_cells.clone(),          
        };

        let _ = MinhashHist::run(
            &Arc::new(params)
        );

        log::info!("MinhashHist has finished succesfully");
        Ok(())
    }
}







/// Algorithm: Generate a histogram of minhashes
pub struct MinhashHist {
    pub path_input: Vec<std::path::PathBuf>,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
    pub include_cells: Option<Vec<CellID>>,
}
impl MinhashHist {

    /// Run the algorithm
    pub fn run(
        params: &Arc<MinhashHist>
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

        //Allocate a big array for all kmers; some guess at capacity
        let mut all_kmer: Vec<String> = Vec::with_capacity(list_cells.len()*500);

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

            // Unzip all cell-specific min-hash specific databases
            for cell_id in cells_for_file {

                if cur_file_id%1000 == 0 {
                    println!("Processing file {}, cell {}", path_input.display(), cur_file_id);
                }

                //Need to check if cell is present, as if multiple input files, the cell might not be in this particular file
                if file_input.has_cell(&cell_id) {
                    //Check if a minhash is present for this cell, otherwise exclude it.
                    //If processing multiple input files, there is a good chance the cell will not be there.
                    //Support streaming of sorts? subset to cells in this file?
                    let list_files = file_input.get_files_for_cell(&cell_id).expect("Could not get list of files for cell"); //////////// TODO may fail if we scan all files in each input file
                    let f1="minhash.txt".to_string();
                    if list_files.contains(&f1) {

                        let path_f1 = params.path_tmp.join(format!("cell_{}.minhash.txt", cur_file_id).to_string());
                        file_input.extract_as(&cell_id, &f1, &path_f1).unwrap();

                        //Get the content and add to list
                        let file = File::open(&path_f1)?;
                        let lines = std::io::BufReader::new(file).lines();
                        for line in lines {
                            let line = line.unwrap();
                            
                            //Only get the kmer; there can be more columns as well
                            let mut splitter = line.split("\t");
                            let kmer_string = splitter.next().expect("Could not parse KMER sequence from minhash.txt in Bascet");

                            all_kmer.push(kmer_string.to_string());
                        }

                        //Delete file when done with it
                        std::fs::remove_file(&path_f1)?;
                    } 
                    cur_file_id = cur_file_id + 1;
                }
            }
        }
        println!("Obtained minhashes from {} cells", cur_file_id);

        //Sort the KMERs; we assume that they are few enough that we can do it in memory for speed
        println!("Counting minhashes");
        let hist = make_histogram(all_kmer);

        //Write out histogram
        println!("Storing histogram");
        let f=File::create(&params.path_output).expect("Could not open KMER histogram file for writing");
        let mut bw=BufWriter::new(f);
        for (kmer_string,cnt) in hist {
            writeln!(bw, "{}\t{}", &kmer_string, cnt).unwrap();    
        }

        //Delete temp folder
        fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }


    
}



/// From an iterator, generate a histogram of element occurences
fn make_histogram<I>(it: I) -> HashMap<I::Item, usize>
where
    I: IntoIterator,
    I::Item: Eq + core::hash::Hash,
{
    let mut result = HashMap::new();

    for item in it {
        *result.entry(item).or_insert(0) += 1;
    }

    result
}

