use anyhow::Result;
use clap::Args;
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::command::determine_thread_counts_1;
use crate::fileformat::iterate_shard_reader;
use crate::fileformat::read_cell_list_file;
use crate::fileformat::CellID;
use crate::fileformat::ShardFileExtractor;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;

#[derive(Args)]
pub struct CountsketchMatCMD {
    // Input bascets
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser = clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl CountsketchMatCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        println!("Running Countsketch MAT");
        let num_threads_total = determine_thread_counts_1(self.num_threads_total)?;
        println!("Using threads {}", num_threads_total);

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

            num_threads_total: num_threads_total,
        };

        let _ = CountsketchMat::run(&Arc::new(params));

        log::info!("countsketch_mat has finished succesfully");
        Ok(())
    }
}

pub struct CountsketchMat {
    pub path_input: Vec<std::path::PathBuf>,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub include_cells: Option<Vec<CellID>>,

    num_threads_total: usize,
}
impl CountsketchMat {
    /// Run the algorithm
    pub fn run(params: &Arc<CountsketchMat>) -> anyhow::Result<()> {
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

        //Open file for output
        let f =
            File::create(&params.path_output).expect("Could not open CS matrix file for writing");
        let bw = BufWriter::new(f);

        let clone_params = Arc::clone(params);

        let bw = Mutex::new(bw);
        let bw = Arc::new(bw);
        //let mut cell_count = 0;
        //        let cell_count = Mutex::new(0);

        //Function to apply to each cell that is being read.
        //In this case, concatenate the content of each countsketch.txt
        let process_cell_fn =
            move |(cell_id, shard): (String, &mut Box<&mut dyn ShardFileExtractor>)| {
                let list_files = shard
                    .get_files_for_cell()
                    .expect("Could not get list of files for cell"); //////////// TODO may fail if we scan all files in each input file
                let f1 = "countsketch.txt".to_string();

                println!("{:?}", list_files);

                if list_files.contains(&f1) {
                    let path_f1 = clone_params
                        .path_tmp
                        .join(format!("cell_{}.countsketch.txt", cell_id).to_string());
                    shard.extract_as(&f1, &path_f1).unwrap(); // TODO way of getting content directly?

                    let mut bw = bw.lock().unwrap();

                    //Print which cell we are in
                    write!(bw, "{}", cell_id).unwrap();

                    //Get the content and add to list
                    let file = File::open(&path_f1).expect(&format!(
                        "Could not open extracted file for reading {}",
                        &path_f1.display()
                    ));
                    let lines = std::io::BufReader::new(file).lines();
                    for line in lines {
                        let line = line.unwrap();
                        write!(bw, "\t{}", line).unwrap();
                    }
                    writeln!(bw, "").unwrap();

                    //Delete file when done with it
                    std::fs::remove_file(&path_f1).expect("Could not remove temp file");

                    //Count cells
                    //                *cell_count.lock().unwrap() += 1;
                    //                let mut cell_count = cell_count.lock().unwrap();
                    //                *cell_count += 1;
                    //                *cell_count = cell_count + 1;
                }
            };
        let process_cell_fn = Arc::new(process_cell_fn);

        //Process each input file
        for path_input in &params.path_input {
            //Iterate over all cells using suitable readers
            let path_input = path_input.clone();
            let num_threads_total = params.num_threads_total;
            iterate_shard_reader::iterate_shard_reader_multithreaded(
                num_threads_total,
                &path_input.clone(),
                &process_cell_fn,
            )?;
        }

        /*
                let cell_count = cell_count.lock().unwrap();
                println!("Obtained CS from {} cells", cell_count);
        */

        //Delete temp folder
        fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }
}
