use anyhow::Result;
use itertools::Itertools;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::{path::PathBuf, sync::Arc};

use crate::fileformat::read_cell_list_file;
use crate::fileformat::shard::ShardCellDictionary;
use crate::fileformat::CellID;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::utils::check_kmc_tools;

use clap::Args;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;

#[derive(Args)]
pub struct FeaturiseKmcCMD {
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
impl FeaturiseKmcCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };

        let params = FeaturiseParamsKMC {
            path_tmp: self.path_tmp.clone(),
            path_input: self.path_in.clone(),
            path_output: self.path_out.clone(),
            include_cells: include_cells.clone(),
            threads_work: self.threads_work,
        };

        let _ = FeaturiseKMC::run(&Arc::new(params));

        log::info!("Featurise has finished succesfully");
        Ok(())
    }
}

pub struct FeaturiseParamsKMC {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub include_cells: Option<Vec<CellID>>,

    pub threads_work: usize,
}

pub struct FeaturiseKMC {}
impl FeaturiseKMC {
    /// Run the algorithm
    pub fn run(params: &Arc<FeaturiseParamsKMC>) -> anyhow::Result<()> {
        check_kmc_tools().unwrap();

        let mut file_input =
            ZipBascetShardReader::new(&params.path_input).expect("Failed to open input file");

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

        //TODO need to support multiple shard files as input!!
        //or be prepared to always do one final merge if needed --

        //Pick cells to work on
        let list_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {
            file_input
                .get_cell_ids()
                .expect("Failed to get content listing for input file")
        };

        // Unzip all cell-specific kmer databases
        let mut cur_file_id = 0;
        let mut dbs_to_merge: Vec<(PathBuf, String)> = Vec::new();
        for cell_id in list_cells {
            file_input.set_current_cell(&cell_id);

            //Check if a KMC database is present for this cell, otherwise exclude it
            let list_files = file_input
                .get_files_for_cell()
                .expect("Could not get list of files for cell");
            let f1 = "kmc.kmc_suf".to_string();
            let f2 = "kmc.kmc_pre".to_string();
            if list_files.contains(&f1) && list_files.contains(&f2) {
                println!("Extracting cell {}", cell_id);

                let db_file_path = params
                    .path_tmp
                    .join(format!("cell_{}", cur_file_id).to_string());
                let path_f1 = params
                    .path_tmp
                    .join(format!("cell_{}.kmc_suf", cur_file_id).to_string());
                let path_f2 = params
                    .path_tmp
                    .join(format!("cell_{}.kmc_pre", cur_file_id).to_string());

                //Extract the files
                file_input.extract_as(&f1, &path_f1).unwrap();
                file_input.extract_as(&f2, &path_f2).unwrap();

                //Add this db to the list of all db's to merge later
                // NOTE: '-' is a unary operator in kmc complex scripts. cannot be part of name
                dbs_to_merge.push((db_file_path, cell_id)); //// is there any reason to keep cell_id at all?
                cur_file_id += 1;
            }
        }

        // Generate the union script
        println!("Making KMC union script");
        let path_kmc_union_script = params.path_tmp.join("kmc_union.op");
        //let path_kmc_union_db = params.path_tmp.join("kmc_union");
        let path_kmc_union_db = &params.path_output; //.join("kmc_union");
        write_union_script(&path_kmc_union_script, &path_kmc_union_db, dbs_to_merge).unwrap();

        // Run KMC tools on union script --- output is the KMC database
        println!("Running KMC union script");
        run_kmc_tools(&path_kmc_union_script, params.threads_work).unwrap();

        /*
                // Generate a total summary file, text format
                //let path_dump = params.path_output;  //params.path_tmp.join("dump.txt");  /////// or to path out??   should be features.0.txt  ..
                dump_kmc_db(
                    &path_kmc_union_db,
                    &params.path_output
                ).unwrap();
        */

        //Delete temp folder
        fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }
}

fn run_kmc_tools(path_script: &PathBuf, threads_work: usize) -> anyhow::Result<()> {
    let kmc_union = std::process::Command::new("kmc_tools")
        .arg("complex")
        .arg(&path_script)
        .arg("-t")
        .arg(format!("{}", threads_work))
        .output()?;

    if !kmc_union.status.success() {
        anyhow::bail!(
            "KMC merge failed: {}",
            String::from_utf8_lossy(&kmc_union.stderr)
        );
    }

    Ok(())
}

pub fn dump_kmc_db(path_db: &PathBuf, path_dump: &PathBuf) -> anyhow::Result<()> {
    let kmc_dump = std::process::Command::new("kmc_tools")
        .arg("transform")
        .arg(&path_db)
        .arg("dump")
        .arg(&path_dump)
        .output()
        .expect("KMC dump command failed");

    if !kmc_dump.status.success() {
        anyhow::bail!(
            "KMC dump failed: {}",
            String::from_utf8_lossy(&kmc_dump.stderr)
        );
    }

    Ok(())
}

/**
 *  Generate a script (kmc_union.op) that looks like this:
 *
 *
 * INPUT:
 * cell_id = pathToCell_id   #one line per cell
 * OUTPUT:
 * total = cell_id1 + cell_id2 + ....
 *
 */
fn write_union_script(
    path_kmc_union_script: &PathBuf,
    path_output_db: &PathBuf,
    dbs_to_merge: Vec<(PathBuf, CellID)>,
) -> anyhow::Result<()> {
    let file_kmc_union_script =
        File::create(&path_kmc_union_script).expect("Failed to create KMC union script");
    let mut writer_kmc_union_script = BufWriter::new(&file_kmc_union_script);

    writeln!(writer_kmc_union_script, "INPUT:")?;
    for (path, barcode) in &dbs_to_merge {
        writeln!(
            writer_kmc_union_script,
            "{} = {}",
            barcode,
            path.to_str().unwrap()
        )
        .unwrap();
    }
    writeln!(writer_kmc_union_script, "OUTPUT:")?;

    write!(
        writer_kmc_union_script,
        "{} = ",
        &path_output_db.to_str().unwrap()
    )
    .unwrap();

    write!(
        writer_kmc_union_script,
        "{}",
        dbs_to_merge.iter().map(|(_, barcode)| barcode).join(" + ")
    )
    .unwrap();

    writer_kmc_union_script.flush().unwrap();

    Ok(())
}
