use std::fs;
use std::sync::Arc;
use std::fs::File;
use std::io::BufWriter;
use std::process;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use anyhow::bail;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use itertools::Itertools;
use log::info;
use log::debug;
use zip::ZipWriter;

use crate::utils;
use crate::fileformat::bascet::BascetShardReader;
use crate::fileformat::mapcell_script;
use crate::fileformat::mapcell_script::MapCellScript;
use crate::fileformat::mapcell_script::MissingFileMode;

use super::params;



pub struct MapCell {}

impl MapCell {

    pub fn run(
        params_io: params::IO
    ) -> anyhow::Result<()> {

        let mut params_io = params_io.clone();

        //Create thread pool. note that worker threads here refer to script threads (script manages it)
        let thread_pool = threadpool::ThreadPool::new(params_io.threads_read + params_io.threads_write);

        //Need to create temp dir
        if params_io.path_tmp.exists() {
            //todo delete temp dir after run
            bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params_io.path_tmp.display());
        } else {
            let _ = fs::create_dir(&params_io.path_tmp);  
        }
        //Check if using a new script or a preset. user scripts start with _
        if params_io.path_script.to_str().expect("argument conversion error").starts_with("_") {
            println!("using preset {:?}", params_io.path_script);

            let map_presets = get_preset_scripts();
            let preset_name=params_io.path_script.to_str().expect("failed to get string from script path");
            let preset_name=&preset_name[1..]; //Remove the initial _  ; or capital letter? 

            if let Some(&ref preset_script_code) = map_presets.get(preset_name) {
                //If using a preset, create the file
                let path_script = params_io.path_tmp.join("preset_script.sh");
                {
                    let script_file = File::create(&path_script).unwrap();
                    let mut buf_writer = BufWriter::new(script_file);
                    let _ = std::io::copy(&mut preset_script_code.as_slice(), &mut buf_writer).unwrap();   
                }
                //Make the script executable
                let _ = process::Command::new("chmod")
                    .arg("u+x")
                    .arg(path_script.to_str().expect("failed to convert string"))
                    .output()?;
                //Use this script
                println!("Extracted preset script to {:?}", &path_script);
                params_io.path_script = path_script;
            } else {
                bail!("Preset {} does not exist", preset_name);
            }
        } else {
            println!("Using user provided script");
        }

        let params_io = Arc::new(params_io);


        //Initialize script
        let mapcell_script = Arc::new(MapCellScript::new(&params_io.path_script)?);
        println!("Script API version: {}", mapcell_script.api_version);
        println!("Script expects files: {:?}", mapcell_script.expect_files);
        println!("Script file missing mode: {}", mapcell_script.missing_file_mode);

        //Limit cells in queue to how many we can process at the final stage
        let shard = BascetShardReader::new(&params_io.path_in)?;
        let list_cells = shard.files_for_cell.keys().collect::<Vec<&String>>();
        let queue_limit = params_io.threads_write*2;

        //Queue of cells to be extracted
        let (tx_cell_to_read, rx_cell_to_read) = crossbeam::channel::bounded::<Option<String>>(queue_limit);
        let (tx_cell_to_read, rx_cell_to_read) = (Arc::new(tx_cell_to_read), Arc::new(rx_cell_to_read));
    
        //Queue of cells that have been extracted
        let (tx_loaded_cell, rx_loaded_cell) = crossbeam::channel::bounded::<Option<String>>(queue_limit);
        let (tx_loaded_cell, rx_loaded_cell) = (Arc::new(tx_loaded_cell), Arc::new(rx_loaded_cell));


        //Create all readers
        // note from julian: readers alter the file? at least make separate readers. start with just 1
        let reader_thread_group = ThreadGroup::new(params_io.threads_read);//: &Arc<ThreadGroup>

        for _tidx in 0..params_io.threads_read {
            _ = create_reader(
                &params_io,
                &thread_pool,
                &mapcell_script,
                &rx_cell_to_read,
                &tx_loaded_cell,
                &reader_thread_group
            );
        }

        //Create all writers
        let mut list_out_zipfiles: Vec<PathBuf> = Vec::new();
        for tidx in 0..params_io.threads_write {
            let file_zip = params_io.path_tmp.join(format!("out-{}.zip", tidx));
            _ = create_writer(
                &params_io,
                &file_zip,
                &mapcell_script,
                &thread_pool,
                &rx_loaded_cell
            );

            list_out_zipfiles.push(file_zip);
        }

        //Go through all cells, then terminate all readers
        for cell_id in list_cells {
            _ = tx_cell_to_read.send(Some(cell_id.clone()));
        }
        for i in 0..params_io.threads_read {
            debug!("Sending termination signal to reader {i}");
            _ = tx_cell_to_read.send(None).unwrap();
        }

        //Wait for all reader threads to complete. Readers tell writers to finish
        reader_thread_group.join();

        //Terminate all writers. Then wait for all threads to finish
        for i in 0..params_io.threads_write {
            debug!("Sending termination signal to writer {i}");
            _ = tx_loaded_cell.send(None).unwrap();
        }
        thread_pool.join();
        
        // Merge temp zip archives into one new zip archive 
        println!("Merging zip from writers");
        utils::merge_archives_and_delete(&params_io.path_out, &list_out_zipfiles).unwrap();

        //Finally remove the temp directory
        if !params_io.keep_files {
            let _ = fs::remove_dir_all(&params_io.path_tmp);
        }

        Ok(())
    }
}




fn create_reader(
    params_io: &Arc<params::IO>,
    thread_pool: &threadpool::ThreadPool,
    mapcell_script: &Arc<MapCellScript>,
    rx: &Arc<Receiver<Option<String>>>,
    tx: &Arc<Sender<Option<String>>>,
    thread_group: &Arc<ThreadGroup>
) -> anyhow::Result<()> {

    let rx = Arc::clone(rx);
    let tx = Arc::clone(tx);

    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);

    let thread_group = Arc::clone(thread_group);

    thread_pool.execute(move || {
        debug!("Worker started");

        let mut shard = BascetShardReader::new(&params_io.path_in).expect("Failed to create bascet reader");

        while let Ok(Some(cell_id)) = rx.recv() {
            info!("request to read {}",cell_id);

            let path_cell_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            let _ = fs::create_dir(&path_cell_dir);  


            let fail_if_missing = mapcell_script.missing_file_mode != MissingFileMode::Ignore;
            let success = shard.extract_to_outdir(
                &cell_id, 
                &mapcell_script.expect_files,
                fail_if_missing,
                &path_cell_dir//&PathBuf::from("/Users/mahogny/Desktop/rust/hack_robert/testdata/out")
            ).expect("error during extraction");

            if success {
                //Inform writer that the cell is ready for processing
                _ = tx.send(Some(cell_id));
            } else {
                if mapcell_script.missing_file_mode==MissingFileMode::Fail {
                    panic!("Failed extraction of {}; shutting down process, keeping temp files for inspection", cell_id);
                } 
                if mapcell_script.missing_file_mode==MissingFileMode::Ignore {
                    println!("Did not find all expected files for '{}', ignoring. Files present: {:?}", cell_id, shard.files_for_cell.get(&cell_id));
                } 
            }
        }
        thread_group.is_done();
    });
    Ok(())
}






fn create_writer(
    params_io: &Arc<params::IO>,
    zip_file: &PathBuf,
    mapcell_script: &Arc<MapCellScript>,
    thread_pool: &threadpool::ThreadPool,
    rx: &Arc<Receiver<Option<String>>>
) -> anyhow::Result<()> {
    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);
    let rx = Arc::clone(rx);
    let zip_file = zip_file.clone();
    thread_pool.execute(move || {

        //Open zip file for writing
        debug!("Writer started");
        let zip_file = File::create(zip_file).unwrap();
        let buf_writer = BufWriter::new(zip_file);
        let mut zip_writer = ZipWriter::new(buf_writer);
        
        //Handle all extracted cells
        while let Ok(Some(cell_id)) = rx.recv() {

            println!("Processing extracted {}",cell_id);

            //////// Run the script on the input, creating files in output
            let path_input_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            let _ = fs::create_dir(&path_input_dir);  

            let path_output_dir = params_io.path_tmp.join(format!("output-{}", cell_id));
            let _ = fs::create_dir(&path_output_dir);  

            let (success, script_output) = mapcell_script.invoke(
                &path_input_dir,
                &path_output_dir,
            params_io.threads_work
            ).expect("Failed to invoke script");

            if !success && mapcell_script.missing_file_mode==MissingFileMode::Fail {
                panic!("Failed to process a cell, and this script is set to fail in such a scenario");
            }

            //Store script output as log file
            {
                let path_logfile = path_output_dir.join("cellmap.log");
                let log_file = File::create(&path_logfile).unwrap();
                let mut buf_writer = BufWriter::new(log_file);
                let _ = std::io::copy(&mut script_output.as_bytes(), &mut buf_writer).unwrap();   
            }

            //Check what files we got out
            let list_output_files = recurse_files(&path_output_dir).expect("failed to list output files");

            //////// Add all files in output to the zip file
            //chop off params_io.path_tmp from each path, to get name in zip. not sure how safe this approach is for different OS'
            let basepath_len = params_io.path_tmp.display().to_string().len() + 1 + "output-".len();
            let fname_as_string: Vec<String> = list_output_files.iter().map(|f| f.display().to_string() ).collect();
            let names_in_zip: Vec<&str> = fname_as_string.iter().map(|f| &f[basepath_len..] ).collect();

            debug!("got files {:?}", list_output_files);
            debug!("got names {:?}", names_in_zip);

            //Add each file to the zip
            for (file_path, &file_name) in list_output_files.iter().zip(names_in_zip.iter()) {

                //Open file for reading
                let mut file_input = File::open(&file_path).unwrap();

                //Set up zip file
                let compression_mode = match mapcell_script.compression_mode {
//                    mapcell_script::CompressionMode::Default => zip::CompressionMethod::Zstd,  //R unzip does not support natively
                    mapcell_script::CompressionMode::Default => zip::CompressionMethod::DEFLATE,  //not as fast; for testing only
                    mapcell_script::CompressionMode::Uncompressed => zip::CompressionMethod::Stored,
                };
                let opts_zipwriter: zip::write::FileOptions<()> = zip::write::FileOptions::default().compression_method(compression_mode);

                //Write zip entry
                let _ = zip_writer.start_file(file_name, opts_zipwriter);
                let _ = std::io::copy(&mut file_input, &mut zip_writer).unwrap();
            }

            //Remove input and output files
            if !params_io.keep_files {
                let _ = fs::remove_dir_all(&path_input_dir);
                let _ = fs::remove_dir_all(&path_output_dir);
            }
        }
        let _ = zip_writer.finish();   
        debug!("Writer exiting");
        // note from julian: included finishing the writers here before, chance that removing this fucked things up
        //      but unfortunately borrow checker didnt like that at all
    });


    Ok(())
}



fn recurse_files(path: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let mut buf = vec![];
    let entries = fs::read_dir(path)?;

    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;

        if meta.is_dir() {
            let mut subdir = recurse_files(entry.path())?;
            buf.append(&mut subdir);
        }

        if meta.is_file() {
            buf.push(entry.path());
        }
    }

    Ok(buf)
}






const PRESET_SCRIPT_TEST: &[u8] = include_bytes!("test_script.sh");

pub fn get_preset_scripts() -> HashMap<String,Vec<u8>> {
    let mut map: HashMap<String, Vec<u8>> = HashMap::new();
    map.insert("test".to_string(), PRESET_SCRIPT_TEST.to_vec());
    map
}


pub fn get_preset_script_names() -> Vec<String> {
    let map= get_preset_scripts();
    let names: Vec<String> =map.keys().sorted().cloned().collect();
    names
}

















/////////////////// barrier for a set of threads
struct ThreadGroup {
    rx_done: Receiver<()>,
    tx_done: Sender<()>,
    num_thread: usize
}
impl ThreadGroup {
    pub fn new(num_thread:usize) -> Arc<ThreadGroup> {
        let (tx_done, rx_done) = crossbeam::channel::bounded::<()>(1000);
        return Arc::new(ThreadGroup {
            rx_done: rx_done,
            tx_done: tx_done,
            num_thread: num_thread
        })
    }

    pub fn join(&self) {
        for _i in 0..self.num_thread {
            _ = self.rx_done.recv();
        }
    }

    pub fn is_done(&self) {
        _ = self.tx_done.send(());
    }
}


////// would be nice to generalize this pattern, and then hide some things like number of threads etc