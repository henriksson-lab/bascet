use std::fs;
use std::sync::Arc;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use anyhow::Result;
use clap::Args;

use anyhow::bail;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use log::info;
use zip::ZipWriter;

use crate::command::threadcount::determine_thread_counts_mapcell;
use crate::fileformat::tirp::TirpBascetShardReaderFactory;
use crate::fileformat::zip::ZipBascetShardReaderFactory;
use crate::fileformat::TirpStreamingShardReaderFactory;
use crate::utils;

use crate::fileformat;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ShardStreamingFileExtractor;
use crate::fileformat::ConstructFromPath;
use crate::fileformat::detect_shard_format;
use crate::fileformat::DetectedFileformat;

use crate::mapcell::CompressionMode;
use crate::mapcell::MissingFileMode;
use crate::mapcell::MapCellFunction;
use crate::{command::mapcell, mapcell::MapCellFunctionShellScript};


pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct MapCellCMD {
    // Input bascet, TIRP etc
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: Option<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: Option<PathBuf>,


    //The script to run
    #[arg(short = 's', value_parser = clap::value_parser!(PathBuf))]
    pub path_script: PathBuf,

    //If we should show script output in terminal
    #[arg(long = "show-script-output")]
    pub show_script_output: bool,


    //Show a list of preset scripts available
    #[arg(long = "show-presets")]
    pub show_presets: bool,

    //Keep files extracted for the script. For debugging purposes
    #[arg(long = "keep-files")]
    pub keep_files: bool,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    num_threads_read: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    num_threads_write: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    num_threads_mapcell: Option<usize>,
}



impl MapCellCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        if self.show_presets {
            let names = crate::mapcell_scripts::get_preset_script_names();
            println!("Available preset scripts: {:?}", names);
            return Ok(());
        }

        //Figure out what script to use.
        //Check if using a new script or a preset. user scripts start with _
        let preset_name = self.path_script.to_str().expect("argument conversion error");
        let script: Arc<Box<dyn MapCellFunction>> = if preset_name.starts_with("_") {
            println!("Using preset script: {:?}", self.path_script);
            let preset_name=&preset_name[1..]; //Remove the initial _  ; or capital letter? 
            crate::mapcell_scripts::get_preset_script(preset_name).expect("Unable to load preset script")            
        } else {
            println!("Using user provided script: {:?}", self.path_script);
            let s = MapCellFunctionShellScript::new_from_file(&self.path_script).expect("Failed to load user defined script");
            Arc::new(Box::new(s))
        };

        println!("Script info: {:?}", script);

        //Normally we give one thread to each mapcell script
        //let num_threads_mapcell = self.num_threads_mapcell.unwrap_or(1);

        //Note: we always have two extra writer threads, because reading is expected to be the slow part. not an ideal implementation!
        let (num_threads_read, num_threads_write, num_threads_mapcell) = determine_thread_counts_mapcell(
            self.num_threads_total,
            self.num_threads_read,
            self.num_threads_write,
            self.num_threads_mapcell,
            script.get_recommend_threads()
        )?;
        println!("Using threads, readers: {}, writers: {}, mapcell: {}",num_threads_read, num_threads_write, num_threads_mapcell);



        let params = mapcell::MapCell {
            
            path_in: self.path_in.as_ref().expect("Input file was not provided").clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.as_ref().expect("Output file was not provided").clone(),
            script: script,

            threads_read: num_threads_read,
            threads_write: num_threads_write,
            threads_mapcell: num_threads_mapcell,

            show_script_output: self.show_script_output,
            keep_files: self.keep_files            
        };

        let _ = mapcell::MapCell::run(params).expect("mapcell failed");

        log::info!("Mapcell has finished!");
        Ok(())
    }
}




#[derive(Clone)]
pub struct MapCell {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
    
    pub script: Arc<Box<dyn MapCellFunction>>, 

    //How many threads are reading the input zip file?
    pub threads_read: usize,
    //How many runners are there? each runner writes it's own zip file output, to be merged later
    pub threads_write: usize,
    //How many threads should the invoked script use? Passed on as a parameter. Not all commands will support this
    pub threads_mapcell: usize,

    pub show_script_output: bool,    
    pub keep_files: bool

}

impl MapCell {

    pub fn run(
        params: MapCell
    ) -> anyhow::Result<()> {


        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            println!("Using tempdir {}", params.path_tmp.display());
            if fs::create_dir_all(&params.path_tmp).is_err() {
                panic!("Failed to create temporary directory");
            };  
        }

        let params = Arc::new(params);

        //Limit cells in queue to how many we can process at the final stage  ------------- would be nice with a general getter to not replicate code!
        let read_queue_size = params.threads_write*2;

        println!("Queue of cells is of size: {}",read_queue_size);
    
        //Queue of cells that have been extracted
        let (tx_loaded_cell, rx_loaded_cell) = crossbeam::channel::bounded::<Option<String>>(read_queue_size);
        let (tx_loaded_cell, rx_loaded_cell) = (Arc::new(tx_loaded_cell), Arc::new(rx_loaded_cell));

        //Create all writers. these also take care of running mapcell
        let thread_pool_writers = threadpool::ThreadPool::new(params.threads_write);
        let mut list_out_zipfiles: Vec<PathBuf> = Vec::new();
        for tidx in 0..params.threads_write {
            let file_zip = params.path_tmp.join(format!("out-{}.zip", tidx));
            println!("Temporary zip file {}", file_zip.display());
            _ = create_writer(
                &params,
                &file_zip,
                &Arc::clone(&params.script),
                &thread_pool_writers,
                &rx_loaded_cell
            );

            list_out_zipfiles.push(file_zip);
        }

        //Figure out how to read the data
        let input_shard_type = detect_shard_format(&params.path_in);

        println!("Input file: {:?}",params.path_in);

        let perform_streaming=input_shard_type == DetectedFileformat::TIRP;

        if perform_streaming {
            ////////////////////////////////// Streaming reading of input
            println!("Reading will be streamed");

            //Create all streaming readers. Detect what we need from the file extension.
            //The readers will start immediately
            let thread_pool_readers = threadpool::ThreadPool::new(params.threads_read);
            if input_shard_type == DetectedFileformat::TIRP {
                println!("Detected input as TIRP");
                for _tidx in 0..params.threads_read {  /////////// option #2: keep list of files separately from list of readers
                    _ = create_streaming_shard_reader(
                        &params,
                        &thread_pool_readers,
                        &Arc::clone(&params.script),
                        &tx_loaded_cell,
                        &Arc::new(TirpStreamingShardReaderFactory::new())
                    );
                }
            } else {
                bail!("Cannot tell the type of the input format"); /////////////////////////// TODO add support for BAM etc as a shardreader
            }

            //Wait for all reader threads to complete
            thread_pool_readers.join();
            println!("Streaming readers have finished")

        } else {
            ////////////////////////////////// Random reading of input
            println!("Reading will be random (this can be slow depending on file format)");

//            panic!("this need to be rewritten; let readers stream on their own")

            let thread_pool_readers = threadpool::ThreadPool::new(params.threads_read);

            //Create all random readers. Detect what we need from the file extension
            //let reader_thread_group = ThreadGroup::new(params.threads_read);
            let input_shard_type = detect_shard_format(&params.path_in);
            if input_shard_type == DetectedFileformat::TIRP {
                println!("Detected input as TIRP");
                for _tidx in 0..params.threads_read {  /////////// option #2: keep list of files separately from list of readers
                    _ = create_random_shard_reader(
                        &params,
                        &thread_pool_readers,
                        &Arc::clone(&params.script),
                        &tx_loaded_cell,
                        &Arc::new(TirpBascetShardReaderFactory::new())
                    );
                }
            } else if input_shard_type == DetectedFileformat::ZIP {
                println!("Detected input as ZIP");
                // note from julian: readers alter the ZIP file? at least make separate readers. start with just 1
                for _tidx in 0..params.threads_read {
                    _ = create_random_shard_reader(
                        &params,
                        &thread_pool_readers,
                        &Arc::clone(&params.script),
                        &tx_loaded_cell,
                        &Arc::new(ZipBascetShardReaderFactory::new())
                    );
                }
            } else {
                bail!("Cannot tell the type of the input format");
            }

            //Wait for all reader threads to complete
            thread_pool_readers.join();
            println!("Random I/O readers have finished")
        }


        //Terminate all writers. Then wait for all threads to finish
        println!("Waiting for writers to finish");
        for i in 0..params.threads_write {
            println!("Sending termination signal to writer {i}");
            _ = tx_loaded_cell.send(None).unwrap();
        }
        thread_pool_writers.join();
        println!("Writers have finished");
        
        // Merge temp zip archives into one new zip archive 
        println!("Merging zip from writers");
        utils::merge_archives_and_delete(&params.path_out, &list_out_zipfiles).unwrap();

        //Finally remove the temp directory
        if !params.keep_files {
            let _ = fs::remove_dir_all(&params.path_tmp);
        }

        Ok(())
    }
}



//////////////////////////////////// Reader for random I/O shard files
fn create_random_shard_reader<R>(
    params_io: &Arc<MapCell>,
    thread_pool: &threadpool::ThreadPool,
    mapcell_script: &Arc<Box<dyn MapCellFunction>>,
    tx: &Arc<Sender<Option<String>>>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R:ShardRandomFileExtractor {

    let tx = Arc::clone(tx);

    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);

    let constructor = Arc::clone(constructor);

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor.new_from_path(&params_io.path_in).expect("Failed to create bascet reader");

        //Tell readers to go through all cells, then terminate all readers
        let list_cells = fileformat::try_get_cells_in_file(&params_io.path_in).expect("Could not get list of cells from input file");
        let list_cells = if let Some(list_cells) = list_cells {
            list_cells
        } else {
            panic!("unable to figure out a list of cells ahead of time; this has not yet been implemented (provide suitable input file format, or manually specify cells)");
        };

        // TODO: each reader manages its own list of cells
        let mut num_cells_processed = 0;
        for cell_id in list_cells { //  let Ok(Some(cell_id)) = rx.recv()
            info!("request to read {}",cell_id);

            if num_cells_processed%10 ==0 {
                println!("processed {} cells, now at {}",num_cells_processed, cell_id);
            }

            let path_cell_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            fs::create_dir(&path_cell_dir).unwrap();

            let fail_if_missing = mapcell_script.get_missing_file_mode() != MissingFileMode::Ignore;
            let success = shard.extract_to_outdir(
                &cell_id, 
                &mapcell_script.get_expect_files(),
                fail_if_missing,
                &path_cell_dir
            ).expect("error during extraction");

            if success {
                //Inform writer that the cell is ready for processing
                _ = tx.send(Some(cell_id));
            } else {
                let missing_file_mode = mapcell_script.get_missing_file_mode();

                if missing_file_mode==MissingFileMode::Fail {
                    panic!("Failed extraction of {}; shutting down process, keeping temp files for inspection", cell_id);
                } 
                if missing_file_mode==MissingFileMode::Ignore {
                    println!("Did not find all expected files for '{}', ignoring. Files present: {:?}", cell_id, shard.get_files_for_cell(&cell_id));
                }
            }

            num_cells_processed += 1;
        }
        println!("Reader ended; read a total of {} cells", num_cells_processed);
    });
    Ok(())
}







//////////////////////////////////// Reader for streaming I/O shard files
fn create_streaming_shard_reader<R>(
    params_io: &Arc<MapCell>,
    thread_pool: &threadpool::ThreadPool,
    mapcell_script: &Arc<Box<dyn MapCellFunction>>,
    tx: &Arc<Sender<Option<String>>>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R:ShardStreamingFileExtractor {

    let tx = Arc::clone(tx);

    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);

    //let thread_group = Arc::clone(thread_group);
    let constructor = Arc::clone(constructor);

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor.new_from_path(&params_io.path_in).expect("Failed to create bascet reader");

        let mut num_cells_processed = 0;
        while let Ok(Some(cell_id)) = shard.next_cell() {

            //println!("Starting extraction of {}", num_cells_processed);

            if num_cells_processed%10 ==0 {
                println!("processed {} cells, now at {}",num_cells_processed, cell_id);
            }

            let path_cell_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            let _ = fs::create_dir(&path_cell_dir);  

            let fail_if_missing = mapcell_script.get_missing_file_mode() != MissingFileMode::Ignore;
            let success = shard.extract_to_outdir(
                &mapcell_script.get_expect_files(),
                fail_if_missing,
                &path_cell_dir
            ).expect("error during extraction");

            //println!("Done extraction of {}", num_cells_processed);

            if success {
                //Inform writer that the cell is ready for processing
                _ = tx.send(Some(cell_id));
            } else {
                let missing_file_mode = mapcell_script.get_missing_file_mode();

                if missing_file_mode==MissingFileMode::Fail {
                    panic!("Failed extraction of {}; shutting down process, keeping temp files for inspection", cell_id);
                } 
                if missing_file_mode==MissingFileMode::Ignore {
                    println!("Did not find all expected files for '{}', ignoring. Files present: {:?}", cell_id, shard.get_files_for_cell());
                } 
            }
            num_cells_processed+=1;
        }
        println!("Reader ended; read a total of {} cells", num_cells_processed);
    });
    Ok(())
}










///////////////////////////// Worker thread that integrates the writing. in the future, could have a Writer trait instead of hardcoding ZIP files
fn create_writer(
    params_io: &Arc<MapCell>,
    zip_file: &PathBuf,
    mapcell_script: &Arc<Box<dyn MapCellFunction>>,
    thread_pool: &threadpool::ThreadPool,
    rx: &Arc<Receiver<Option<String>>>
) -> anyhow::Result<()> {
    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);
    let rx = Arc::clone(rx);
    let zip_file = zip_file.clone();
    thread_pool.execute(move || {

        //Open zip file for writing
        println!("Writer started");
        let zip_file = File::create(zip_file).unwrap();  //////// called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }
        let buf_writer = BufWriter::new(zip_file);
        let mut zip_writer = ZipWriter::new(buf_writer);
        
        //Handle each cell, for which files have now been extracted
        while let Ok(Some(cell_id)) = rx.recv() {

            //println!("Writer starting mapcell for extracted {}",cell_id);

            //////// Run the script on the input, creating files in output
            let path_input_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            let _ = fs::create_dir(&path_input_dir);  

            let path_output_dir = params_io.path_tmp.join(format!("output-{}", cell_id));
            let _ = fs::create_dir(&path_output_dir);  

            println!("Writer for '{}', running script", cell_id);
            let (success, script_output) = mapcell_script.invoke(
                &path_input_dir,
                &path_output_dir,
            params_io.threads_mapcell
            ).expect("Failed to invoke script"); 
            println!("Writer for '{}', done running script", cell_id);

            if !success {
                if mapcell_script.get_missing_file_mode()==MissingFileMode::Fail {
                    panic!("Failed to process a cell, and this script is set to fail in such a scenario");
                }
            }

            //Show script output in terminal if requested
            if params_io.show_script_output {
                println!("{}",&script_output);
            }

            //Store script output as log file
            println!("Writer for '{}', adding log file to zip", cell_id);
            {
                let path_logfile = path_output_dir.join("_mapcell.log");
                let log_file = File::create(&path_logfile).unwrap();
                let mut buf_writer = BufWriter::new(log_file);
                let _ = std::io::copy(&mut script_output.as_bytes(), &mut buf_writer).unwrap();   
            }

            //Check what files we got out from executing the script
            let list_output_files = recurse_files(&path_output_dir).expect("failed to list output files");

            //////// Add all files in output to the zip file
            //chop off params_io.path_tmp from each path, to get name in zip. not sure how safe this approach is for different OS'
            let basepath_len = params_io.path_tmp.display().to_string().len() + 1 + "output-".len();
            let fname_as_string: Vec<String> = list_output_files.iter().map(|f| f.display().to_string() ).collect();
            let names_in_zip: Vec<&str> = fname_as_string.iter().map(|f| &f[basepath_len..] ).collect();

            println!("Writer for '{}', got files {:?}", cell_id, list_output_files);
            println!("Writer for '{}', got names {:?}", cell_id, names_in_zip);

            //Add each file to the zip
            for (file_path, &file_name) in list_output_files.iter().zip(names_in_zip.iter()) {
                println!("Writer for '{}', adding to zip: {}",cell_id, file_path.display());

                //Open file for reading
                let mut file_input = File::open(&file_path).unwrap();

                //Set up zip file
                let compression_mode = match mapcell_script.get_compression_mode(file_name) {
                    CompressionMode::Default => zip::CompressionMethod::Zstd,  //R unzip does not support natively
//                    mapcell_script::CompressionMode::Default => zip::CompressionMethod::DEFLATE,  //not as fast; for testing only. it really is ridiculously slow on zip 1.x
                    CompressionMode::Uncompressed => zip::CompressionMethod::Stored,
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

            //println!("Writer done mapcell for extracted {}",cell_id);

        }
        println!("Writer got stop signal, now finishing zip");

        let _ = zip_writer.finish();   
        println!("Writer exiting");
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

