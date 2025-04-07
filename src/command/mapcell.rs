use std::fs;
use std::sync::Arc;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;

use anyhow::bail;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use log::info;
use log::debug;
use zip::ZipWriter;

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


#[derive(Clone)]
pub struct MapCellParams {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
    
    pub script: Arc<Box<dyn MapCellFunction>>, 

    //How many threads are reading the input zip file?
    pub threads_read: usize,

    //How many runners are there? each runner writes it's own zip file output, to be merged later
    pub threads_write: usize,

    //How many threads should the invoked script use? Passed on as a parameter. Not all commands will support this
    pub threads_work: usize,

    pub show_script_output: bool,
    
    pub keep_files: bool
}




pub struct MapCell {}

impl MapCell {

    pub fn run(
        params: MapCellParams
    ) -> anyhow::Result<()> {

        //Create thread pool. note that worker threads here refer to script threads (script manages it)
        let thread_pool = threadpool::ThreadPool::new(params.threads_read + params.threads_write);

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


        //Queue of cells that have been extracted
        let (tx_loaded_cell, rx_loaded_cell) = crossbeam::channel::bounded::<Option<String>>(read_queue_size);
        let (tx_loaded_cell, rx_loaded_cell) = (Arc::new(tx_loaded_cell), Arc::new(rx_loaded_cell));

        //Create all writers
        let mut list_out_zipfiles: Vec<PathBuf> = Vec::new();
        for tidx in 0..params.threads_write {
            let file_zip = params.path_tmp.join(format!("out-{}.zip", tidx));
            println!("Temporary zip file {}", file_zip.display());
            _ = create_writer(
                &params,
                &file_zip,
                &Arc::clone(&params.script),
                &thread_pool,
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
            let reader_thread_group = ThreadGroup::new(params.threads_read);
            if input_shard_type == DetectedFileformat::TIRP {
                println!("Detected input as TIRP");
                for _tidx in 0..params.threads_read {
                    _ = create_streaming_shard_reader(
                        &params,
                        &thread_pool,
                        &Arc::clone(&params.script),
                        &tx_loaded_cell,
                        &reader_thread_group,
                        &Arc::new(TirpStreamingShardReaderFactory::new())
                    );
                }
            } else {
                bail!("Cannot tell the type of the input format");
            }

            //Wait for all reader threads to complete
            reader_thread_group.join();


        } else {
            ////////////////////////////////// Random reading of input
            println!("Reading will be random (this can be slow depending on file format)");

            //Queue of cells to be extracted
            let (tx_cell_to_read, rx_cell_to_read) = crossbeam::channel::unbounded::<Option<String>>();
            let (tx_cell_to_read, rx_cell_to_read) = (Arc::new(tx_cell_to_read), Arc::new(rx_cell_to_read));        

            //Create all random readers. Detect what we need from the file extension
            let reader_thread_group = ThreadGroup::new(params.threads_read);
            let input_shard_type = detect_shard_format(&params.path_in);
            if input_shard_type == DetectedFileformat::TIRP {
                println!("Detected input as TIRP");
                for _tidx in 0..params.threads_read {
                    _ = create_random_shard_reader(
                        &params,
                        &thread_pool,
                        &Arc::clone(&params.script),
                        &rx_cell_to_read,
                        &tx_loaded_cell,
                        &reader_thread_group,
                        &Arc::new(TirpBascetShardReaderFactory::new())
                    );
                }
            } else if input_shard_type == DetectedFileformat::ZIP {
                println!("Detected input as ZIP");
                // note from julian: readers alter the ZIP file? at least make separate readers. start with just 1
                for _tidx in 0..params.threads_read {
                    _ = create_random_shard_reader(
                        &params,
                        &thread_pool,
                        &Arc::clone(&params.script),
                        &rx_cell_to_read,
                        &tx_loaded_cell,
                        &reader_thread_group,
                        &Arc::new(ZipBascetShardReaderFactory::new())
                    );
                }
            } else {
                bail!("Cannot tell the type of the input format");
            }

            //Tell readers to go through all cells, then terminate all readers
            let list_cells = fileformat::try_get_cells_in_file(&params.path_in).expect("Could not get list of cells from input file");
            if let Some(list_cells) = list_cells {
                let num_total_cell = list_cells.len();
                for cell_id in list_cells {
                    _ = tx_cell_to_read.send(Some(cell_id.clone()));
                }
                println!("Processed a final of {} cells", num_total_cell);
            } else {
                panic!("unable to figure out a list of cells ahead of time; this has not yet been implemented (provide suitable input file format, or manually specify cells)");
            }
            for i in 0..params.threads_read {
                debug!("Sending termination signal to reader {i}");
                _ = tx_cell_to_read.send(None).unwrap();
            }

            //Wait for all reader threads to complete
            reader_thread_group.join();

        }


        //Terminate all writers. Then wait for all threads to finish
        for i in 0..params.threads_write {
            debug!("Sending termination signal to writer {i}");
            _ = tx_loaded_cell.send(None).unwrap();
        }
        thread_pool.join();
        
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
    params_io: &Arc<MapCellParams>,
    thread_pool: &threadpool::ThreadPool,
    mapcell_script: &Arc<Box<dyn MapCellFunction>>,
    rx: &Arc<Receiver<Option<String>>>,
    tx: &Arc<Sender<Option<String>>>,
    thread_group: &Arc<ThreadGroup>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R:ShardRandomFileExtractor {

    let rx = Arc::clone(rx);
    let tx = Arc::clone(tx);

    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);

    let thread_group = Arc::clone(thread_group);
    let constructor = Arc::clone(constructor);

    thread_pool.execute(move || {
        debug!("Worker started");

        let mut shard = constructor.new_from_path(&params_io.path_in).expect("Failed to create bascet reader");

        while let Ok(Some(cell_id)) = rx.recv() {
            info!("request to read {}",cell_id);

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
        }
        thread_group.is_done();
    });
    Ok(())
}







//////////////////////////////////// Reader for streaming I/O shard files
fn create_streaming_shard_reader<R>(
    params_io: &Arc<MapCellParams>,
    thread_pool: &threadpool::ThreadPool,
    mapcell_script: &Arc<Box<dyn MapCellFunction>>,
    tx: &Arc<Sender<Option<String>>>,
    thread_group: &Arc<ThreadGroup>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R:ShardStreamingFileExtractor {

    let tx = Arc::clone(tx);

    let params_io = Arc::clone(&params_io);
    let mapcell_script = Arc::clone(mapcell_script);

    let thread_group = Arc::clone(thread_group);
    let constructor = Arc::clone(constructor);

    thread_pool.execute(move || {
        debug!("Worker started");

        let mut shard = constructor.new_from_path(&params_io.path_in).expect("Failed to create bascet reader");

        let mut num_cells_processed = 0;
        while let Ok(Some(cell_id)) = shard.next_cell() {
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
        thread_group.is_done();
    });
    Ok(())
}










///////////////////////////// Worker thread that integrates the writing. in the future, could have a Writer trait instead of hardcoding ZIP files
fn create_writer(
    params_io: &Arc<MapCellParams>,
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
        debug!("Writer started");
        let zip_file = File::create(zip_file).unwrap();  //////// called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }
        let buf_writer = BufWriter::new(zip_file);
        let mut zip_writer = ZipWriter::new(buf_writer);
        
        //Handle each cell, for which files have now been extracted
        while let Ok(Some(cell_id)) = rx.recv() {

            //println!("Processing extracted {}",cell_id);

            //////// Run the script on the input, creating files in output
            let path_input_dir = params_io.path_tmp.join(format!("cell-{}", cell_id));
            let _ = fs::create_dir(&path_input_dir);  

            let path_output_dir = params_io.path_tmp.join(format!("output-{}", cell_id));
            let _ = fs::create_dir(&path_output_dir);  

            debug!("Writer for '{}', running script", cell_id);
            let (success, script_output) = mapcell_script.invoke(
                &path_input_dir,
                &path_output_dir,
            params_io.threads_work
            ).expect("Failed to invoke script"); ////////////////// thread '<unnamed>' panicked at src/command/mapcell.rs:396:15:  Failed to invoke script: No such file or directory (os error 2)
            debug!("Writer for '{}', done running script", cell_id);

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
            debug!("Writer for '{}', adding log file to zip", cell_id);
            {
                let path_logfile = path_output_dir.join("cellmap.log");
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

            debug!("Writer for '{}', got files {:?}", cell_id, list_output_files);
            debug!("Writer for '{}', got names {:?}", cell_id, names_in_zip);

            //Add each file to the zip
            for (file_path, &file_name) in list_output_files.iter().zip(names_in_zip.iter()) {
                debug!("Writer for '{}', adding to zip: {}",cell_id, file_path.display());

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
        }
        debug!("Writer got stop signal, now finishing zip");

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