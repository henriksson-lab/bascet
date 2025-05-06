use std::sync::Arc;
use std::path::PathBuf;

use anyhow::bail;
use log::info;

use crate::fileformat::tirp::TirpBascetShardReaderFactory;
use crate::fileformat::zip::ZipBascetShardReaderFactory;
use crate::fileformat::TirpStreamingShardReaderFactory;

use crate::fileformat;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ShardStreamingFileExtractor;
use crate::fileformat::ConstructFromPath;
use crate::fileformat::detect_shard_format;
use crate::fileformat::DetectedFileformat;


//////////////////////////////////// 
/// General interface to all types of readers, enabling iteration over shard-type files
pub fn iterate_shard_reader_multithreaded(
    threads_read: usize,
    path_in: &PathBuf,
    run_func: &Arc<impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static>
) -> anyhow::Result<()> {

    //Figure out how to read the data
    let input_shard_type = detect_shard_format(&path_in);

    println!("Input file: {:?}",path_in);

    let perform_streaming=input_shard_type == DetectedFileformat::TIRP;

    if perform_streaming {
        ////////////////////////////////// Streaming reading of input
        println!("Reading will be streamed");

        //Create all streaming readers. Detect what we need from the file extension.
        //The readers will start immediately
        let thread_pool_readers = threadpool::ThreadPool::new(threads_read);
        if input_shard_type == DetectedFileformat::TIRP {
            println!("Detected input as TIRP");
            for _tidx in 0..threads_read {  /////////// option #2: keep list of files separately from list of readers
                _ = create_streaming_shard_reader(
                    &path_in,
                    &thread_pool_readers,
                    &Arc::new(TirpStreamingShardReaderFactory::new()),
                    &run_func
                );
            }
        } else {
            bail!("Cannot tell the type of the input format"); /////////////////////////// TODO add support for BAM etc as a shardreader
        }

        //Wait for all reader threads to complete
        thread_pool_readers.join();
        println!("Streaming readers have finished");
        anyhow::Ok(())
    } else {
        ////////////////////////////////// Random reading of input
        println!("Reading will be random (this can be slow depending on file format)");

//            panic!("this need to be rewritten; let readers stream on their own")

        let thread_pool_readers = threadpool::ThreadPool::new(threads_read);

        //Create all random readers. Detect what we need from the file extension
        //let reader_thread_group = ThreadGroup::new(params.threads_read);
        let input_shard_type = detect_shard_format(&path_in);
        if input_shard_type == DetectedFileformat::TIRP {
            println!("Detected input as TIRP");
            for _tidx in 0..threads_read {  /////////// option #2: keep list of files separately from list of readers
                _ = create_random_shard_reader(
                    &path_in,
                    &thread_pool_readers,
                    &Arc::new(TirpBascetShardReaderFactory::new()),
                    &run_func
                );
            }
        } else if input_shard_type == DetectedFileformat::ZIP {
            println!("Detected input as ZIP");
            // note from julian: readers alter the ZIP file? at least make separate readers. start with just 1
            for _tidx in 0..threads_read {
                _ = create_random_shard_reader(
                    &path_in,
                    &thread_pool_readers,
                    &Arc::new(ZipBascetShardReaderFactory::new()),
                    &run_func
                );
            }
        } else {
            bail!("Cannot tell the type of the input format");
        }

        //Wait for all reader threads to complete
        thread_pool_readers.join();
        println!("Random I/O readers have finished");
        anyhow::Ok(())
    }
}



//////////////////////////////////// 
/// Reader for random I/O shard files
fn create_random_shard_reader<R>(
    //params_io: &Arc<MapCell>,
    path_in: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    constructor: &Arc<impl ConstructFromPath<R> + Send + 'static + Sync>,
    run_func: &Arc<impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static>
) -> anyhow::Result<()> where R:ShardRandomFileExtractor+ShardFileExtractor {

    //let params_io = Arc::clone(&params_io);
    let constructor = Arc::clone(constructor);
    let run_func = Arc::clone(run_func);
    let path_in = path_in.clone();

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor.
            new_from_path(&path_in).
            expect("Failed to create bascet reader");

        //Tell readers to go through all cells, then terminate all readers
        let list_cells = fileformat::try_get_cells_in_file(&path_in).expect("Could not get list of cells from input file");
        let list_cells = if let Some(list_cells) = list_cells {
            list_cells
        } else {
            panic!("unable to figure out a list of cells ahead of time; this has not yet been implemented (provide suitable input file format, or manually specify cells)");
        };

        // TODO: each reader manages its own list of cells
        let mut num_cells_processed = 0;
        for cell_id in list_cells { 
            info!("request to read {}",cell_id);
            shard.set_current_cell(&cell_id);

            if num_cells_processed%10 ==0 {
                println!("processed {} cells, now at {}",num_cells_processed, cell_id);
            }

            let mut shard: Box<&mut dyn ShardFileExtractor> = Box::new(&mut shard);
            run_func((
                cell_id,
                &mut shard
            ));



            6666666;


            num_cells_processed += 1;
        }
        println!("Reader ended; read a total of {} cells", num_cells_processed);
    });
    Ok(())
}


//////////////////////////////////// 
/// Reader for streaming I/O shard files
fn create_streaming_shard_reader<R>(
    path_in: &PathBuf,
    //params_io: &Arc<MapCell>,
    thread_pool: &threadpool::ThreadPool,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>,
    run_func: &Arc<impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static>
) -> anyhow::Result<()> where R:ShardStreamingFileExtractor+ShardFileExtractor {

    //let params_io = Arc::clone(&params_io);
    let constructor = Arc::clone(constructor);
    let run_func = Arc::clone(run_func);
    let path_in = path_in.clone();

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor.
            new_from_path(&path_in).
            expect("Failed to create bascet reader");

        let mut num_cells_processed = 0;
        while let Ok(Some(cell_id)) = shard.next_cell() {

            //println!("Starting extraction of {}", num_cells_processed);

            if num_cells_processed%10 ==0 {
                println!("processed {} cells, now at {}",num_cells_processed, cell_id);
            }

            let mut shard: Box<&mut dyn ShardFileExtractor> = Box::new(&mut shard);
            run_func((
                cell_id,
                &mut shard
            ));

            num_cells_processed+=1;
        }
        println!("Reader ended; read a total of {} cells", num_cells_processed);
    });
    Ok(())
}

