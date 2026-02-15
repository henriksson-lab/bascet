use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::bail;
use bascet_core::DEFAULT_SIZEOF_ARENA;
use bytesize::ByteSize;
use log::info;

use crate::fileformat::ReadPair;
use crate::fileformat::inmem_readpairs::ShardFileExtractorInmem;
use crate::fileformat::tirp::TirpBascetShardReaderFactory;
use crate::fileformat::zip::ZipBascetShardReaderFactory;

use crate::fileformat;
use crate::fileformat::ConstructFromPath;
use crate::fileformat::DetectedFileformat;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::detect_shard_format;



use bascet_core::{
    attr::{meta::*, sequence::*, quality::*},
    *,
};



/// 
/// General interface to all types of readers, enabling iteration over shard-type files
/// 
pub fn iterate_shard_reader_multithreaded(
    threads_read: usize,
    path_in: &PathBuf,
    run_func: &Arc<
        impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static,
    >,
) -> anyhow::Result<()> {
    //Figure out how to read the data
    let input_shard_type = detect_shard_format(&path_in);

    println!("Input file: {:?}", path_in);

    let perform_streaming = input_shard_type == DetectedFileformat::TIRP;

    if perform_streaming {
        ////////////////////////////////// Streaming reading of input
        println!("Reading will be streamed");

        //Create all streaming readers. Detect what we need from the file extension.
        //The readers will start immediately
        let thread_pool_readers = threadpool::ThreadPool::new(threads_read);
        if input_shard_type == DetectedFileformat::TIRP {
            println!("Detected input as TIRP");
            for _tidx in 0..threads_read {
                /////////// option #2: keep list of files separately from list of readers
                _ = create_streaming_tirp_reader( ///////////////////////////////////////////////////// Note: Use Bascet 2.x TIRP-specific streamer here
                    &path_in,
                    &thread_pool_readers,
                    &run_func,
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

        //Figure out what cells there are to process - get all of them by default
        let list_cells = fileformat::try_get_cells_in_file(&path_in)
            .expect("Could not get list of cells from input file");
        let list_cells = if let Some(list_cells) = list_cells {
            list_cells
        } else {
            panic!(
                "unable to figure out a list of cells ahead of time; this has not yet been implemented (provide suitable input file format, or manually specify cells)"
            );
            //Could revert to streaming here
        };
        let list_cells = list_cells.clone();
        let mut list_cells = Arc::new(Mutex::new(list_cells));

        //            panic!("this need to be rewritten; let readers stream on their own")

        let thread_pool_readers = threadpool::ThreadPool::new(threads_read);

        //Create all random readers. Detect what we need from the file extension
        //let reader_thread_group = ThreadGroup::new(params.threads_read);
        let input_shard_type = detect_shard_format(&path_in);
        if input_shard_type == DetectedFileformat::TIRP {
            println!("Detected input as TIRP");
            for _tidx in 0..threads_read {
                /////////// option #2: keep list of files separately from list of readers
                _ = create_random_shard_reader(
                    &path_in,
                    &thread_pool_readers,
                    &Arc::new(TirpBascetShardReaderFactory::new()), ////////////////////////////////////////////////////// bug! decide on a subset of cells before entering, or make a mutex list
                    &run_func,
                    &mut list_cells,
                );
            }
        } else if input_shard_type == DetectedFileformat::ZIP {
            println!("Detected input as ZIP");
            // note from julian: readers alter the ZIP file? at least make separate readers. start with just 1
            for _tidx in 0..threads_read {
                _ = create_random_shard_reader(
                    &path_in,
                    &thread_pool_readers,
                    &Arc::new(ZipBascetShardReaderFactory::new()), ////////////////////////////////////////////////////// bug! decide on a subset of cells before entering, or make a mutex list
                    &run_func,
                    &mut list_cells,
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









/// 
/// Reader for random I/O shard files
/// 
fn create_random_shard_reader<R>(
    path_in: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    constructor: &Arc<impl ConstructFromPath<R> + Send + 'static + Sync>,
    run_func: &Arc<
        impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static,
    >,
    list_cells: &mut Arc<Mutex<Vec<String>>>,
) -> anyhow::Result<()>
where
    R: ShardRandomFileExtractor + ShardFileExtractor,
{
    let constructor = Arc::clone(constructor);
    let run_func = Arc::clone(run_func);
    let path_in = path_in.clone();
    let list_cells = list_cells.clone();

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor
            .new_from_path(&path_in)
            .expect("Failed to create bascet reader");

        //For all cells in queue
        let mut num_cells_processed = 0; // could have a global counter
        loop {
            //Try to get a cell to process from the queue
            let cell_id = {
                let mut list_cells = list_cells.lock().unwrap();
                list_cells.pop().clone()
            };

            //Process the cell if it exists
            if let Some(cell_id) = cell_id {
                info!("request to read {}", cell_id);
                shard.set_current_cell(&cell_id);

                if num_cells_processed % 10 == 0 {
                    println!(
                        "processed {} cells, now at {}",
                        num_cells_processed, cell_id
                    );
                }

                let mut shard: Box<&mut dyn ShardFileExtractor> = Box::new(&mut shard);
                run_func((cell_id, &mut shard));

                num_cells_processed += 1;
            } else {
                break;
            }
        }
        println!(
            "Reader ended; read a total of {} cells",
            num_cells_processed
        );
    });
    Ok(())
}


/*
/// 
/// Reader for streaming I/O shard files --- not used right now but could be readded if alternatives to TIRP added
/// 
fn create_streaming_shard_reader<R>(
    path_in: &PathBuf,
    //params_io: &Arc<MapCell>,
    thread_pool: &threadpool::ThreadPool,
    constructor: &Arc<impl ConstructFromPath<R> + Send + 'static + Sync>,
    run_func: &Arc<
        impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static,
    >,
) -> anyhow::Result<()>
where
    R: ShardStreamingFileExtractor + ShardFileExtractor,
{
    let constructor = Arc::clone(constructor);
    let run_func = Arc::clone(run_func);
    let path_in = path_in.clone();

    thread_pool.execute(move || {
        println!("Reader started");

        let mut shard = constructor
            .new_from_path(&path_in)
            .expect("Failed to create bascet reader");

        let mut num_cells_processed = 0;
        while let Ok(Some(cell_id)) = shard.next_cell() {
            //println!("Starting extraction of {}", num_cells_processed);

            if num_cells_processed % 10 == 0 {
                println!(
                    "processed {} cells, now at {}",
                    num_cells_processed, cell_id
                );
            }

            let mut shard: Box<&mut dyn ShardFileExtractor> = Box::new(&mut shard);
            run_func((cell_id, &mut shard));

            num_cells_processed += 1;
        }
        println!(
            "Reader ended; read a total of {} cells",
            num_cells_processed
        );
    });
    Ok(())
}


 */











/// 
/// Reader for streaming I/O on new Bascet 2.x TIRP files
/// 
fn create_streaming_tirp_reader(
    path_in: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    //constructor: &Arc<impl ConstructFromPath<R> + Send + 'static + Sync>,
    run_func: &Arc<
        impl Fn((String, &mut Box<&mut dyn ShardFileExtractor>)) + Sync + Send + 'static,
    >,
) -> anyhow::Result<()> {
    let path_in = path_in.clone();
    let run_func = Arc::clone(run_func);

    thread_pool.execute(move || {

        // Streamer from input TIRP
        let num_threads=bounded_integer::BoundedU64::new(5).unwrap(); //////////////////////////// parameter is made up TODO
        let sizeof_stream_arena=DEFAULT_SIZEOF_ARENA;
        let sizeof_stream_buffer:ByteSize = ByteSize::gib(4);  //////////////////////////// parameter is made up TODO
        let decoder: bascet_io::BBGZDecoder = bascet_io::codec::BBGZDecoder::builder()
            .with_path(&path_in)
            .countof_threads(num_threads)
            .build();
        let parser = bascet_io::parse::Tirp::builder().build();

        let mut stream = bascet_core::Stream::builder()
            .with_decoder(decoder)
            .with_parser(parser)
            .sizeof_decode_arena(sizeof_stream_arena)
            .sizeof_decode_buffer(sizeof_stream_buffer)
            .build();

        let mut query = stream.query::<bascet_io::tirp::Record>();
        
        //Handle all cell requests
        let mut num_proc_cell: u64 = 0;
        let mut last_cellid = Vec::new();
        let mut cur_rps:Vec<ReadPair> = Vec::new();

        loop {
            match query.next_into::<bascet_io::tirp::Record>() {
                Ok(Some(record)) => {
                    let record_id = *record.get_ref::<Id>();
                    let record_r1 = *record.get_ref::<R1>();
                    let record_r2 = *record.get_ref::<R2>();
                    let record_q1 = *record.get_ref::<Q1>();
                    let record_q2 = *record.get_ref::<Q2>();
                    let record_umi = *record.get_ref::<Umi>();

                    let rp = ReadPair {
                        r1: record_r1.to_vec(),
                        r2: record_r2.to_vec(),
                        q1: record_q1.to_vec(),
                        q2: record_q2.to_vec(),
                        umi: record_umi.to_vec(),
                    };

                    //Send records to process if we got them all
                    if record_id != last_cellid.as_slice() {
                        if cur_rps.len() > 0 {
                            let prev_cur_rps=cur_rps;
                            cur_rps=Vec::new();
                            let cellid = String::from_utf8_lossy(last_cellid.as_slice());

                            let mut dat = ShardFileExtractorInmem {
                                cellid: cellid.to_string(),
                                rp: prev_cur_rps,
                            };
                            run_func((cellid.to_string(), &mut Box::new(&mut dat)));
                        } else {
                            num_proc_cell += 1;
                            if num_proc_cell % 1000 == 0 {
                                println!("Processed {} cells", num_proc_cell);
                            }
                        }
                        last_cellid = record_id.to_vec();
                    }
                    cur_rps.push(rp);                    
                }
                Ok(None) => {
                        break;
                }
                Err(e) => {
                    panic!("{:?}", e);
                }
            }
        }

        //Send final records to process
        if cur_rps.len() > 0 {
            let cellid = String::from_utf8_lossy(last_cellid.as_slice());
            let mut dat = ShardFileExtractorInmem {
                cellid: cellid.to_string(),
                rp: cur_rps,
            };
            run_func((cellid.to_string(), &mut Box::new(&mut dat)));

            num_proc_cell += 1;
        } 

        println!("Processed a final of {} cells", num_proc_cell);
        println!("Shutting down streaming reader for {}", path_in.display());
    });

    Ok(())
}
