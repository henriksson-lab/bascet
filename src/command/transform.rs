use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashSet;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use anyhow::Result;
use clap::Args;
use crate::fileformat::paired_fastq::PairedFastqStreamingReadPairReaderFactory;
use crate::fileformat::read_cell_list_file;

use crate::fileformat::single_fastq::BascetSingleFastqWriterFactory;
use crate::fileformat::paired_fastq::BascetPairedFastqWriterFactory;
use crate::fileformat::tirp::BascetTIRPWriterFactory;
use crate::fileformat::tirp::TirpBascetShardReaderFactory;
use crate::fileformat::bam::BAMStreamingReadPairReaderFactory;
use crate::fileformat::TirpStreamingReadPairReaderFactory;
use crate::fileformat::{CellID, ReadPair};
use crate::fileformat::DetectedFileformat;
use crate::fileformat::try_get_cells_in_file;
use crate::fileformat::ReadPairWriter;
use crate::fileformat::ReadPairReader;
use crate::fileformat::StreamingReadPairReader;
use crate::fileformat::ConstructFromPath;
use crate::fileformat::ShardCellDictionary;

type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;




#[derive(Args)]
pub struct TransformCMD {
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,
    
}
impl TransformCMD {

    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {

        if self.path_in.is_empty() {
            anyhow::bail!("No input files were specified");
        }
        if self.path_out.is_empty() {
            anyhow::bail!("No output files were specified");
        }

        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };

        //Set up parameters and run the function
        let params = TransformFile {
            path_in: self.path_in.clone(),
            //path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),

            include_cells: include_cells
        };
        
        let _ = TransformFile::run(&Arc::new(params)).expect("tofastq failed");

        log::info!("Transform has finished succesfully");
        Ok(())
    }
}







/// Algorithm:
/// Convert from [X] -> [Y], with selection of cells. this enables splitting, merging and subsetting in one command.
/// Note that this operation cannot shardify, as this requires readers to synchronize and merge data
pub struct TransformFile { 
    pub path_in: Vec<std::path::PathBuf>,
    //pub path_tmp: std::path::PathBuf,
    pub path_out: Vec<std::path::PathBuf>,

    pub include_cells: Option<Vec<CellID>>,
}
impl TransformFile {

    /// Run the algorithm
    pub fn run(
        params: &Arc<TransformFile>
    ) -> anyhow::Result<()> {


        //Set up writer thread pools
        let n_output = params.path_in.len();
        let thread_pool_write = threadpool::ThreadPool::new(n_output); 

        /////////////////////////////// Should probably actually rather use normal threads the way we use them! 
        // i.e. main thread sends request to read one cell. reader picks random input, then writes on random output.
        // if unclear what cells to read then one reader/input, keep reading, and spawn new writers for each cell
        // ...but current approach to threading works too

        // Set up channel for sending data, readers => writers
        let (tx_data, rx_data) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(n_output*2);
        let (tx_data, rx_data) = (Arc::new(tx_data), Arc::new(rx_data));
 

        /////////////////////////////////////////////// Start writer threads
        for p in &params.path_out {
            //let writer = BascetFastqWriter::new(&p).expect("Could not open output fastq file");

            match crate::fileformat::detect_shard_format(&p) {
                DetectedFileformat::ZIP => {
                    panic!("Storing reads in ZipBascet not implemented. Consider TIRP format instead as it is a more relevant option")
                },
                DetectedFileformat::TIRP => {
                    create_writer_thread(
                        &p,
                        &thread_pool_write,
                        &rx_data,
                        &Arc::new(BascetTIRPWriterFactory::new())
                    ).unwrap()
                },
                DetectedFileformat::SingleFASTQ => {
                    create_writer_thread(
                        &p,
                        &thread_pool_write,
                        &rx_data,
                        &Arc::new(BascetSingleFastqWriterFactory::new())
                    ).unwrap()
                },
                DetectedFileformat::PairedFASTQ => {
                    create_writer_thread(
                        &p,
                        &thread_pool_write,
                        &rx_data,
                        &Arc::new(BascetPairedFastqWriterFactory::new())
                    ).unwrap()
                },
                DetectedFileformat::BAM => {
                    //// need separate index file
                    panic!("Output to BAM/CRAM yet to be implemented for this operation")
                },
                _ => { anyhow::bail!("Output file format for {} not supported for this operation", p.display()) }
        
            }

        }


        //Depending on a list of cells is given or not, use random or streaming I/O.
        //Streaming I/O supports more formats and is likely to be faster
        if let Some(_p) = &params.include_cells {
            create_random_readers(
                &params,
                &params.path_in, 
                &tx_data
            )?;

        } else {
            create_stream_readers(
                &params.path_in, 
                &tx_data
            )?;
        }
        
        //Tell all writers to shut down 
        for _ in 0..n_output {
            tx_data.send(None).unwrap();
        }

        //Wait for writer threads to be done
        thread_pool_write.join();


        Ok(())
    }
    
}





pub fn create_random_readers(
    params: &Arc<TransformFile>,
    path_in: &Vec<std::path::PathBuf>,
    tx_data: &Sender<Option<ListReadWithBarcode>>
) -> anyhow::Result<()>{


    //Set up thread pools
    let n_input = path_in.len();
    let thread_pool_read = threadpool::ThreadPool::new(n_input); 

    // Set up channel for telling readers which cells to select
    let (tx_readcell, rx_readcell) = crossbeam::channel::bounded::<Option<CellID>>(n_input);

    //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
    let include_cells = get_list_of_all_cells(&params);

    // Start reader threads -- for reading subset of cells
    for p in &params.path_in {

        let read_thread = match crate::fileformat::detect_shard_format(&p) {
            DetectedFileformat::TIRP => {
                create_random_reader_thread( 
                    &p,
                    &thread_pool_read,
                    &rx_readcell,
                    &tx_data,
                    &Arc::new(TirpBascetShardReaderFactory::new())
                )
            },
            _ => { anyhow::bail!("Input file format for {} not supported for reading of specific cells", p.display()) }
    
        };
        read_thread.expect("Failed to open input file");       
    }

    // Loop over all cells
    // todo: if cell list provided, need to wait for a reader to have streamed them all
    let mut num_proc_cell = 0;
    let num_total_cell = include_cells.len();
    for cell_id in include_cells {
        tx_readcell.send(Some(cell_id)).unwrap();
        num_proc_cell+=1;
        if num_proc_cell%1000 == 0 {
            println!("Processed {} / {} cells", num_proc_cell, num_total_cell);
        }
    }
    println!("Processed a final of {} cells", num_total_cell);

    //Tell all readers to shut down
    for _ in 0..n_input {
        tx_readcell.send(None).unwrap();
    }

    //Wait for reader threads to be done
    thread_pool_read.join();

    Ok(())

}





pub fn create_stream_readers(
    path_in: &Vec<std::path::PathBuf>,
    tx_data: &Sender<Option<ListReadWithBarcode>>
) -> anyhow::Result<()>{

    //Set up thread pools
    let n_input = path_in.len();
    let thread_pool_read = threadpool::ThreadPool::new(n_input); 
    
    // Start reader threads -- for streaming all cells. No separate loop needed to tell them which cells to read out
    for p in path_in {

        let read_thread = match crate::fileformat::detect_shard_format(&p) {
            DetectedFileformat::PairedFASTQ => {
                create_stream_reader_thread( 
                    &p,
                    &thread_pool_read,
                    &tx_data,
                    &Arc::new(PairedFastqStreamingReadPairReaderFactory::new())
                )
            },
            DetectedFileformat::TIRP => {
                create_stream_reader_thread( 
                    &p,
                    &thread_pool_read,
                    &tx_data,
                    &Arc::new(TirpStreamingReadPairReaderFactory::new())
                )
            },
            DetectedFileformat::BAM => {
                create_stream_reader_thread( 
                    &p,
                    &thread_pool_read,
                    &tx_data,
                    &Arc::new(BAMStreamingReadPairReaderFactory::new())
                )
            },
            _ => { anyhow::bail!("Input file format for {} not supported for streaming all content", p.display()) }
    
        };
        read_thread.expect("Failed to open input file");       
    }

    //Wait for all readers to finish
    thread_pool_read.join();

    Ok(())
}








//////////////// Writer to any format
fn create_writer_thread<W>(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx_data: &Receiver<Option<ListReadWithBarcode>>,
    constructor: &Arc<impl ConstructFromPath<W>+Send+ 'static+Sync>
) -> anyhow::Result<()> where W: ReadPairWriter  {

    let outfile = outfile.clone();
    let rx_data = rx_data.clone();
    let constructor = Arc::clone(&constructor);

    thread_pool.execute(move || {
        // Open output file

        println!("Starting writer for {}", outfile.display());
        let mut writer = constructor.new_from_path(&outfile).expect(format!("Failed to create output file {}", outfile.display()).as_str());

        while let Ok(Some(dat)) = rx_data.recv() {

            let cell_id=&dat.0;
            let list_reads = &dat.1;

            writer.write_reads_for_cell(&cell_id, &Arc::clone(list_reads));
        }
        writer.writing_done().unwrap();
        
        println!("Shutting down writer for {}", outfile.display());


    });
    Ok(())
}





////////////////
/// Get the list of all files to process, given by user or by getting names from the files.
/// The latter case is only relevant for compatibility with non-streaming APIs (which likely are slower than streaming APIs)
fn get_list_of_all_cells(
    params: &Arc<TransformFile>
) -> Vec<CellID> {

    //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
    if let Some(p) = &params.include_cells {
        p.clone()
    } else {

        let mut all_cells: HashSet<CellID> = HashSet::new();
        for p in &params.path_in {
            if let Some(cells) = try_get_cells_in_file(&p).expect("Failed to parse input file") {
                all_cells.extend(cells);
            } else {
                panic!("Cannot obtain list of cell names from this input file format. Try streaming the content instead");
            }
        }
        let all_cells: Vec<CellID> = all_cells.iter().cloned().collect();
        all_cells
    }
}







//////////////// 
/// Reader from any format that support reading of random cells
fn create_random_reader_thread<R>(
    infile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx_readcell: &Receiver<Option<CellID>>,
    tx_data: &Sender<Option<ListReadWithBarcode>>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R: ReadPairReader+ShardCellDictionary {

    let infile = infile.clone();
    let rx_readcell = rx_readcell.clone();
    let tx_data = tx_data.clone();
    let constructor = Arc::clone(&constructor);

    thread_pool.execute(move || {
        // Open input file
        println!("Starting random reader for {}", infile.display());
        let mut reader = constructor.new_from_path(&infile).expect(format!("Failed to open input file {}", infile.display()).as_str());

        //A single call to get the list of cells is likely the most efficient way
        let list_cells = reader.get_cell_ids().expect("Could not get list of cells for input file");

        //Handle all cell requests
        while let Ok(Some(cell_id)) = rx_readcell.recv() {

            //Only take requests if the cell is present
            if list_cells.contains(&cell_id) {
                let list_reads = reader.get_reads_for_cell(&cell_id).expect("Failed to get reads from input file");
                let tosend = (cell_id, Arc::clone(&list_reads));    
                tx_data.send(Some(Arc::new(tosend))).unwrap();
            }
        }
        println!("Shutting down random reader for {}", infile.display());
    });
    Ok(())
}








//////////////// 
/// Reader from any format that supports streaming
pub fn create_stream_reader_thread<R>(
    infile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    tx_data: &Sender<Option<ListReadWithBarcode>>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
) -> anyhow::Result<()> where R: StreamingReadPairReader {

    let infile = infile.clone();
    let tx_data = tx_data.clone();
    let constructor = Arc::clone(&constructor);
    thread_pool.execute(move || {
        // Open input file
        println!("Starting streaming reader for {}", infile.display());
        let mut reader = constructor.new_from_path(&infile).expect(format!("Failed to open input file {}", infile.display()).as_str());

        //Handle all cell requests
        let mut num_proc_cell = 0;
        loop {
            let list_reads = reader.get_reads_for_next_cell().expect("Failed to get reads from input file");

            num_proc_cell+=1;
            if num_proc_cell%1000 == 0 {
                println!("Processed {} cells", num_proc_cell);
            }

            if list_reads.is_none() {
                //No more data. End!
                break;
            } else {
                tx_data.send(list_reads).unwrap();
            }
        }
        println!("Processed a final of {} cells", num_proc_cell);
        println!("Shutting down streaming reader for {}", infile.display());
    });
    Ok(())
}

