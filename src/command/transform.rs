use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashSet;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use crate::fileformat::fastq::BascetFastqWriterFactory;
use crate::fileformat::tirp::TirpBascetShardReaderFactory;
use crate::fileformat::{CellID, ReadPair};
use crate::fileformat::DetectedFileformat;
use crate::fileformat::try_get_cells_in_file;
use crate::fileformat::ReadPairWriter;
use crate::fileformat::ReadPairReader;

//use crate::fileformat::BascetFastqWriter;
//use crate::fileformat::TirpBascetShardReader;
use crate::fileformat::ConstructFromPath;
use crate::fileformat::ShardCellDictionary;

type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;



pub struct TransformFileParams {

    pub include_cells: Option<Vec<CellID>>,

    pub path_in: Vec<std::path::PathBuf>,
    //pub path_tmp: std::path::PathBuf,
    pub path_out: Vec<std::path::PathBuf>,

}

//Convert from [X] -> [Y], with selection of cells. this enables splitting, merging and subsetting in one command.
//However, this is not quite enough to Shardify, as this requires synchronization between readers
pub struct TransformFile { 
}
impl TransformFile {


    pub fn run(
        params: &Arc<TransformFileParams>
    ) -> anyhow::Result<()> {

        //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
        let include_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {

            let mut all_cells: HashSet<CellID> = HashSet::new();
            for p in &params.path_in {
                if let Some(cells) = try_get_cells_in_file(&p).expect("Failed to parse input file") {
                    all_cells.extend(cells);
                } else {
                    //TODO
                    //make this file just stream the content
                }
            }
            let all_cells: Vec<CellID> = all_cells.iter().cloned().collect();
            all_cells
        };

        //Set up thread pool
        let n_input = params.path_in.len();
        let n_output = params.path_in.len();
        let thread_pool_read = threadpool::ThreadPool::new(n_input); 
        let thread_pool_write = threadpool::ThreadPool::new(n_output); 

        /////////////////////////////// Should probably actually rather use normal threads the way we use them! 
        // i.e. main thread sends request to read one cell. reader picks random input, then writes on random output.
        // if unclear what cells to read then one reader/input, keep reading, and spawn new writers for each cell
        // ...but current approach to threading works too

        // Set up channel for sending data, readers => writers
        let (tx_data, rx_data) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(n_output*2);
        let (tx_data, rx_data) = (Arc::new(tx_data), Arc::new(rx_data));
 
        // Set up channel for telling readers which cells to select
        let (tx_readcell, rx_readcell) = crossbeam::channel::bounded::<Option<CellID>>(n_input);
        let (tx_readcell, rx_readcell) = (Arc::new(tx_readcell), Arc::new(rx_readcell));

        

        // Start reader threads
        for p in &params.path_in {

            let read_thread = match crate::fileformat::detect_shard_format(&p) {
                DetectedFileformat::TIRP => {
                    create_reader_thread( //::<TirpBascetShardReader>
                        &p,
                        &thread_pool_read,
                        &rx_readcell,
                        &tx_data,
                        &Arc::new(TirpBascetShardReaderFactory::new())
                    )
                },
/* 
                DetectedFileformat::ZIP => {

            
                    panic!("TODO")
                },
                DetectedFileformat::FASTQ => {
                    create_writer_thread::<BascetFastqWriter>(
                        &p,
                        &thread_pool_write,
                        &rx_data
                    ).unwrap()
                },
                DetectedFileformat::BAM => {
        
                    //// need separate index file
        
                    panic!("TODO")
                },*/
                _ => { anyhow::bail!("Output file format for {} not supported for this operation", p.display()) }
        
            };

            read_thread.expect("Failed to open input file");       


 
         }

        // Start writer threads
        for p in &params.path_out {
            //let writer = BascetFastqWriter::new(&p).expect("Could not open output fastq file");

            match crate::fileformat::detect_shard_format(&p) {
                /* 
                DetectedFileformat::TIRP => {
                    let mut f = TirpBascetShardReader::new(&p).expect("Unable to open input TIRP file");
                    Ok(Some(f.get_cell_ids().unwrap()))
                },
*/
                DetectedFileformat::ZIP => {
                    /////// Possible to create r1.fq etc inside zip
                    panic!("Storing reads in ZipBascet not implemented. Consider TIRP format instead as it is a more relevant option")
                },
                DetectedFileformat::FASTQ => {
                    create_writer_thread(//::<BascetFastqWriter>(
                        &p,
                        &thread_pool_write,
                        &rx_data,
                        &Arc::new(BascetFastqWriterFactory::new())
                    ).unwrap()
                },
                DetectedFileformat::BAM => {
                    //// need separate index file
                    panic!("Output to BAM/CRAM yet to be implemented for this operation")
                },
                _ => { anyhow::bail!("Output file format for {} not supported for this operation", p.display()) }
        
            }

        }



        // Loop over all cells
        // todo: if cell list provided, need to wait for a reader to have streamed them all
        let mut num_proc_cell = 0;
        let num_total_cell = include_cells.len();
        for cell_id in include_cells {
            //println!("Doing cell {}",cell_id);
            tx_readcell.send(Some(cell_id)).unwrap();
            num_proc_cell+=1;
            if num_proc_cell%1000 == 0 {
                println!("Processed {} / {} cells", num_proc_cell, num_total_cell);
                //println!("Doing cell {}",cell_id);
            }
        }
        println!("Processed a final of {} cells", num_total_cell);

        //Tell all readers to shut down
        for _ in 0..n_input {
            tx_readcell.send(None).unwrap();
        }

        //Wait for reader threads to be done
        thread_pool_read.join();
        
        //Tell all writers to shut down 
        for _ in 0..n_output {
            tx_data.send(None).unwrap();
        }

        //Wait for writer threads to be done
        thread_pool_write.join();


        Ok(())
    }



    
}









//////////////// Writer to any format
fn create_writer_thread<W>(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx_data: &Arc<Receiver<Option<ListReadWithBarcode>>>,
    constructor: &Arc<impl ConstructFromPath<W>+Send+ 'static+Sync>
//    tx_data: &Arc<Sender<Option<ListReadWithBarcode>>>
//    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>, ///////////// possible to keep this if we want!
//    sort: bool, ////////////// if we wanted to use this system for shardifying, would not be impossible to have this flag
//    tempdir: &PathBuf //// not needed, but could be reinstated
) -> anyhow::Result<()> where W: ReadPairWriter  {

    let outfile = outfile.clone();
    let rx_data = Arc::clone(rx_data);
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
        println!("Shutting down writer for {}", outfile.display());


    });
    Ok(())
}













//////////////// Reader from any format
fn create_reader_thread<R>(
    infile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx_readcell: &Arc<Receiver<Option<CellID>>>,
    tx_data: &Arc<Sender<Option<ListReadWithBarcode>>>,
    constructor: &Arc<impl ConstructFromPath<R>+Send+ 'static+Sync>
//    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>, ///////////// possible to keep this if we want!
//    sort: bool, ////////////// if we wanted to use this system for shardifying, would not be impossible to have this flag
//    tempdir: &PathBuf //// not needed, but could be reinstated
) -> anyhow::Result<()> where R: ReadPairReader+ShardCellDictionary {

    let infile = infile.clone();
    let rx_readcell = Arc::clone(rx_readcell);
    let tx_data = Arc::clone(tx_data);
    let constructor = Arc::clone(&constructor);

    thread_pool.execute(move || {
        // Open input file
        println!("Starting reader for {}", infile.display());
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
        println!("Shutting down reader for {}", infile.display());


    });
    Ok(())
}


















