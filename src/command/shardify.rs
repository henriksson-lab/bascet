
use log::{debug, info};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::fs::File;
use anyhow::Result;
use clap::Args;

use std::io::{BufWriter, Write};

use crossbeam::channel::Sender;
use crossbeam::channel::Receiver;

use crate::fileformat::read_cell_list_file;
use crate::fileformat::CellID;
use crate::fileformat::ReadPair;
use crate::fileformat::TirpBascetShardReader;
use crate::fileformat::ShardCellDictionary;
use crate::fileformat::ReadPairReader;
use crate::fileformat::tirp;
use crate::fileformat::shard;


type ListReadPair = Arc<Vec<ReadPair>>;
type MergedListReadWithBarcode = Arc<(CellID,Vec<ListReadPair>)>;


pub const DEFAULT_PATH_TEMP: &str = "temp";

/// Commandline option: Take parsed reads and organize them as shards

#[derive(Args)]
pub struct ShardifyCMD {
    // Input bascets (comma separated; ok with PathBuf???)
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    // File with a list of cells to include
    #[arg(long = "cells")]
    pub include_cells: Option<PathBuf>,

}
impl ShardifyCMD {

    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {

        //Read optional list of cells
        let include_cells = if let Some(p) = &self.include_cells {
            let name_of_cells = read_cell_list_file(&p);
            Some(name_of_cells)
        } else {
            None
        };

        //Set up parameters and run the function
        let params = Shardify {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
            include_cells: include_cells
        };
        
        let _ = Shardify::run(Arc::new(params)).expect("shardify failed");

        log::info!("Shardify has finished succesfully");
        Ok(())
    }
}






/// Algorithm: Take parsed reads and organize them as shards
#[derive(Clone,Debug)]
pub struct Shardify {
    pub path_in: Vec<std::path::PathBuf>,
    pub path_tmp: std::path::PathBuf,
    pub path_out: Vec<std::path::PathBuf>,

    pub include_cells: Option<Vec<CellID>>,
}
impl Shardify {





    //If there are many input files then it is better to implement
    //    TirpStreamingReadPairReader
    //and just filter out irrelevant files

    //zorn should optimize which TIRPs go where at early stage. avoid sending files everywhere such that reading can be kept fairly
    //local




    /// Run the algorithm -- randomized access mode
    pub fn run(
        params: Arc<Shardify>
    ) -> anyhow::Result<()> {

        info!("Running command: shardify");


        // https://github.com/zaeleus/noodles/blob/master/noodles-tabix/examples/tabix_write.rs
        // noodles writing index while writing bed-like file
        // noodles can read, and get virtual position, https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/reader.rs
        // multithreaded reader can also get virtual position https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/multithreaded_reader.rs

        if false {
            crate::utils::check_tabix().expect("tabix not found");  /////// can we write tabix files with rust?? bgzip is possible
            println!("Required software is in place");
        }


        //Open up readers for all input tabix files
        println!("opening input files");
        let mut list_input_files: Vec<TirpBascetShardReader> = Vec::new();
        for p in params.path_in.iter() {
            println!("{}",p.display());
            let reader = TirpBascetShardReader::new(&p).expect("Unable to open input file");
            list_input_files.push(reader);
        }

        //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
        println!("getting list of cells");
        let include_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {
            let mut all_cells: HashSet<CellID> = HashSet::new();
            for f in list_input_files.iter_mut() {
                all_cells.extend(f.get_cell_ids().unwrap());
            }
            let all_cells: Vec<CellID> = all_cells.iter().cloned().collect();
            all_cells
        };
        println!("Read # cells: {}",include_cells.len());

        //Ensure that the cells are sorted. This likely increases the read locality
        let mut include_cells = include_cells.clone();
        include_cells.sort();

        //Create queue: reads going to main thread, for concatenation
        //Limit how many chunks can be in pipe
        let (tx_write_seq, rx_write_seq) = crossbeam::channel::unbounded::<ListReadPair>();  

        //Create queue: concatenated reads going to writers
        //Limit how many chunks can be in pipe
        let (tx_write_cat, rx_write_cat) = crossbeam::channel::bounded::<Option<MergedListReadWithBarcode>>(30);  

        //Start writer threads, one per shard requested
        let thread_pool_write = threadpool::ThreadPool::new(params.path_out.len());
        for p in params.path_out.iter() {
            let _ = create_writer_thread(
                &p,
                &thread_pool_write,
                &rx_write_cat
            ).unwrap();
        }

        //Create queue: cellid's to be read. we will use this as a broadcaster, one cellid at a time
        let (tx_request_cell, rx_request_cell) = crossbeam::channel::unbounded::<Option<CellID>>();  

        //Prepare reader threads, one per input file
        println!("Starting reader threads");
        let thread_pool_read = threadpool::ThreadPool::new(params.path_in.len());  
        for p in params.path_in.iter() {
            let _ = create_reader_thread(
                &p,
                &thread_pool_read,
                &rx_request_cell,
                &tx_write_seq
            ).unwrap();
        }

        //Loop through all the cells that we want
        println!("Starting to write output");
        let n_input = params.path_in.len();
        let n_output = params.path_out.len();
        for cellid in include_cells {

            println!("cell: {}", &cellid);

            //Ask each input thread to provide reads
            for _ in 0..n_input {
                tx_request_cell.send(Some(cellid.clone())).unwrap();
            }

            //Collect reads from each input file
            let mut all_reads_list: Vec<ListReadPair> = Vec::new();
            for _ in 0..n_input {
                let r =rx_write_seq.recv().expect("Failed to get data from input reader");
                debug!("sending {}", r.len());
                all_reads_list.push(r);
            }

            //Send reads to some write thread.
            //Assume that a random thread picks it up, which should lead to output shards of about the same size
            _ = tx_write_cat.send(Some(Arc::new((cellid, all_reads_list))));
        }

        //Tell all readers to shut down
        for _ in 0..n_input {
            tx_request_cell.send(None).unwrap();
        }

        //Tell all writers to shut down
        for _ in 0..n_output {
            tx_write_cat.send(None).unwrap();
        }

        //Wait for threads to be done
        thread_pool_read.join();
        thread_pool_write.join();

        println!("Done shardifying");

        Ok(())
    }
    
}






//////////////// Writer to TIRP format, taking multiple blocks of reads for the same cell
fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx: &Receiver<Option<MergedListReadWithBarcode>>
) -> anyhow::Result<()> {

    let outfile = outfile.clone();
    let rx=rx.clone();
    let outfile_keep = outfile.clone();

    thread_pool.execute(move || {
        // Open output file
        println!("Creating pre-TIRP output file: {}",outfile.display());
        debug!("starting write loop");


        let mut hist = shard::BarcodeHistogram::new();
        let file_output = File::create(&outfile).unwrap();   
        let writer=BufWriter::new(file_output);

        let mut writer = noodles_bgzf::MultithreadedWriter::new(writer);
        //1.0.0 · Source
        //fn write_all(&mut self, buf: &[u8]) -> Result<(), Error>  is implemented

        // Write reads
        let mut n_read_written=0;
        let mut n_cell_written=0;
        while let Ok(Some(entry)) = rx.recv() {

            let cellid = &entry.0;
            let list_of_list_pairs = &entry.1;

            let mut tot_reads_for_cell:u64 = 0;
            for list_pairs in list_of_list_pairs {
                for rp in list_pairs.iter() {
                    tirp::write_records_pair_to_tirp( //::<File>
                        &mut writer, 
                        &cellid, 
                        &rp
                    );
                }
                tot_reads_for_cell = tot_reads_for_cell + list_pairs.len() as u64;
            }
            n_read_written += tot_reads_for_cell;
            n_cell_written += 1;
            hist.inc_by(&cellid, &tot_reads_for_cell);

            if n_cell_written % 1000 == 0 {
                println!("#reads written to outfile: {:?}\t#cells written {:?}", n_read_written, n_cell_written);
            }
        }
    
        //absolutely have to call this before dropping, for bufwriter
        _ = writer.flush(); 

        //Write histogram
        debug!("Writing histogram");
        let hist_path = tirp::get_histogram_path_for_tirp(&outfile);
        hist.write_file(&hist_path).expect("Failed to write histogram");

        //// Index the final file with tabix  
        println!("Indexing final output file");
        tirp::index_tirp(&outfile_keep).expect("Failed to index output file");
        

    });
    Ok(())
}














//////////////// Reader from TIRP format
fn create_reader_thread(
    infile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx_get_cellid: &Receiver<Option<CellID>>,
    tx_send_reads: &Sender<ListReadPair>
) -> anyhow::Result<()> {

    let infile = infile.clone();
    let tx_send_reads=tx_send_reads.clone();
    let rx_get_cellid=rx_get_cellid.clone();

    thread_pool.execute(move || {

        // Open input file
        println!("Opening pre-TIRP input file: {}",infile.display());
        debug!("starting read loop");
        let mut reader = tirp::TirpBascetShardReader::new(&infile).expect("Could not open input file");

        // Read reads for each cell requested
        while let Ok(Some(cell_id)) = rx_get_cellid.recv() {
            let reads = if reader.has_cell(&cell_id) {
                reader.get_reads_for_cell(&cell_id).expect("Failed to read from input file")
            } else {
                Arc::new(Vec::new())
            };
            _ = tx_send_reads.send(Arc::clone(&reads));
        }
        debug!("stopping read loop");
    });
    Ok(())
}








#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn performance_tirp() {



        //Random reading of first 10 cells vs 


        //assert_eq!(cnt, 2);

    }



}