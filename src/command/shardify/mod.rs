
use log::{debug, info};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use std::io::{BufWriter, Write};

use crossbeam::channel::Sender;
use crossbeam::channel::Receiver;


use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ShardReader;
use crate::fileformat::tirp::TirpBascetShardReader;

use crate::fileformat::tirp;
use crate::fileformat::shard;
use crate::fileformat::shard::ReadPair;

use std::fs::File;




#[derive(Clone,Debug)]
pub struct ShardifyParams {
    pub path_in: Vec<std::path::PathBuf>,
    pub path_tmp: std::path::PathBuf,
    pub path_out: Vec<std::path::PathBuf>,

    //
    pub include_cells: Option<Vec<CellID>>,

}






pub struct Shardify {}

impl Shardify {
    pub fn run(
        params: Arc<ShardifyParams>
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
        let mut list_input_files: Vec<TirpBascetShardReader> = Vec::new();
        for p in params.path_in.iter() {
            list_input_files.push(TirpBascetShardReader::new(&p).expect("Unable to open input file"));
        }

        //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
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

        //Ensure that the cells are sorted. This likely increases the read locality
        let mut include_cells = include_cells.clone();
        include_cells.sort();

        //Create queue: reads going to main thread, for concatenation
        //Limit how many chunks can be in pipe
        let (tx_write_seq, rx_write_seq) = crossbeam::channel::unbounded::<ListReadPair>();  
        let (tx_write_seq, rx_write_seq) = (Arc::new(tx_write_seq), Arc::new(rx_write_seq));

        //Create queue: concatenated reads going to writers
        //Limit how many chunks can be in pipe
        let (tx_write_cat, rx_write_cat) = crossbeam::channel::bounded::<Option<MergedListReadWithBarcode>>(30);  
        let (tx_write_cat, rx_write_cat) = (Arc::new(tx_write_cat), Arc::new(rx_write_cat));

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
        let (tx_request_cell, rx_request_cell) = (Arc::new(tx_request_cell), Arc::new(rx_request_cell));

        //Prepare reader threads, one per input file
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
        debug!("Starting to write output");
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










type ListReadPair = Arc<Vec<ReadPair>>;
type MergedListReadWithBarcode = Arc<(CellID,Vec<ListReadPair>)>;


//////////////// Writer to TIRP format, taking multiple blocks of reads for the same cell
fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    rx: &Arc<Receiver<Option<MergedListReadWithBarcode>>>
) -> anyhow::Result<()> {

    let outfile = outfile.clone();
    let rx=Arc::clone(rx);
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
        let mut n_written=0;
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
            n_written += tot_reads_for_cell;
            hist.inc_by(&cellid, &tot_reads_for_cell);
            println!("#reads written to outfile: {:?}", n_written);
        }
    
        //absolutely have to call this before dropping, for bufwriter
        _ = writer.flush(); 

        //Write histogram
        debug!("Writing histogram");
        let hist_path = tirp::get_histogram_path_for_tirp(&outfile);
        hist.write(&hist_path).expect("Failed to write histogram");

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
    rx_get_cellid: &Arc<Receiver<Option<CellID>>>,
    tx_send_reads: &Arc<Sender<ListReadPair>>

) -> anyhow::Result<()> {

    let infile = infile.clone();
    let tx_send_reads=Arc::clone(tx_send_reads);
    let rx_get_cellid=Arc::clone(rx_get_cellid);

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
                Vec::new()
            };
            _ = tx_send_reads.send(Arc::new(reads));
        }
        debug!("stopping read loop");
    });
    Ok(())
}














