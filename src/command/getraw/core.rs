// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use log::{debug, info};
use seq_io::fastq::OwnedRecord;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use crossbeam::channel::Sender;
use crossbeam::channel::Receiver;
use std::io::{BufWriter, Write, Read};

use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

use super::io;

use crate::barcode::Chemistry;

use crate::fileformat::tirp;
use crate::fileformat::shard;
use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;





pub struct GetRawParams {
    pub path_tmp: std::path::PathBuf,

    pub path_forward: std::path::PathBuf,
    pub path_reverse: std::path::PathBuf,
    pub path_output_complete: std::path::PathBuf,
    pub path_output_incomplete: std::path::PathBuf,

    pub sort: bool,

    pub threads_work: usize,   

}




#[derive(Debug,Clone)]
struct RecordPair {
    reverse_record: OwnedRecord,
    forward_record: OwnedRecord
}

type ListReadWithBarcode = Arc<Vec<(ReadPair,CellID)>>;
type ListRecordPair = Arc<Vec<RecordPair>>;







///// loop for a writer thread, sending output to a Writer
pub fn loop_tirp_writer<W>(  
    rx: &Arc<Receiver<Option<ListReadWithBarcode>>>,
    hist: &mut shard::BarcodeHistogram,
    writer: W 
) where W:Write {

    let mut writer= BufWriter::new(writer);

    // Write reads
    let mut n_written=0;
    while let Ok(Some(list_pairs)) = rx.recv() {
        for (bam_cell, cell_id) in list_pairs.iter() {

            tirp::write_records_pair_to_tirp( //::<W>
                &mut writer, 
                &cell_id, 
                &bam_cell
            );

            hist.inc(&cell_id);

            if n_written%100000 == 0 {
                println!("#reads written to outfile: {:?}", n_written);
            }
            n_written = n_written + 1;
        }
    }

    //absolutely have to call this before dropping, for bufwriter
    _ = writer.flush(); 

}





//////////////// Writer to TIRP format, sorting on the fly
fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>,
    sort: bool,
    tempdir: &PathBuf
) -> anyhow::Result<Arc<Sender<Option<ListReadWithBarcode>>>> {

    let outfile = outfile.clone();

    let list_hist = Arc::clone(list_hist);
    let tempdir = tempdir.clone();
    //Create input read queue
    //Limit how many chunks can be in pipe
    let (tx, rx) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(100);  
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));

    thread_pool.execute(move || {
        // Open output file
        println!("Creating pre-TIRP output file: {}",outfile.display());

        let file_output = File::create(outfile).unwrap();   

        let mut hist = shard::BarcodeHistogram::new();

        if sort {

            //Pipe to sort, then to given file
            let mut cmd = Command::new("sort");
            cmd.arg(format!("--temporary-directory={}", tempdir.display())); 

            let mut process = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::from(file_output))  
                .spawn().expect("failed to start sorter");
            
            let mut stdin = process.stdin.as_mut().unwrap();

            debug!("sorter process ready");
            loop_tirp_writer(&rx, &mut hist, &mut stdin);

            //Wait for process to finish
            debug!("Waiting for sorter process to exit");
            let _result = process.wait().unwrap();

            //todo how to terminate pipe?? seems to happen now

        } else {

            debug!("starting non-sorted write loop");

            let mut writer=BufWriter::new(file_output);  //TODO  put in a buffered writer in loop. no need to do twice
            loop_tirp_writer(&rx, &mut hist, &mut writer);
            _ = writer.flush();

        }

        //Keep histogram for final summary
        {
            let mut list_hist = list_hist.lock().unwrap();//: Mutex<Vec<shard::BarcodeHistogram>>
            list_hist.push(hist);
        }
//        _ = tx_hist.send(Arc::new(hist));

    });
    Ok(tx)
}











pub struct GetRaw {}

impl GetRaw {
    pub fn getraw<'a>(
        params_io: Arc<GetRawParams>,
        barcodes: &mut (impl Chemistry+Clone+Send+'static)
    ) -> anyhow::Result<()> {

        info!("Running command: getraw");
        println!("Will sort: {}", params_io.sort);

        if false {
            crate::utils::check_bgzip().expect("bgzip not found");
            crate::utils::check_tabix().expect("tabix not found");
            println!("Required software is in place");
        }


        //Make temp dir
        _ = fs::create_dir(&params_io.path_tmp);

        // Dispatch barcodes (presets + barcodes -> Vec<Barcode>)
        //let mut barcodes = AtrandiChemistry::new();

        // Open fastq files
        let mut forward_file = io::open_fastq(&params_io.path_forward);
        let mut reverse_file = io::open_fastq(&params_io.path_reverse);

        // Find probable barcode starts and ends
        barcodes.prepare(&mut forward_file, &mut reverse_file).expect("Failed to detect barcode setup from reads");
        let mut forward_file = io::open_fastq(&params_io.path_forward); // reopen the file to read from beginning
        let mut reverse_file = io::open_fastq(&params_io.path_reverse); // reopen the file to read from beginning

        // Start writer threads
        let path_temp_complete_sorted = params_io.path_tmp.join(PathBuf::from("tmp_sorted_complete.bed"));
        let path_temp_incomplete_sorted = params_io.path_tmp.join(PathBuf::from("tmp_sorted_incomplete.bed"));

        let list_hist_complete = Arc::new(Mutex::new(Vec::<shard::BarcodeHistogram>::new()));
        let list_hist_incomplete = Arc::new(Mutex::new(Vec::<shard::BarcodeHistogram>::new()));


        let thread_pool_write = threadpool::ThreadPool::new(2);
        let tx_writer_complete = create_writer_thread(
            &path_temp_complete_sorted, 
            &thread_pool_write, 
            &list_hist_complete,
            true,
            &params_io.path_tmp).
            expect("Failed to get writer threads");
        let tx_writer_incomplete = create_writer_thread(
            &path_temp_incomplete_sorted, 
            &thread_pool_write,
            &list_hist_incomplete,
             false,
             &params_io.path_tmp).
             expect("Failed to get writer threads");

        // Start worker threads.
        // Limit how many chunks can be in the air at the same time, as writers must be able to keep up with the reader
        let thread_pool_work = threadpool::ThreadPool::new(params_io.threads_work);
        let (tx, rx) = crossbeam::channel::bounded::<Option<ListRecordPair>>(100);   
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));        
        for tidx in 0..params_io.threads_work {
            let rx = Arc::clone(&rx);
            let tx_writer_complete=Arc::clone(&tx_writer_complete);
            let tx_writer_incomplete=Arc::clone(&tx_writer_incomplete);

            println!("Starting worker thread {}",tidx);

            let mut barcodes = barcodes.clone(); //This is needed to keep mutating the pattern in this structure

            thread_pool_work.execute(move || {

                while let Ok(Some(list_bam_cell)) = rx.recv() {
                    let mut pairs_complete: Vec<(ReadPair, CellID)> = Vec::with_capacity(list_bam_cell.len());
                    let mut pairs_incomplete: Vec<(ReadPair, CellID)> = Vec::with_capacity(list_bam_cell.len());

                    for bam_cell in list_bam_cell.iter() {


                        let (is_ok, cellid, readpair) = barcodes.detect_barcode_and_trim(
                            &bam_cell.reverse_record.seq(),
                            &bam_cell.reverse_record.qual(),
                            &bam_cell.forward_record.seq(),
                            &bam_cell.forward_record.qual()
                        );

                        if is_ok {
                            pairs_complete.push((readpair, cellid));
                        } else {
                            pairs_incomplete.push((readpair, cellid));
                        }
                    }

                let _ = tx_writer_complete.send(Some(Arc::new(pairs_complete)));
                let _ = tx_writer_incomplete.send(Some(Arc::new(pairs_incomplete)));
                }
            });
        }

        // Read the fastq files, send to worker threads
        println!("Starting to read input file");
        read_all_reads(
            &mut forward_file,
            &mut reverse_file,
            &tx
        );

        // Send termination signals to workers, then wait for them to complete
        for _ in 0..params_io.threads_work {
            let _ = tx.send(None);
        }
        thread_pool_work.join();
        
        // Send termination signals to writers, then wait for them to complete
        let _ = tx_writer_complete.send(None);
        let _ = tx_writer_incomplete.send(None);
        thread_pool_write.join();


        //Sort the complete output files and compress the output.
        let mut list_inputfiles:Vec<PathBuf> = Vec::new(); 
        list_inputfiles.push(path_temp_complete_sorted.clone());
        catsort_files(
            &list_inputfiles, 
            &params_io.path_output_complete, 
            params_io.sort
        );

        //// Concatenate also the incomplete reads. Sorting never needed
        let mut list_inputfiles:Vec<PathBuf> = Vec::new(); 
        list_inputfiles.push(path_temp_incomplete_sorted.clone());
        catsort_files(
            &list_inputfiles, 
            &params_io.path_output_incomplete, 
            false
        );

        //// Index the final file with tabix  
        println!("Indexing final output file");
        tirp::index_tirp(&params_io.path_output_complete).expect("Failed to index file");

        //// Store histogram
        println!("Storing histogram for final output file");
        debug!("Collecting histograms");
        sum_and_store_histogram(
            &list_hist_complete,
            &tirp::get_histogram_path_for_tirp(&params_io.path_output_complete)
        );
        sum_and_store_histogram(
            &list_hist_incomplete,
            &tirp::get_histogram_path_for_tirp(&params_io.path_output_incomplete)
        );

        //// Remove temp files
        debug!("Removing temp files");
        _ = fs::remove_dir_all(&params_io.path_tmp);

        info!("done!");

        Ok(())
    }
}



pub fn sum_and_store_histogram(
    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>,
    path: &PathBuf
) {
    debug!("Collecting histograms");

    let list_hist = list_hist.lock().unwrap();

    let mut totalhist = shard::BarcodeHistogram::new();
    for one_hist in list_hist.iter() {
        //let one_hist = rx.recv().expect("Could not get one histrogram");
        totalhist.add_histogram(&one_hist);
    }
    totalhist.write(&path).expect(format!("Failed to write histogram to {:?}", path).as_str());
}






////////// read the reads, send to next threads
fn read_all_reads(
    forward_file: &mut FastqReader<Box<dyn Read>>,
    reverse_file: &mut FastqReader<Box<dyn Read>>,
    tx: &Arc<Sender<Option<ListRecordPair>>>
){
    let mut num_read = 0;
    loop {

        //Read out chunks. By sending in blocks, we can
        //1. keep threads asleep until they got enough work to do to motivate waking them up
        //2. avoid send operations, which likely aren't for free
        let chunk_size = 1000;

        let mut curit = 0;
        let mut list_recpair:Vec<RecordPair> = Vec::with_capacity(chunk_size);
        while curit<chunk_size {
            if let Some(record) = reverse_file.next() {
                let reverse_record: seq_io::fastq::RefRecord<'_> = record.expect("Error reading record rev");
                let forward_record = forward_file.next().unwrap().expect("Error reading record fwd");

                let recpair = RecordPair {
                    reverse_record: reverse_record.to_owned_record(),
                    forward_record: forward_record.to_owned_record()
                };  
                list_recpair.push(recpair);

                num_read = num_read + 1;

                if num_read % 100000 == 0 {
                    println!("read: {:?}", num_read);
                }
    
            } else {
                break;
            }
            curit = curit + 1;
        }

        if !list_recpair.is_empty() {
            let _ = tx.send(Some(Arc::new(list_recpair)));    
        } else {
            break;
        }
    }

}



/// Concatenate or merge sort files, then process them with bgzip
// sort --merge  some_sorted.pregascet.0 some_sorted.pregascet.1 ... | bgzip -c /dev/stdin > test.gascet.0.gz    
//
// we could also skip bgsort but it should be fast; we need to recompress it later otherwise. but it is a huge gain in ratio and need for temporary space!
// also, output file is ready for use unless merging with other shards is needed
//
//  later index: tabix -p bed test.gascet.0.gz   
// able to get a cell: tabix out_complete.0.gascet.gz A1_H5_D9_H12 | head
pub fn catsort_files(
    list_inputfiles: &Vec<PathBuf>, 
    path_final: &PathBuf, 
    sort: bool
) {
    let use_bgzip = true;

    //Final destination file
    let file_final_output = File::create(path_final).unwrap();
    println!("Compressing and writing final output file: {:?}    from input files {:?}",path_final, list_inputfiles);

    //Compress on the fly with bgzip, pipe output to a file
    let mut process_b = if use_bgzip {
        let mut process_b = Command::new("bgzip");
        process_b.arg("-c").arg("/dev/stdin");
        process_b
    } else {
        // for testing on osx without bgzip
        print!("Warning: using gzip for final file. This will not work with tabix later. Not recommended");
        Command::new("gzip")
    };
    let process_b = process_b.
        stdin(Stdio::piped()).
        stdout(Stdio::from(file_final_output)).
        spawn()
        .expect("Failed to start zip-command");

    //Sort or concatenate
    let mut process_a = if sort {
        let mut cmd = Command::new("sort");
        cmd.arg("--merge");
        cmd
    } else {
        Command::new("cat")
    };

    //Provide all previously written output files to sort/cat
    let list_inputfiles:Vec<String> = list_inputfiles.iter().map(|p| p.to_str().expect("failed to convert path to string").to_string()).collect();
    process_a.args(list_inputfiles);


    //Wait for the process to finish
    let out= process_a.
        stdout(process_b.stdin.expect("failed to get stdin on bgzip")).
        output().
        expect("failed to get result from bgzip");
    println!("{}", String::from_utf8(out.stdout).unwrap());


}







#[cfg(test)]
mod tests {
/* 
    use super::*;

    #[test]
    fn test_get_boundaries() {
        let pool = 1;
        let starts: Vec<(u32, usize, usize)> = vec![(2, 10, 20), (1, 30, 40)];
        assert_eq!(get_boundaries(pool, &starts), (30, 40));
    }
    */

    /* 
    #[test]
    fn test_validate_barcode_inputs_and_pools() {
        let no_barcodes = vec![];
        let preset: Option<PathBuf> = Some(PathBuf::from("data/barcodes/atrandi/barcodes.tsv"));
        let bc = read_barcodes(&no_barcodes, &preset);
        assert_eq!(bc[0].sequence, b"GTAACCGA".to_vec());
        assert_eq!(bc[0].name, "A1");

        let pools = get_pools(&bc);
        assert_eq!(pools, HashSet::from([1, 2, 3, 4]));
    }*/

    /* 
    #[test]
    fn test_find_probable_barcode_boundaries() {
        let reads_file = PathBuf::from("data/test_reads_R2.fastq");
        let reads = io::open_fastq(&reads_file);

        let mut barcodes = vec![io::Barcode {
            index: 0,
            name: "A1".to_string(),
            pool: 1,
            sequence: b"GTAACCGA".to_vec(),
            pattern: Myers::<u64>::new(b"GTAACCGA".to_vec()),
        }];
        let pools = get_pools(&barcodes);
        let boundaries = barcode::find_probable_barcode_boundaries(reads, &mut barcodes, &pools, 9);
        assert_eq!(boundaries, vec![(1, 36, 44)]);
    }
    */
}


