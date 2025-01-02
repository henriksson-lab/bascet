// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use log::{debug, error, info};
use seq_io::fastq::OwnedRecord;
use std::path::PathBuf;
use std::process;
use std::process::Command;
use std::sync::Arc;
use crossbeam::channel::Sender;

use semver::{Version, VersionReq};

use super::{io, barcode, params};

use seq_io::fastq::Record as FastqRecord;

struct ReadPair {
    reverse_record: OwnedRecord,
    forward_record: OwnedRecord
}



pub fn check_dep_samtools() {
    debug!("Checking for the correct samtools");
    let req_samtools_version = VersionReq::parse(">=1.18").unwrap();
    let samtools = Command::new("samtools").arg("version").output();
    match samtools {
        Ok(samtools) => {
            let samtools_version = String::from_utf8_lossy(
                samtools
                    .stdout
                    .split(|c| *c == b'\n')
                    .next()
                    .unwrap()
                    .split(|c| *c == b' ')
                    .last()
                    .unwrap(),
            );
            let samtools_version = samtools_version.parse::<Version>().unwrap();
            if req_samtools_version.matches(&samtools_version) {
                debug!("Samtools version is recent enough");
            } else {
                error!("babbles extract requires Samtools >= 1.18");
                process::exit(1)
            }
        }
        Err(_error) => {
            error!("Samtools is either not installed or not in PATH");
            process::exit(1)
        }
    };
}


type ReadWithBarcode = (Arc<ReadPair>, Arc<Vec<String>>);


fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool
) -> anyhow::Result<Arc<Sender<Option<ReadWithBarcode>>>> {

    let outfile = outfile.clone();

    let (tx, rx) = crossbeam::channel::bounded::<Option<ReadWithBarcode>>(10000);
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));

    thread_pool.execute(move || {
        // Open cram output file
        println!("Creating output file: {}",outfile.display());
        let (cram_header, mut cram_writer) = io::create_cram_file(&outfile.with_extension("cram"));

        // Write reads
        while let Ok(Some((bam_cell,hits_names))) = rx.recv() {
            let reverse_record=&bam_cell.reverse_record;
            let forward_record=&bam_cell.forward_record;
            io::write_records_pair_to_cram(
                &cram_header,
                &mut cram_writer,
                forward_record,
                reverse_record,
                &hits_names
            );
            
        }
        //Flush the file
        cram_writer.try_finish(&cram_header).unwrap();
    });

    

    Ok(tx)
}


pub struct GetRaw {}

impl GetRaw {
    pub fn getraw<'a>(
        params_io: Arc<params::IO>,
        _params_runtime: Arc<params::Runtime>,
        params_threading: Arc<params::Threading>,
    ) -> anyhow::Result<()> {

        info!("Running command: getraw");

        // Dispatch barcodes (presets + barcodes -> Vec<Barcode>)
        let mut barcodes: barcode::CombinatorialBarcoding = barcode::read_barcodes(&params_io.barcode_file);
        //let pools = barcode::get_pools(&barcodes); // get unique pool names
        let n_pools=barcodes.num_pools();

        // Open fastq files
        let mut forward_file = io::open_fastq(&params_io.path_forward);
        let reverse_file = io::open_fastq(&params_io.path_reverse);

        // Find probable barcode starts and ends
        barcodes.find_probable_barcode_boundaries(reverse_file, 1000).expect("Failed to detect barcode setup from reads");
        let mut reverse_file = io::open_fastq(&params_io.path_reverse); // reopen the file to read from beginning

        // Start writer threads
        let thread_pool_write = threadpool::ThreadPool::new(2);
        let tx_writer_complete = create_writer_thread(&params_io.path_output_complete, &thread_pool_write).expect("Failed to get writer threads");
        let tx_writer_incomplete = create_writer_thread(&params_io.path_output_incomplete, &thread_pool_write).expect("Failed to get writer threads");

        // Start worker threads
        let thread_pool_work = threadpool::ThreadPool::new(params_threading.threads_work);
        let (tx, rx) = crossbeam::channel::bounded::<Option<Arc<ReadPair>>>(1000);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));        
        for tidx in 0..params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let tx_writer_complete=Arc::clone(&tx_writer_complete);
            let tx_writer_incomplete=Arc::clone(&tx_writer_incomplete);

            println!("Starting worker thread {}",tidx);

            let mut barcodes = barcodes.clone(); //This is needed to keep mutating the pattern in this structure

            thread_pool_work.execute(move || {

                while let Ok(Some(bam_cell)) = rx.recv() {

                    
                    let hits_names = barcodes.detect_barcode(&bam_cell.reverse_record.seq());
                    let hits_names = Arc::new(hits_names);

                    // Finally, write the forward and reverse together with barcode info in the output cram.
                    // Separate complete entries from incomplete ones
                    if hits_names.len()==n_pools {
                        let _ = tx_writer_complete.send(Some((
                            Arc::clone(&bam_cell),
                            Arc::clone(&hits_names),
                        )));
                    } else {
                        let _ = tx_writer_incomplete.send(Some((
                            Arc::clone(&bam_cell),
                            Arc::clone(&hits_names),
                        )));
                    }
                }
            });
        }

        // Read the fastq files, send to worker threads
        println!("Starting to read input file");
        while let Some(record) = reverse_file.next() {

            //println!("read line");
            let reverse_record: seq_io::fastq::RefRecord<'_> = record.expect("Error reading record");
            let forward_record = forward_file.next().unwrap().expect("Error reading record");

            let recpair = ReadPair {
             reverse_record: reverse_record.to_owned_record(),
             forward_record: forward_record.to_owned_record()
           };
            
            let recpair = Arc::new(recpair);
            let _ = tx.send(Some(Arc::clone(&recpair)));    
        }

        // Send termination signals to workers, then wait for them to complete
        for _ in 0..params_threading.threads_work {
            let _ = tx.send(None);
        }
        thread_pool_work.join();
        
        // Send termination signals to writers, then wait for them to complete
        let _ = tx_writer_complete.send(None);
        let _ = tx_writer_incomplete.send(None);
        thread_pool_write.join();


        //Sort the output files if requested.
        //this only performed for complete entries
        if params_io.sort {
            info!("sorting cram file with samtools");
            check_dep_samtools();
            // samtools sort -t CB -o sorted.cram t0.cram
            let samtools_sort = Command::new("samtools")
                .arg("sort")
                .arg("-t")
                .arg("CB")
                .arg("-o")
                .arg(&params_io.path_output_complete.with_extension("sorted.cram")) // TODO change output name
                .arg(&params_io.path_output_complete.with_extension("cram"))
                // to change to unsorted? need earlier logic for sorted vs unsorted file names
                .output();
            match samtools_sort {
                Ok(samtools_sort) => {
                    info!("samtools sort finished");
                    samtools_sort
                }
                Err(_) => {
                    error!("samtools sort failed");
                    process::exit(1)
                }
            };
        }
        info!("done!");




        Ok(())
    }
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
