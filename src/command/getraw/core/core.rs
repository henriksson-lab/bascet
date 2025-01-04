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

#[derive(Debug,Clone)]
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


//type ReadWithBarcode = (Arc<ReadPair>, Arc<Vec<String>>);

type ListReadWithBarcode = Arc<Vec<(ReadPair,Vec<String>)>>;



fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool
) -> anyhow::Result<Arc<Sender<Option<ListReadWithBarcode>>>> {

    let outfile = outfile.clone();

    //Limit how many chunks can be in pipe
    let (tx, rx) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(100);  
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));

    thread_pool.execute(move || {
        // Open cram output file
        println!("Creating output file: {}",outfile.display());
        let mut writer = create_new_bam(&outfile).expect("failed to create bam-like file");

        // Write reads
        let mut n_written=0;
        while let Ok(Some(list_pairs)) = rx.recv() {
            for (bam_cell, hits_names) in list_pairs.iter() {
                let reverse_record=&bam_cell.reverse_record;
                let forward_record=&bam_cell.forward_record;

                write_records_pair_to_bamlike(
                    &mut writer,
                    forward_record,
                    reverse_record,
                    &hits_names
                );

                if n_written%100000 == 0 {
                    println!("written to {:?} -- {:?}",outfile, n_written);
                }
                n_written = n_written + 1;
            }

            
        }
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

        // Start worker threads.
        // Limit how many chunks can be in the air at the same time, as writers must be able to keep up with the reader
        let thread_pool_work = threadpool::ThreadPool::new(params_threading.threads_work);
        let (tx, rx) = crossbeam::channel::bounded::<Option<Arc<Vec<ReadPair>>>>(100);   
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));        
        for tidx in 0..params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let tx_writer_complete=Arc::clone(&tx_writer_complete);
            let tx_writer_incomplete=Arc::clone(&tx_writer_incomplete);

            println!("Starting worker thread {}",tidx);

            let mut barcodes = barcodes.clone(); //This is needed to keep mutating the pattern in this structure

            thread_pool_work.execute(move || {

                while let Ok(Some(list_bam_cell)) = rx.recv() {
                    let mut pairs_complete: Vec<(ReadPair, Vec<String>)> = Vec::with_capacity(list_bam_cell.len());
                    let mut pairs_incomplete: Vec<(ReadPair, Vec<String>)> = Vec::with_capacity(list_bam_cell.len());

                    for bam_cell in list_bam_cell.iter() {
                        let hits_names = barcodes.detect_barcode(&bam_cell.reverse_record.seq());
                        if hits_names.len()==n_pools {
                            pairs_complete.push((bam_cell.clone(), hits_names.clone()));
                        } else {
                            pairs_incomplete.push((bam_cell.clone(), hits_names.clone()));
                        }
                    }

                let _ = tx_writer_complete.send(Some(Arc::new(pairs_complete)));
                let _ = tx_writer_incomplete.send(Some(Arc::new(pairs_incomplete)));
                }
            });
        }

        // Read the fastq files, send to worker threads
        println!("Starting to read input file");
        let mut num_read = 0;
        loop {

            //Read out chunks. By sending in blocks, we can
            //1. keep threads asleep until they got enough work to do to motivate waking them up
            //2. avoid send operations, which likely aren't for free
            let chunk_size = 1000;

            let mut curit = 0;
            let mut list_recpair:Vec<ReadPair> = Vec::with_capacity(chunk_size);
            while curit<chunk_size {
                if let Some(record) = reverse_file.next() {
                    let reverse_record: seq_io::fastq::RefRecord<'_> = record.expect("Error reading record rev");
                    let forward_record = forward_file.next().unwrap().expect("Error reading record fwd");
        
                    let recpair = ReadPair {
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



/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////

/// ideal partition:
/// zip/bc/r1.fq
/// zip/bc/r2.fq


/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////

// generally seems like a better lib? unified SAM/BAM/CRAM interface
// https://github.com/rust-bio/rust-htslib/blob/master/src/bam/mod.rs


use rust_htslib;
use rust_htslib::bam;
use rust_htslib::bam::record::Aux;
use std::path::Path;
//use std::path::PathBuf;
use anyhow;
use anyhow::bail;



//detect file format from file extension
pub fn detect_bam_file_format(fname: &Path) -> anyhow::Result<bam::Format> {

    let fext = fname.extension().expect("Output file lacks file extension");
    match fext.to_str().expect("Failing string conversion") {
        "sam" => Ok(bam::Format::Sam),
        "bam" => Ok(bam::Format::Bam),
        "cram" => Ok(bam::Format::Cram),
        _ => bail!("Cannot detect BAM/CRAM/SAM type from file extension")
    }
}


pub fn create_new_bam(
    fname: &Path
// num_threads
// compression level
) -> anyhow::Result<bam::Writer> {

    let file_format = detect_bam_file_format(fname)?;

    let mut header = bam::Header::new();
    header.push_comment("Debarcoded by Bascet".as_bytes());

    let mut writer = bam::Writer::from_path(fname, &header, file_format).unwrap();

    _ = writer.set_threads(5);  //  need we also give a pool? https://docs.rs/rust-htslib/latest/rust_htslib/bam/struct.Writer.html#method.set_threads
    _ = writer.set_compression_level(bam::CompressionLevel::Fastest);  //or no compression, do later ; for user to specify

    Ok(writer)
}



pub fn write_records_pair_to_bamlike(
    writer: &mut bam::Writer,
    forward: &impl seq_io::fastq::Record,
    reverse: &impl seq_io::fastq::Record,
    barcodes_hits: &Vec<String>
) {
    // create the forward record
    let fname = forward
        .id()
        .unwrap()
        .as_bytes()
        .split_last_chunk::<2>() // we want to remove the paired identifiers /1 and /2
        .unwrap().0;

    let cell_barcode = barcodes_hits.join("-");

    ////// Forward record
    let mut record = bam::Record::new();
    let qual = forward.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>();
    record.set(
        fname,
        None,
        forward.seq(),
        qual.as_slice()  //forward.qual()
    );
    _ = record.push_aux("CB".as_bytes(), Aux::String(cell_barcode.as_str()));
    record.set_flags(0x4D); // 0x4D  read paired, read unmapped, mate unmapped, first in pair
    //.set_flags(cram::record::Flags::from(0x07))   what is this?
    writer.write(&record).expect("Failed to write forward read");
    
    ////// Reverse record
    let mut record = bam::Record::new();
    let qual = reverse.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>();
    record.set(
        fname,
        None,
        reverse.seq(),
        qual.as_slice() //reverse.qual()
    );
    _ = record.push_aux("CB".as_bytes(), Aux::String(cell_barcode.as_str()));
    record.set_flags(0x8D); // 0x8D  read paired, read unmapped, mate unmapped, second in pair
    //.set_flags(cram::record::Flags::from(0x03))  hm?
    writer.write(&record).expect("Failed to write reverse read");

}



//Can .drop to end file earlier

