use clap::builder::Str;
// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use seq_io::fastq::Record;
use seq_io::fastq::RefRecord;
use seq_io::fastq::OwnedRecord;
use std::clone;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::process;
use std::process::Command;
use std::sync::Arc;
use std::io::Read;
use crossbeam::channel::Sender;

use semver::{Version, VersionReq};

use super::{io, barcode, barcode::Barcode, params};


use bio::pattern_matching::myers::Myers;
use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct Row {
    pos: u32,
    well: String,
    seq: String,
}


//let reverse_record = record.expect("Error reading record");
//let forward_record = forward_file.next().unwrap().expect("Error reading record");


//unable to access qual record!!
//https://docs.rs/seq_io/latest/seq_io/fastq/struct.OwnedRecord.html

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


type ReadWithBarcode = (Arc<ReadPair>, Arc<Vec<String>>, Arc<Vec<String>>);


fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool
) -> anyhow::Result<Arc<Sender<Option<ReadWithBarcode>>>> {

    let outfile = outfile.clone();

    let (tx, rx) = crossbeam::channel::bounded::<Option<(Arc<ReadPair>, Arc<Vec<String>>, Arc<Vec<String>>)>>(10000);
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));
    //let rx_writer = Arc::clone(&rx_writer_complete);
    //let params_io_w = Arc::clone(&params_io);

    thread_pool.execute(move || {
        // Open cram output file
        println!("Creating output file: {}",outfile.display());
        let (cram_header, mut cram_writer) = io::create_cram_file(&outfile.with_extension("cram"));

        // Write reads
        while let Ok(Some((bam_cell,hits_names, hits_seq))) = rx.recv() {
            let reverse_record=&bam_cell.reverse_record;
            let forward_record=&bam_cell.forward_record;
            io::write_records_pair_to_cram(
                &cram_header,
                &mut cram_writer,
                forward_record,
                reverse_record,
                &hits_names,
                &hits_seq,
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
        params_runtime: Arc<params::Runtime>,
        params_threading: Arc<params::Threading>,
    ) -> anyhow::Result<()> {

        info!("Running command: getraw");

        // Dispatch barcodes (presets + barcodes -> Vec<Barcode>)
        let mut barcodes: Vec<Barcode> = validate_barcode_inputs(&params_io.barcode_file);
        let pools = get_pools(&barcodes); // get unique pool names
        let n_pools=pools.len();

        // Open fastq files
        let mut forward_file = io::open_fastq(&params_io.path_forward);
        let reverse_file = io::open_fastq(&params_io.path_reverse);

        // Find probable barcode starts and ends
        // Vec<(pool, barcodes_start, barcodes_end)>
        let starts = find_probable_barcode_boundaries(reverse_file, &mut barcodes, &pools, 1000);
        let mut reverse_file = io::open_fastq(&params_io.path_reverse); // reopen the file to read from beginning

        // Make it possible to share data with threads
        let barcodes = Arc::new(barcodes);
        let pools = Arc::new(pools);
        let starts = Arc::new(starts);

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
            let params_runtime = Arc::clone(&params_runtime);

            let barcodes = Arc::clone(&barcodes);
            let pools = Arc::clone(&pools);
            let starts = Arc::clone(&starts);
            let tx_writer_complete=Arc::clone(&tx_writer_complete);
            let tx_writer_incomplete=Arc::clone(&tx_writer_incomplete);

            println!("Starting worker thread {}",tidx);

            thread_pool_work.execute(move || {
                //Ugly copy, for myers algorithm structure
                let barcodes_old=barcodes;
                let mut barcodes:Vec<Barcode> = Vec::new();
                for bc in barcodes_old.iter() {
                    barcodes.push(bc.clone());
                }

                while let Ok(Some(bam_cell)) = rx.recv() {

                    let reverse_record=&bam_cell.reverse_record;
                    let forward_record=&bam_cell.forward_record;

                    // One hit is (name, pool, seq, start, stop, score)
                    // Note: since we are passing a slide to the seek function, the hit's position is
                    // relative to the start of the slice (the probable start of the barcode)
                    let mut hits: Vec<(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();
                    let mut best_hits: Vec<&(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();

                    //for barcode in barcodes.iter() {
                    for barcode in barcodes.iter_mut() {
                        let (start, stop) = get_boundaries(barcode.pool, &starts);
                        let slice = &bam_cell.reverse_record.seq()[start..stop];
                        hits.extend(barcode.seek(slice, 1)); // seek returns the best hit for that query   .................. this is mutable. wtf? myers lazy algorithm
                    }

                    // For each pool, only keep the best barcode hit
                    for pool in pools.iter() {
                        let pool_hits: Vec<&(&String, u32, Vec<u8>, usize, usize, i32)> =
                            hits.iter().filter(|x| pool == &x.1).collect();
                        if pool_hits.len() > 0 {
                            // take the element with the lowest score
                            let best_hit = pool_hits.iter().min_by(|a, b| a.5.cmp(&b.5)).unwrap();
                            best_hits.push(*best_hit);
                        }
                    }

                    //Get the name of the barcodes
                    let hits_names: Vec<String> = best_hits.iter().map(|x| x.0.to_string()).collect();
                    let hits_seq: Vec<String> = best_hits
                        .iter()
                        .map(|x| String::from_utf8(x.2.clone()).unwrap())
                        .collect();

                    let hits_names=Arc::new(hits_names);
                    let hits_seq=Arc::new(hits_seq);


                    // Finally, write the forward and reverse together with barcode info in the output cram.
                    // Separate complete entries from incomplete ones
                    if hits_names.len()==n_pools {
                        let _ = tx_writer_complete.send(Some(((
                            Arc::clone(&bam_cell),
                            Arc::clone(&hits_names),
                            Arc::clone(&hits_seq)
                        ))));
                    } else {
                        let _ = tx_writer_incomplete.send(Some(((
                            Arc::clone(&bam_cell),
                            Arc::clone(&hits_names),
                            Arc::clone(&hits_seq)
                        ))));
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






/* 
fn read_barcodes_file(
    opened: &dyn Read, ///////// difficult type!
    barcodes: &mut Vec<Barcode> 
) {

    //as bytes gives: &[u8]

    let mut barcodes: Vec<Barcode> = Vec::new();

    let mut n_barcodes = 0;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(*opened);
    for result in reader.deserialize() {
        let record: Row = result.unwrap();
        let b = Barcode {
            index: n_barcodes,
            name: record.well,
            pool: record.pos,
            sequence: record.seq.as_bytes().to_vec(),
            pattern: Myers::<u64>::new(record.seq.as_bytes().to_vec()),
        };
        barcodes.push(b);
        n_barcodes += 1;
    }
    if n_barcodes==0 {
        println!("Warning: empty barcodes file");
    }
}
*/






fn validate_barcode_inputs(
    barcode_file: &Option<PathBuf>
) -> Vec<Barcode> {


    let mut barcodes: Vec<Barcode> = Vec::new();



    //let a = include_str!("hello.txt");
    let atrandi_bcs = include_bytes!("atrandi_barcodes.tsv");
    let c = String::from_utf8(atrandi_bcs.to_vec()).unwrap();

    //read_barcodes_file(&atrandi_bcs.as_ref(), &mut barcodes);

    let mut n_barcodes = 0; //TODO: why is this needed later?

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(c.as_bytes());
    for result in reader.deserialize() {
        let record: Row = result.unwrap();

        let b = Barcode::new(
                n_barcodes,
                record.well.as_str(),
                record.pos,
                record.seq.as_bytes()
        );    

        let b = Barcode {
            index: n_barcodes,
            name: record.well,
            pool: record.pos,
            sequence: record.seq.as_bytes().to_vec(),
            pattern: Myers::<u64>::new(record.seq.as_bytes().to_vec()),
        };
        barcodes.push(b);
        n_barcodes += 1;
    }

    if n_barcodes==0 {
        println!("Warning: empty barcodes file");
    }
    //TODO support reading of new files too

/* 

    // takes either presets or barcode files and returns a vector of Barcodes
    // TODO while presets are being implemented, barcode files support is currently disabled
    match preset {
        Some(preset) => {
            debug!("loading barcode preset: {:?}", preset);
            // TODO RESOLVE PRESET FILEPATH
            // not easy to include data in rust binary?
            // let's give the path for now
            // can include downloading in the future? of include the data in the binary?
            let opened = match File::open(&preset) {
                Ok(file) => file,
                Err(_) => {
                    error!("Could not open preset file {}", &preset.display());
                    process::exit(1)
                }
            };
            let mut n_barcodes = 0;

            
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_reader(opened);
            for result in reader.deserialize() {
                let record: Row = result.unwrap();
                let b = Barcode {
                    index: n_barcodes,
                    name: record.well,
                    pool: record.pos,
                    sequence: record.seq.as_bytes().to_vec(),
                    pattern: Myers::<u64>::new(record.seq.as_bytes().to_vec()),
                };
                barcodes.push(b);
                n_barcodes += 1;
            }
            
            if(n_barcodes==0){
                println!("Warning: empty barcodes file");
            }

            read_barcodes_file(&opened, &mut barcodes);

        }
        None => {
            // load the barcodes here
            println!("loading barcodes: {:?}", barcode_files);
        }
    }
*/


    barcodes
}






fn find_probable_barcode_boundaries(
    mut fastq_file: FastqReader<Box<dyn std::io::Read>>,
    barcodes: &mut Vec<Barcode>,
    pools: &HashSet<u32>,
    n_reads: u32,
) -> Vec<(u32, usize, usize)> {
    // Vec<(pool, start, stop)>
    let mut starts: Vec<(u32, usize, usize)> = Vec::new();
    // find most probable barcode start through iterating over the first n reads
    let mut all_hits: Vec<(u32, usize, usize, i32)> = Vec::new();
    for _ in 0..n_reads {
        let record = fastq_file.next().unwrap();
        let record = record.expect("Error reading record");
        for barcode in barcodes.iter_mut() {
            let mut hits = barcode.seek(&record.seq(), 1);
            // only keep pool, start, stop, score from hits
            let hits_filtered = hits.iter_mut().map(|x| (x.1, x.3, x.4, x.5));
            all_hits.extend(hits_filtered);
        }
    }

    let limit = (0.9 * n_reads as f32).floor() as usize;

    // now find the most likely possible starts and ends for each pool
    for pool in pools.iter() {
        let pool_hits_for_start = all_hits.iter().filter(|x| pool == &x.0);
        let pool_hits_for_end = all_hits.iter().filter(|x| pool == &x.0);
        // now the start and stop for that pool hit
        let possible_starts = pool_hits_for_start
            .map(|x| x.1)
            .counts()
            .into_iter()
            .filter(|&(_, v)| v > limit)
            .collect::<HashMap<_, _>>();
        let possible_ends = pool_hits_for_end
            .map(|x| x.2)
            .counts()
            .into_iter()
            .filter(|&(_, v)| v > limit)
            .collect::<HashMap<_, _>>();
        trace!(
            "Possible start positions for pool {:?}: {:?}",
            pool,
            possible_starts
        );
        trace!(
            "Possible end positions for pool {:?}: {:?}",
            pool,
            possible_ends
        );
        let smallest_start = match possible_starts.is_empty() {
            true => {
                warn!(
                    "No possible start positions found on the first {} reads",
                    n_reads
                );
                warn!("The barcode detection will be performed on the whole read");
                1 as usize
            }
            false => *possible_starts.keys().min().unwrap(),
        };
        let biggest_end = match possible_ends.is_empty() {
            true => {
                warn!(
                    "No possible start positions found on the first {} reads",
                    n_reads
                );
                warn!("The barcode detection will be performed on the whole read");
                1 as usize // TODO have read length here
            }
            false => *possible_ends.keys().max().unwrap(),
        };
        debug!(
            "Pool {:?} - Most probable start and end position for barcodes: {} - {}",
            pool, smallest_start, biggest_end
        );
        starts.push((*pool, smallest_start, biggest_end));
    }
    starts
}




fn get_pools(barcodes: &Vec<Barcode>) -> HashSet<u32> {
    // from a vector of Barcodes, return the distinct barcode pools
    let pools = barcodes.iter().map(|x| x.pool).collect::<HashSet<_>>();
    pools
}



fn get_boundaries(pool: u32, starts: &Vec<(u32, usize, usize)>) -> (usize, usize) {
    // get the start and end for a pool
    // unwrapping here is safe because (i) pools are unique and (ii)
    // find_probable_barcode_boundaries() garantees one element per pool
    let elem = starts.iter().find(|x| x.0 == pool).unwrap();
    (elem.1, elem.2)
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_boundaries() {
        let pool = 1;
        let starts: Vec<(u32, usize, usize)> = vec![(2, 10, 20), (1, 30, 40)];
        assert_eq!(get_boundaries(pool, &starts), (30, 40));
    }

    /* 
    #[test]
    fn test_validate_barcode_inputs_and_pools() {
        let no_barcodes = vec![];
        let preset: Option<PathBuf> = Some(PathBuf::from("data/barcodes/atrandi/barcodes.tsv"));
        let bc = validate_barcode_inputs(&no_barcodes, &preset);
        assert_eq!(bc[0].sequence, b"GTAACCGA".to_vec());
        assert_eq!(bc[0].name, "A1");

        let pools = get_pools(&bc);
        assert_eq!(pools, HashSet::from([1, 2, 3, 4]));
    }*/

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
        let boundaries = find_probable_barcode_boundaries(reads, &mut barcodes, &pools, 9);
        assert_eq!(boundaries, vec![(1, 36, 44)]);
    }
}
