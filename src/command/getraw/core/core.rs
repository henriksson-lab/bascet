// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::process;
use std::process::Command;

use semver::{Version, VersionReq};

use super::{io, io::Barcode};


use bio::pattern_matching::myers::Myers;
use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct Row {
    pos: u32,
    well: String,
    seq: String,
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




pub struct GetRaw {}

impl GetRaw {

}



pub fn prepare(
    forward: &PathBuf,
    reverse: &PathBuf,
    output: &PathBuf,
    barcode_files: &Vec<PathBuf>,
    preset: &Option<PathBuf>,
    sorted: &bool,
) {
    info!("Starting babbles prepare");
    // TODO
    // - remove the "bad" cells
    // - sort the CRAM
    // - add the possibility to index

    // prepare  converts fastq to bam
    // and perform barcode detection

    // Dispatch barcodes (presets + barcodes -> Vec<Barcode>)
    let mut barcodes: Vec<Barcode> = validate_barcode_inputs(barcode_files, preset);
    let pools = get_pools(&barcodes); // get unique pool names

    // Open fastq files
    let mut forward_file = io::open_fastq(&forward);
    let reverse_file = io::open_fastq(&reverse);

    // Find probable barcode starts and ends
    // Vec<(pool, barcodes_start, barcodes_end)>
    let starts = find_probable_barcode_boundaries(reverse_file, &mut barcodes, &pools, 1000);
    let mut reverse_file = io::open_fastq(&reverse); // reopen the file to read from beginning

    // Open cram utput file
    let (cram_header, mut cram_writer) = io::create_cram_file(&output.with_extension("cram"));

    // Read the fastq files, detect barcodes and write to cram
    while let Some(record) = reverse_file.next() {
        let reverse_record = record.expect("Error reading record");
        let forward_record = forward_file.next().unwrap().expect("Error reading record");

        // One hit is (name, pool, seq, start, stop, score)
        // Note: since we are passing a slide to the seek function, the hit's position is
        // relative to the start of the slice (the probable start of the barcode)
        let mut hits: Vec<(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();
        let mut best_hits: Vec<&(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();

        for barcode in barcodes.iter_mut() {
            let (start, stop) = get_boundaries(barcode.pool, &starts);
            let slice = &reverse_record.seq()[start..stop];
            hits.extend(barcode.seek(slice, 1)); // seek returns the best hit for that query
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

        // TODO are there reads without hits / with fewer than n_pools hits?
        // what should we do with them?
        let hits_names: Vec<String> = best_hits.iter().map(|x| x.0.to_string()).collect();
        let hits_seq: Vec<String> = best_hits
            .iter()
            .map(|x| String::from_utf8(x.2.clone()).unwrap())
            .collect();

        // Finally, write the forward and reverse together with barcode info in the output cram
        io::write_records_pair_to_cram(
            &cram_header,
            &mut cram_writer,
            forward_record,
            reverse_record,
            &hits_names,
            &hits_seq,
        );
    }
    cram_writer.try_finish(&cram_header).unwrap();

    if *sorted {
        info!("sorting cram file with samtools");
        check_dep_samtools();
        // samtools sort -t CB -o sorted.cram t0.cram
        let samtools_sort = Command::new("samtools")
            .arg("sort")
            .arg("-t")
            .arg("CB")
            .arg("-o")
            .arg(&output.with_extension("sorted.cram")) // TODO change output name
            .arg(&output.with_extension("cram"))
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
}




fn validate_barcode_inputs(barcode_files: &Vec<PathBuf>, preset: &Option<PathBuf>) -> Vec<Barcode> {
    // takes either presets or barcode files and returns a vector of Barcodes
    // TODO while presets are being implemented,  barcode files support is currently disabled
    let mut barcodes: Vec<Barcode> = Vec::new();
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
        }
        None => {
            // load the barcodes here
            println!("loading barcodes: {:?}", barcode_files);
        }
    }
    barcodes
}






fn find_probable_barcode_boundaries(
    mut fastq_file: FastqReader<Box<dyn std::io::Read>>,
    barcodes: &mut Vec<io::Barcode>,
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

    #[test]
    fn test_validate_barcode_inputs_and_pools() {
        let no_barcodes = vec![];
        let preset: Option<PathBuf> = Some(PathBuf::from("data/barcodes/atrandi/barcodes.tsv"));
        let bc = validate_barcode_inputs(&no_barcodes, &preset);
        assert_eq!(bc[0].sequence, b"GTAACCGA".to_vec());
        assert_eq!(bc[0].name, "A1");

        let pools = get_pools(&bc);
        assert_eq!(pools, HashSet::from([1, 2, 3, 4]));
    }

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
