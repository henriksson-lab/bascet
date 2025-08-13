// This software is released under the MIT license.
// See file LICENSE for full license details.
use anyhow::bail;
use anyhow::Result;
use clap::Args;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use log::{debug, info};
use seq_io::fastq::OwnedRecord;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;

use seq_io::fastq::Reader as FastqReader;
use seq_io::fastq::Record as FastqRecord;

use super::determine_thread_counts_1;
use crate::barcode::atrandi_wgs_barcode::AtrandiWGSChemistry;
use crate::barcode::general_barcode::GeneralCombinatorialBarcode;
use crate::barcode::AtrandiRNAseqChemistry;
use crate::barcode::Chemistry;
use crate::barcode::ParseBioChemistry3;
use crate::barcode::PetriseqChemistry;
use crate::barcode::TenxRNAChemistry;
<<<<<<< HEAD
use crate::fileformat::shard;
use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;
use crate::fileformat::tirp;
=======
use crate::fileformat::tirp;
use crate::fileformat::shard;
use crate::fileformat::shard::CellID;
use crate::fileformat::shard::ReadPair;
>>>>>>> main

type ListReadWithBarcode = Arc<Vec<(ReadPair, CellID)>>;
type ListRecordPair = Arc<Vec<RecordPair>>;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_CHEMISTRY: &str = "atrandi_wgs";

#[derive(Args)]
pub struct GetRawCMD {
    // FASTQ for r1
    #[arg(long = "r1", value_parser)]
    pub path_forward: PathBuf,

    // FASTQ for r2
    #[arg(long = "r2", value_parser)]
    pub path_reverse: PathBuf,

    // Output filename for complete reads
    #[arg(short = 'o', long = "out-complete", value_parser)]
    pub path_output_complete: PathBuf,

    // Output filename for incomplete reads
    #[arg(long = "out-incomplete", value_parser)]
    pub path_output_incomplete: PathBuf,

    // Optional: chemistry with barcodes to use
    #[arg(long = "chemistry", value_parser, default_value = DEFAULT_CHEMISTRY)]
    pub chemistry: String,

    // Optional: file with barcodes to use
    #[arg(long = "barcodes", value_parser)]
    pub path_barcodes: Option<PathBuf>,

    // Optional: Prepend library name to barcodes
    #[arg(long = "libname", value_parser)]
    pub libname: Option<String>,

    // Temporary file directory. TODO - use system temp directory as default? TEMP variable?
    #[arg(short = 't', value_parser, default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    //Whether to sort or not
    #[arg(long = "no-sort")]
    pub no_sort: bool,

    // Optional: Total error tolerance in barcode detection
    #[arg(long = "barcode-tol", value_parser)]
    pub total_barcode_error_tol: Option<usize>,

    // Optional: Partial error tolerance in barcode detection
    #[arg(long = "part-barcode-tol", value_parser)]
    pub part_barcode_error_tol: Option<usize>,

    // Optional: How many threads to use. Need better way of specifying? TODO
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_work: Option<usize>,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl GetRawCMD {
    /// Run the commandline option.
    /// This one takes raw FASTQ files, figures out the barcodes, and trims the reads
    pub fn try_execute(&mut self) -> Result<()> {
        crate::fileformat::verify_input_fq_file(&self.path_forward)?;
        crate::fileformat::verify_input_fq_file(&self.path_reverse)?;

        //Note: we always have two extra writer threads, because reading is expected to be the slow part. not an ideal implementation!
        let num_threads_reader = determine_thread_counts_1(self.num_threads_total)?;
        println!("Using threads: {}", num_threads_reader);

        //Set default libname
        let libname = if let Some(libname) = &self.libname {
            libname.clone()
        } else {
            "".to_string()
        };

        let params_io = GetRaw {
            path_tmp: self.path_tmp.clone(),
            path_forward: self.path_forward.clone(),
            path_reverse: self.path_reverse.clone(),
            path_output_complete: self.path_output_complete.clone(),
            path_output_incomplete: self.path_output_incomplete.clone(),
            libname: libname,
            //barcode_file: self.barcode_file.clone(),
            sort: !self.no_sort,
            threads_reader: num_threads_reader,
        };

        // Start the debarcoding for specified chemistry
        if self.chemistry == "atrandi_wgs" {
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut AtrandiWGSChemistry::new(
                    self.total_barcode_error_tol,
                    self.part_barcode_error_tol,
                ),
            );
        } else if self.chemistry == "atrandi_rnaseq" {
<<<<<<< HEAD
            let _ = GetRaw::getraw(Arc::new(params_io), &mut AtrandiRNAseqChemistry::new());
=======
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut AtrandiRNAseqChemistry::new(
                )
            );
>>>>>>> main
        } else if self.chemistry == "petriseq" {
            let _ = GetRaw::getraw(Arc::new(params_io), &mut PetriseqChemistry::new());
        } else if self.chemistry == "combinatorial" {
            if let Some(path_barcodes) = &self.path_barcodes {
                let _ = GetRaw::getraw(
                    Arc::new(params_io),
                    &mut GeneralCombinatorialBarcode::new(&path_barcodes),
                );
            } else {
                bail!("Barcode file not specified");
            }
        } else if self.chemistry == "10xrna" || self.chemistry == "10x_rna" {
<<<<<<< HEAD
            let _ = GetRaw::getraw(Arc::new(params_io), &mut TenxRNAChemistry::new());
=======
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut TenxRNAChemistry::new(
                )
            );
>>>>>>> main
        } else if self.chemistry == "pb_rnaseq" || self.chemistry == "pb_rna" {
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut ParseBioChemistry3::new(
                    //TODO: option to be more specific
<<<<<<< HEAD
                ),
=======
                )
>>>>>>> main
            );
        } else {
            bail!("Unidentified chemistry");
        }

        println!("GetRaw has finished successfully");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecordPair {
    pub reverse_record: OwnedRecord,
    pub forward_record: OwnedRecord,
}

////////////////
/// loop for a writer thread, sending output to a Writer
pub fn loop_tirp_writer<W>(
    writer_name: String,
    rx: &Receiver<Option<ListReadWithBarcode>>,
    hist: &mut shard::BarcodeHistogram,
    writer: W,
) where
    W: Write,
{
    let mut writer = BufWriter::new(writer);

    // Write reads
    let mut n_written = 0;
    while let Ok(Some(list_pairs)) = rx.recv() {
        for (bam_cell, cell_id) in list_pairs.iter() {
            tirp::write_records_pair_to_tirp(
                //::<W>
                &mut writer,
                &cell_id,
                &bam_cell,
            );

            hist.inc(&cell_id);

            if n_written % 100000 == 0 {
                println!("#reads written to {} outfile: {:?}", writer_name, n_written);
            }
            n_written = n_written + 1;
        }
    }

    //absolutely have to call this before dropping, for bufwriter
    _ = writer.flush();
}

////////////////
/// Writer to TIRP format, sorting on the fly
fn create_writer_thread(
    writer_name: String,
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool,
    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>,
    sort: bool,
    tempdir: &PathBuf,
) -> anyhow::Result<Sender<Option<ListReadWithBarcode>>> {
    let outfile = outfile.clone();

    let list_hist = Arc::clone(list_hist);
    let tempdir = tempdir.clone();
    //Create input read queue
    //Limit how many chunks can be in pipe
    let (tx, rx) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(100);

    thread_pool.execute(move || {
        // Open output file
        println!("Creating pre-TIRP output file: {}", outfile.display());

        let file_output = File::create(outfile).unwrap();

        let mut hist = shard::BarcodeHistogram::new();

        if sort {
            //Pipe to sort, then to given file
            let mut cmd = Command::new("sort");
            cmd.arg(format!("--temporary-directory={}", tempdir.display()));

            let mut process = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::from(file_output))
                .spawn()
                .expect("failed to start sorter");

            let mut stdin = process.stdin.as_mut().unwrap();

            debug!("sorter process ready");
            loop_tirp_writer(writer_name, &rx, &mut hist, &mut stdin);

            //Wait for process to finish
            debug!("Waiting for sorter process to exit");
            let _result = process.wait().unwrap();

            //TODO how to terminate pipe?? seems to happen now anyway
        } else {
            debug!("starting non-sorted write loop");

            let mut writer = BufWriter::new(file_output); //TODO  put in a buffered writer in loop. no need to do twice
            loop_tirp_writer(writer_name, &rx, &mut hist, &mut writer);
            _ = writer.flush();
        }

        //Keep histogram for final summary
        {
            let mut list_hist = list_hist.lock().unwrap(); //: Mutex<Vec<shard::BarcodeHistogram>>
            list_hist.push(hist);
        }
        //        _ = tx_hist.send(Arc::new(hist));
    });
    Ok(tx)
}

////////////////
///
pub struct GetRaw {
    pub path_tmp: std::path::PathBuf,

    pub path_forward: std::path::PathBuf,
    pub path_reverse: std::path::PathBuf,
    pub path_output_complete: std::path::PathBuf,
    pub path_output_incomplete: std::path::PathBuf,
    pub libname: String,

    pub sort: bool,

    pub threads_reader: usize,
}
impl GetRaw {
    pub fn getraw<'a>(
        params: Arc<GetRaw>,
        barcodes: &mut (impl Chemistry + Clone + Send + 'static),
    ) -> anyhow::Result<()> {
        info!("Running command: getraw");
        println!("Will sort: {}", params.sort);

        if false {
            crate::utils::check_bgzip().expect("bgzip not found");
            crate::utils::check_tabix().expect("tabix not found");
            println!("Required software is in place");
        }

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            println!("for debugging");
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            println!("Using tempdir {}", params.path_tmp.display());
            if fs::create_dir_all(&params.path_tmp).is_err() {
                panic!("Failed to create temporary directory");
            };
        }

        // Dispatch barcodes (presets + barcodes -> Vec<Barcode>)
        //let mut barcodes = AtrandiChemistry::new();

        // Open fastq files
        let mut forward_file = open_fastq(&params.path_forward).unwrap();
        let mut reverse_file = open_fastq(&params.path_reverse).unwrap();

        // Find probable barcode starts and ends
        barcodes
            .prepare(&mut forward_file, &mut reverse_file)
            .expect("Failed to detect barcode setup from reads");
        let mut forward_file = open_fastq(&params.path_forward).unwrap(); // reopen the file to read from beginning
        let mut reverse_file = open_fastq(&params.path_reverse).unwrap(); // reopen the file to read from beginning

        // Start writer threads
        let path_temp_complete_sorted = params
            .path_tmp
            .join(PathBuf::from("tmp_sorted_complete.bed"));
        let path_temp_incomplete_sorted = params
            .path_tmp
            .join(PathBuf::from("tmp_sorted_incomplete.bed"));

        let list_hist_complete = Arc::new(Mutex::new(Vec::<shard::BarcodeHistogram>::new()));
        let list_hist_incomplete = Arc::new(Mutex::new(Vec::<shard::BarcodeHistogram>::new()));

        let thread_pool_write = threadpool::ThreadPool::new(2);
        let tx_writer_complete = create_writer_thread(
            "complete".to_string(),
            &path_temp_complete_sorted,
            &thread_pool_write,
            &list_hist_complete,
            true,
            &params.path_tmp,
        )
        .expect("Failed to get writer threads");

        let tx_writer_incomplete = create_writer_thread(
            "incomplete".to_string(),
            &path_temp_incomplete_sorted,
            &thread_pool_write,
            &list_hist_incomplete,
            false,
            &params.path_tmp,
        )
        .expect("Failed to get writer threads");

        // Start worker threads.
        // Limit how many chunks can be in the air at the same time, as writers must be able to keep up with the reader
        let thread_pool_work = threadpool::ThreadPool::new(params.threads_reader);
        let (tx, rx) = crossbeam::channel::bounded::<Option<ListRecordPair>>(100);
        for tidx in 0..params.threads_reader {
            let rx = rx.clone();
            let tx_writer_complete = tx_writer_complete.clone();
            let tx_writer_incomplete = tx_writer_incomplete.clone();

            println!("Starting worker thread {}", tidx);

            let mut barcodes = barcodes.clone(); //This is needed to keep mutating the pattern in this structure
            let libname = params.libname.clone();

            thread_pool_work.execute(move || {
                while let Ok(Some(list_bam_cell)) = rx.recv() {
                    let mut pairs_complete: Vec<(ReadPair, CellID)> =
                        Vec::with_capacity(list_bam_cell.len());
                    let mut pairs_incomplete: Vec<(ReadPair, CellID)> =
                        Vec::with_capacity(list_bam_cell.len());

                    for bam_cell in list_bam_cell.iter() {
                        //Debarcode!
                        let (is_ok, cellid, readpair) = barcodes.detect_barcode_and_trim(
                            &bam_cell.forward_record.seq(),
                            &bam_cell.forward_record.qual(),
                            &bam_cell.reverse_record.seq(),
                            &bam_cell.reverse_record.qual(),
                        );

                        //Add library name to cellID
                        let cellid = format!("{}_{}", libname, cellid);

                        //Send readpair to writer
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
        read_all_reads(&mut forward_file, &mut reverse_file, &tx);

        // Send termination signals to workers, then wait for them to complete
        for _ in 0..params.threads_reader {
            let _ = tx.send(None);
        }
        thread_pool_work.join();

        // Send termination signals to writers, then wait for them to complete
        let _ = tx_writer_complete.send(None);
        let _ = tx_writer_incomplete.send(None);
        thread_pool_write.join();

        //Sort the complete output files and compress the output.
        let mut list_inputfiles: Vec<PathBuf> = Vec::new();
        list_inputfiles.push(path_temp_complete_sorted.clone());
        catsort_files(
            &list_inputfiles,
            &params.path_output_complete,
            params.sort,
            params.threads_reader,
        );

        //// Concatenate also the incomplete reads. Sorting never needed
        let mut list_inputfiles: Vec<PathBuf> = Vec::new();
        list_inputfiles.push(path_temp_incomplete_sorted.clone());
        catsort_files(
            &list_inputfiles,
            &params.path_output_incomplete,
            false,
            params.threads_reader,
        );

        //// Index the final file with tabix
        println!("Indexing final output file");
        tirp::index_tirp(&params.path_output_complete).expect("Failed to index file");

        //// Store histogram
        println!("Storing histogram for final output file");
        debug!("Collecting histograms");
        sum_and_store_histogram(
            &list_hist_complete,
            &tirp::get_histogram_path_for_tirp(&params.path_output_complete),
        );
        sum_and_store_histogram(
            &list_hist_incomplete,
            &tirp::get_histogram_path_for_tirp(&params.path_output_incomplete),
        );

        //// Remove temp files
        debug!("Removing temp files");
        _ = fs::remove_dir_all(&params.path_tmp);

        info!("done!");

        Ok(())
    }
}

pub fn sum_and_store_histogram(
    list_hist: &Arc<Mutex<Vec<shard::BarcodeHistogram>>>,
    path: &PathBuf,
) {
    debug!("Collecting histograms");

    let list_hist = list_hist.lock().unwrap();

    let mut totalhist = shard::BarcodeHistogram::new();
    for one_hist in list_hist.iter() {
        //let one_hist = rx.recv().expect("Could not get one histrogram");
        totalhist.add_histogram(&one_hist);
    }
    totalhist
        .write_file(&path)
        .expect(format!("Failed to write histogram to {:?}", path).as_str());
}

//////////
/// read the reads, send to next threads
fn read_all_reads(
    forward_file: &mut FastqReader<Box<dyn Read>>,
    reverse_file: &mut FastqReader<Box<dyn Read>>,
    tx: &Sender<Option<ListRecordPair>>,
) {
    let mut num_read = 0;
    loop {
        //Read out chunks. By sending in blocks, we can
        //1. keep threads asleep until they got enough work to do to motivate waking them up
        //2. avoid send operations, which likely aren't for free
        let chunk_size = 1000;

        let mut curit = 0;
        let mut list_recpair: Vec<RecordPair> = Vec::with_capacity(chunk_size);
        while curit < chunk_size {
            if let Some(record) = reverse_file.next() {
                let reverse_record: seq_io::fastq::RefRecord<'_> =
                    record.expect("Error reading record rev");
                let forward_record = forward_file
                    .next()
                    .unwrap()
                    .expect("Error reading record fwd");

                let recpair = RecordPair {
                    reverse_record: reverse_record.to_owned_record(),
                    forward_record: forward_record.to_owned_record(),
                };
                list_recpair.push(recpair);

                num_read = num_read + 1;

                if num_read % 100000 == 0 {
                    println!("read: {:?}", num_read);
                }
            } else {
                break;
            }
            curit += 1;
        }

        if !list_recpair.is_empty() {
            let _ = tx.send(Some(Arc::new(list_recpair)));
        } else {
            break;
        }
    }
}

/// Concatenate or merge sort files, then process them with bgzip
/// sort --merge  some_sorted.pregascet.0 some_sorted.pregascet.1 ... | bgzip -c /dev/stdin > test.gascet.0.gz    
///
/// we could also skip bgsort but it should be fast; we need to recompress it later otherwise. but it is a huge gain in ratio and need for temporary space!
/// also, output file is ready for use unless merging with other shards is needed
///
///  later index: tabix -p bed test.gascet.0.gz   
/// able to get a cell: tabix out_complete.0.gascet.gz A1_H5_D9_H12 | head
pub fn catsort_files(
    list_inputfiles: &Vec<PathBuf>,
    path_final: &PathBuf,
    sort: bool,
    num_cpu: usize,
) {
    let use_bgzip = true;

    //Final destination file
    let file_final_output = File::create(path_final).unwrap();
    println!(
        "Compressing and writing final output file: {:?}    from input files {:?}",
        path_final, list_inputfiles
    );

    //Compress on the fly with bgzip, pipe output to a file
    let mut process_b = if use_bgzip {
        let mut process_b = Command::new("bgzip");
        process_b
            .arg("-c")
            .arg("/dev/stdin")
            .arg("-@")
            .arg(format!("{}", num_cpu));
        process_b
    } else {
        // for testing on osx without bgzip
        print!("Warning: using gzip for final file. This will not work with tabix later. Not recommended");
        Command::new("gzip")
    };
    let process_b = process_b
        .stdin(Stdio::piped())
        .stdout(Stdio::from(file_final_output))
        .spawn()
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
    let list_inputfiles: Vec<String> = list_inputfiles
        .iter()
        .map(|p| {
            p.to_str()
                .expect("failed to convert path to string")
                .to_string()
        })
        .collect();
    process_a.args(list_inputfiles);

    //Wait for the process to finish
    let out = process_a
        .stdout(process_b.stdin.expect("failed to get stdin on bgzip"))
        .output()
        .expect("failed to get result from bgzip");
    println!("{}", String::from_utf8(out.stdout).unwrap());
}

/// Open a FASTQ file
pub fn open_fastq(file_handle: &PathBuf) -> anyhow::Result<FastqReader<Box<dyn std::io::Read>>> {
    let opened_handle = File::open(file_handle)
        .expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    let (reader, compression) = niffler::get_reader(Box::new(opened_handle))
        .expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    debug!(
        "Opened file {} with compression {:?}",
        &file_handle.display(),
        &compression
    );
    Ok(FastqReader::new(reader))
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
