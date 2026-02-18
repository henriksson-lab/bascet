use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use bascet_io::fastq::fastq;
use bascet_io::tirp::tirp;
use bascet_io::{
    BBGZWriteBlock, Compression,
};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::{Args, Subcommand};
use clio::{InputPath, OutputPath};
use crossbeam::channel::{Receiver, RecvTimeoutError};
use gxhash::HashMapExt;
use itertools::{izip};

use bascet_core::attr::{meta::*, quality::*, sequence::*};
use bascet_core::*;
use bascet_derive::Budget;
use bascet_io::{
    codec::{self, bbgz},
    parse, BBGZHeader, BBGZWriter,
};
use serde::Serialize;
use smallvec::{SmallVec, ToSmallVec};

use crate::barcode::atrandi_wgs_barcode_illumina::DebarcodeAtrandiWGSChemistryIllumina;
use crate::barcode::atrandi_wgs_barcode_longread::DebarcodeAtrandiWGSChemistryLongread;
use crate::barcode::{Chemistry, ParseBioChemistry3};
use crate::command::shardify::ShardifyCMD;
use crate::{bbgz_compression_parser, bounded_parser};
use tracing::{debug, info, warn, error};

#[derive(Args)]
pub struct GetRawCMD {
    #[arg(
        short = '1',
        long = "r1",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of input R1 FASTQ files (comma-separated)"
    )]
    pub paths_r1: Vec<InputPath>,

    #[arg(
        short = '2',
        long = "r2",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of input R2 FASTQ files (comma-separated)"
    )]
    pub paths_r2: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of output file paths (comma-separated)"
    )]
    pub paths_out: Vec<OutputPath>,

    #[arg(
        long = "hist",
        help = "Histogram file paths. Defaults to <path_out>.hist"
    )]
    pub paths_hist: Option<Vec<OutputPath>>,

    #[arg(
        long = "temp",
        help = "Temporary storage directory. Defaults to <path_out>"
    )]
    pub path_temp: Option<PathBuf>,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use",
        value_name = "6..",
        value_parser = bounded_parser!(BoundedU64<6, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<6, { u64::MAX }>>,

    #[arg(
        long = "countof-threads-read",
        help = "Number of reader threads",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    countof_threads_read: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "countof-threads-debarcode",
        help = "Number of debarcoding threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    countof_threads_debarcode: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "countof-threads-sort",
        help = "Number of initial sort sorting threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    countof_threads_sort: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "countof-threads-write",
        help = "Number of writer threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    countof_threads_write: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "countof-threads-compress",
        help = "Number of compressor threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    countof_threads_compress: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(32),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size",
        value_name = "12.5%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,

    #[arg(
        long = "sizeof-sort-buffer",
        help = "Total sort buffer size",
        value_name = "50%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_sort_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-compress-buffer",
        help = "Total compression buffer size",
        value_name = "12.5%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_compress_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-compress-raw-buffer",
        help = "Total compression raw copy buffer size",
        value_name = "25%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_compress_raw_buffer: Option<ByteSize>,

    #[arg(
        long = "compression-level",
        help = "Compression level (0=none, 12=best, default=6)",
        value_name = "0..=12",
        value_parser = bbgz_compression_parser!(bbgz::Compression),
        default_value_t = bbgz::Compression::balanced(),
    )]
    pub compression_level: bbgz::Compression,

    #[arg(
        long = "library",
        help = "Library name to prefix cell barcodes with. Defaults to unix timestamp"
    )]
    pub library: Option<String>,

    #[arg(
        long = "skip-debarcode",
        num_args = 1..,
        value_delimiter = ',',
        help = "Skip debarcoding phase and merge existing chunk files (comma-separated list of chunk files)"
    )]
    pub skip_debarcode: Option<Vec<InputPath>>,

    #[arg(
        long = "countof-merge-streams",
        help = "Number of files to merge simultaneously. Defaults to memory / sizeof-stream-arena.",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    countof_merge_streams: Option<BoundedU64<2, { u64::MAX }>>,

    #[command(subcommand)]
    pub chemistry: GetRawChemistryCMD,
}

#[derive(Subcommand)]
pub enum GetRawChemistryCMD {
    /// AtrandiWGS chemistry, uses combinatorial 8bp barcodes for debarcoding -- short read for illumina, paired end
    AtrandiWGS,
    /// AtrandiWGS chemistry, uses combinatorial 8bp barcodes for debarcoding -- long read for pacbio/nanopore, single read
    AtrandiWGSLR,
    /// ParseBio chemistry, uses combinatorial 8bp barcodes for debarcoding
    ParseBio {
        #[arg(
            long = "subchemistry",
            default_value_t = String::from(""),
            help = "ParseBio subchemistry"
        )]
        subchemistry: String,
    },
}

#[derive(Clone)]
#[enum_dispatch::enum_dispatch(Chemistry)]
pub enum GetRawChemistry {
    AtrandiWGS(DebarcodeAtrandiWGSChemistryIllumina),
    AtrandiWGSLR(DebarcodeAtrandiWGSChemistryLongread),
    ParseBio(ParseBioChemistry3),
}

#[derive(Budget, Debug)]
struct GetrawBudget {
    #[threads(Total)]
    threads: BoundedU64<6, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.15) as u64))]
    countof_threads_read: BoundedU64<2, { u64::MAX }>,
    #[threads(TDebarcode, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.3) as u64))]
    countof_threads_debarcode: BoundedU64<1, { u64::MAX }>,

    #[threads(TSort, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.3) as u64))]
    countof_threads_sort: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.05) as u64))]
    countof_threads_write: BoundedU64<1, { u64::MAX }>,
    #[threads(TCompress, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.2) as u64))]
    countof_threads_compress: BoundedU64<1, { u64::MAX }>,

    #[mem(MStreamBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.5) as u64))]
    sizeof_stream_buffer: ByteSize,

    #[mem(MCompressBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.25) as u64))]
    sizeof_compress_buffer: ByteSize,
    #[mem(MCompressRawBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.25) as u64))]
    sizeof_compress_raw_buffer: ByteSize,
}

impl GetRawCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let budget = GetrawBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to determine available parallelism, using 6 threads");
                        6
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to convert parallelism to valid thread count, using 6 threads");
                        6.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_countof_threads_read(self.countof_threads_read)
            .maybe_countof_threads_debarcode(self.countof_threads_debarcode)
            .maybe_countof_threads_sort(self.countof_threads_sort)
            .maybe_countof_threads_write(self.countof_threads_write)
            .maybe_countof_threads_compress(self.countof_threads_compress)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .maybe_sizeof_compress_buffer(self.sizeof_compress_buffer)
            .maybe_sizeof_compress_raw_buffer(self.sizeof_compress_raw_buffer)
            .build();

        budget.log();
        if self.compression_level.level() == 0 {
            warn!("Compression level is 0 (uncompressed)")
        }

        let mut vec_input_debarcode_merge = self.skip_debarcode.clone().unwrap_or(Vec::new());

        if self.paths_out.is_empty() {
            error!("No valid output file paths specified. All output paths failed verification.");
            panic!("No valid output file paths specified");
        }

        if self.paths_hist.is_some()
            && self.paths_hist.as_ref().unwrap().len() != self.paths_out.len()
        {
            let n_hist = self.paths_hist.as_ref().unwrap().len();
            let n_out = self.paths_out.len();
            error!("Number of histogram paths ({n_hist}) does not match number of output paths ({n_out})");
            panic!("Histogram paths count mismatch");
        }

        let timestamp_temp_files = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp_temp_files = timestamp_temp_files.to_string();

        let library = self.library.clone().unwrap_or(String::from(""));

        let path_temp_dir = if let Some(temp_path) = self.path_temp.clone() {
            temp_path
        } else {
            self.paths_out
                .first()
                .unwrap()
                .path()
                .parent()
                .unwrap_or_else(|| {
                    error!("No valid output parent directory found.");
                    panic!("No valid output parent directory found");
                })
                .to_path_buf()
        };


        //Only perform debarcoding if skipping is disabled
        if vec_input_debarcode_merge.is_empty() {

            //Provide further settings to the chosen chemistry
            let mut chemistry = match &self.chemistry {
                GetRawChemistryCMD::AtrandiWGS { .. } => {
                    GetRawChemistry::AtrandiWGS(DebarcodeAtrandiWGSChemistryIllumina::new())
                }
                GetRawChemistryCMD::AtrandiWGSLR { .. } => {
                    GetRawChemistry::AtrandiWGSLR(DebarcodeAtrandiWGSChemistryLongread::new())
                }
                GetRawChemistryCMD::ParseBio { subchemistry, .. } => {
                    GetRawChemistry::ParseBio(ParseBioChemistry3::new(&subchemistry))
                }
            };
            
            //Check if we have single-end or paired-end data
            let paths_r1 = self.paths_r1.clone();
            let paths_r2 = self.paths_r2.clone();
            if paths_r1.is_empty() {
                error!("No valid input files found. All input files failed to open or do not exist.");
                panic!("No valid input files found");
            }

            let ((r1_rx, r2_rx), (r1_handle, r2_handle)) = if paths_r2.len()==0 {
                //No R2 files given ==> this must be single-end input

                //////////// For the given chemistry, check the read content (single-end version)
                {
                    info!("Preparing chemistry...");
                    let input_r1 = paths_r1.first().unwrap();
                    let b1 = sample_reads(input_r1, &self, &budget, "R1");
                    let mut b2 = Vec::new();
                    for _i in 0..b1.len() {
                        b2.push(bascet_io::parse::fastq::OwnedRecord::empty());
                    }
                    chemistry.prepare_using_rp_vecs(b1, b2)?;
                }
                info!("Finished preparing chemistry...");

                //////////// Prepare readers to process the full file (single-end version)
                spawn_single_readers(paths_r1, &budget, self.sizeof_stream_arena)

            } else {
                //Both R1 and R2 ==> this must be paired-end input
                if paths_r1.len() != paths_r2.len() {
                    panic!("Both R1 and R2 specified but lists are of different length")
                }
                let vec_input: Vec<(InputPath, InputPath)> = izip!(paths_r1, paths_r2).collect();

                //////////// For the given chemistry, check the read content (paired-end version)
                {
                    info!("Preparing chemistry...");
                    let (input_r1, input_r2) = &vec_input.first().unwrap();
                    let b1 = sample_reads(input_r1, &self, &budget, "R1");
                    let b2 = sample_reads(input_r2, &self, &budget, "R2");
                    chemistry.prepare_using_rp_vecs(b1, b2)?;
                }
                info!("Finished preparing chemistry...");

                //////////// Prepare readers to process the full file (paired-end version)
                spawn_paired_readers(vec_input, &budget, self.sizeof_stream_arena)
            };


            let (rp_rx, rt_handle) = spawn_debarcode_router(r1_rx, r2_rx, &budget);
            let (db_rx, db_handles, chemistry) = spawn_debarcode_workers(rp_rx, chemistry, &budget);

            let (ct_rx, ct_handle) = spawn_collector(db_rx, &budget);
            let (st_rx, st_handles) = spawn_sort_workers(ct_rx, chemistry, &budget);

            let wt_handles = spawn_chunk_writers(
                st_rx,
                timestamp_temp_files.clone(),
                path_temp_dir.clone(),
                &budget,
                self.compression_level,
                &library,
            );

            info!("Waiting for R1 and R2 reader threads to finish...");
            r1_handle.join().expect("R1 reader thread panicked");
            r2_handle.join().expect("R2 reader thread panicked");
            info!("R1 and R2 reader threads finished");

            ////////////////// The rest here is in common

            info!("Waiting for router thread to finish...");
            rt_handle.join().expect("Router thread panicked");
            info!("Router thread finished");

            debug!(
                "Waiting for {} debarcode worker threads to finish...",
                db_handles.len()
            );
            for (i, handle) in IntoIterator::into_iter(db_handles).enumerate() {
                handle
                    .join()
                    .expect(&format!("Worker thread {} panicked", i));
            }
            debug!("All debarcode worker threads finished");

            debug!("Waiting for collector thread to finish...");
            ct_handle.join().expect("Collector thread panicked");
            debug!("Collector thread finished");

            debug!(
                "Waiting for {} sort worker threads to finish...",
                st_handles.len()
            );
            for (i, handle) in IntoIterator::into_iter(st_handles).enumerate() {
                handle
                    .join()
                    .expect(&format!("Sort worker thread {} panicked", i));
            }
            debug!("All sort worker threads finished");

            debug!(
                "Waiting for {} chunk writer threads to finish...",
                wt_handles.len()
            );
            for (i, handle) in wt_handles.into_iter().enumerate() {
                let paths: Vec<InputPath> = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i));

                vec_input_debarcode_merge.extend(paths);
            }
            debug!(
                "All chunk writer threads finished. Total chunks: {}",
                vec_input_debarcode_merge.len()
            );
        }

        do_merging(
            &self,
            &budget,
            &path_temp_dir,
            &timestamp_temp_files,
            &vec_input_debarcode_merge
        )?;

        Ok(())
    }
}



/// 
/// Given R1 and R2 input paths, spawn readers
/// 
fn do_merging(
    s: &GetRawCMD, 
    budget: &GetrawBudget, 
    path_temp_dir: &PathBuf,
    timestamp_temp_files: &String,
    vec_input_debarcode_merge: &Vec<InputPath>
) -> anyhow::Result<()> {
    let countof_merge_streams = (*budget.threads::<Total>()).get() as usize;
    let vec_input_debarcode_merge = vec_input_debarcode_merge.clone();

    let mergeround_target_count = s.paths_out.len();
    let mut mergeround_counter = 1;
    let mut mergeround_merge_next = vec_input_debarcode_merge;

    while mergeround_merge_next.len() > mergeround_target_count {
        let current_count = mergeround_merge_next.len();

        info!(
            starting_with = current_count,
            target = mergeround_target_count,
            merge_streams = countof_merge_streams,
            "Mergesort round {mergeround_counter}"
        );

        let mut vec_next_round: Vec<InputPath> = Vec::new();
        let mut batch_idx = 0;

        let countof_merged_outputs =
            (current_count + countof_merge_streams - 1) / countof_merge_streams;
        let countof_passthrough = if countof_merged_outputs < mergeround_target_count {
            mergeround_target_count - countof_merged_outputs
        } else {
            0
        };

        let countof_to_merge = current_count - countof_passthrough;
        let (vec_to_merge, vec_passthrough) = mergeround_merge_next.split_at(countof_to_merge);

        for path in vec_passthrough {
            vec_next_round.push(path.clone());
        }

        for batch in vec_to_merge.chunks(countof_merge_streams) {
            if batch.len() == 1 {
                vec_next_round.push(batch[0].clone());
                continue;
            }

            let temp_fname =
                format!("{}_{mergeround_counter}_{batch_idx}", timestamp_temp_files);
            let temp_pathbuf = path_temp_dir.join(temp_fname).with_extension("tirp.bbgz");

            let temp_output_path = match OutputPath::try_from(&temp_pathbuf) {
                Ok(path) => path,
                Err(e) => {
                    error!(path = ?temp_pathbuf, error = %e, "Failed to create output path");
                    panic!("Failed to create output path");
                }
            };

            let vec_batch = batch.to_vec();
            let vec_batch_paths: Vec<_> =
                vec_batch.iter().map(|p| p.path().to_path_buf()).collect();

            spawn_mergesort_workers(
                vec_batch,
                temp_output_path,
                path_temp_dir.clone(),
                &budget,
                s.sizeof_stream_arena,
            );

            for path in vec_batch_paths {
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(path = ?path, error = %e, "Failed to delete merged file");
                }
            }

            let temp_input_path = match InputPath::try_from(&temp_pathbuf) {
                Ok(path) => path,
                Err(e) => panic!("{e}"),
            };
            vec_next_round.push(temp_input_path);
            batch_idx += 1;
        }

        debug!("Finished mergesort round {mergeround_counter}");

        mergeround_merge_next = vec_next_round;

        info!(
            "Mergesort round {}: Finished with {} files",
            mergeround_counter,
            mergeround_merge_next.len()
        );
        mergeround_counter += 1;
    }

    let mut output_paths = Vec::new();
    for (final_path, output_path) in izip!(&mergeround_merge_next, &s.paths_out) {
        match std::fs::rename(&**final_path.path(), &**output_path.path()) {
            Ok(_) => {
                debug!("Moved {final_path} -> {output_path}");
                output_paths.push(output_path.clone());
            }
            Err(e) => {
                warn!(error = %e, "Failed moving {final_path:?} > {output_path:?}");
                let output_path = match OutputPath::try_from(&**final_path.path()) {
                    Ok(path) => path,
                    Err(e) => panic!("{e}"),
                };
                output_paths.push(output_path);
            }
        }
    }

    let output_hist_pairs: Vec<(OutputPath, OutputPath)> = output_paths
        .into_iter()
        .enumerate()
        .map(|(i, output_path)| {
            let hist_path = if let Some(ref hist_paths) = s.paths_hist {
                hist_paths[i].clone()
            } else {
                match OutputPath::try_from(&format!(
                    "{}.hist",
                    output_path.path().path().display()
                )) {
                    Ok(path) => path,
                    Err(e) => panic!("{e}, {:?}.hist", output_path.path().path().display()),
                }
            };
            (output_path, hist_path)
        })
        .collect();

    let hist_handles =
        spawn_histogram_workers(output_hist_pairs, &budget, s.sizeof_stream_arena);

    for (i, handle) in hist_handles.into_iter().enumerate() {
        handle
            .join()
            .expect(&format!("Histogram worker thread {} panicked", i));
    }
    debug!("All histogram worker threads finished");

    Ok(())

}




/// 
/// Given R1 and R2 input paths, spawn paired readers
/// 
fn spawn_paired_readers(
    vec_input: Vec<(InputPath, InputPath)>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
) -> (
    (Receiver<fastq::Record>, Receiver<fastq::Record>),
    (JoinHandle<()>, JoinHandle<()>),
) {
    let (r1_tx, r1_rx) = crossbeam::channel::unbounded();
    let (r2_tx, r2_rx) = crossbeam::channel::unbounded();
    let arc_vec_input = Arc::new(vec_input);
    let countof_threads_read = (*budget.threads::<TRead>()).get();
    let stream_each_n_threads = BoundedU64::new_saturating(countof_threads_read / 2);
    let sizeof_stream_each_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);
    let r1_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
    let r2_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));

    let input_r1 = Arc::clone(&arc_vec_input);
    let handle_r1 = budget.spawn::<TRead, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R1 reader");

        for (input_r1, _) in &*input_r1 {
            let d1 = codec::bgzf::Bgzf::builder()
                .with_path(&**input_r1.path())
                .countof_threads(stream_each_n_threads)
                .build();
            let p1 = parse::Fastq::builder().build();

            let mut s1 = Stream::builder()
                .with_decoder(d1)
                .with_parser(p1)
                .with_opt_decode_arena_pool(Arc::clone(&r1_shared_alloc))
                .build();

            let mut q1 = s1.query::<fastq::Record>();

            while let Ok(Some(record)) = q1.next() {
                let _ = r1_tx.send(record);
            }
            debug!("R1 finished reading");
        }
    });

    let input_r2 = Arc::clone(&arc_vec_input);
    let handle_r2 = budget.spawn::<TRead, _, _>(1, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R2 reader");

        for (_, input_r2) in &*input_r2 {
            let d2 = codec::bgzf::Bgzf::builder()
                .with_path(&**input_r2.path())
                .countof_threads(stream_each_n_threads)
                .build();
            let p2 = parse::Fastq::builder().build();

            let mut s2 = Stream::builder()
                .with_decoder(d2)
                .with_parser(p2)
                .with_opt_decode_arena_pool(Arc::clone(&r2_shared_alloc))
                .build();

            let mut q2 = s2.query::<fastq::Record>();

            while let Ok(Some(record)) = q2.next() {
                let _ = r2_tx.send(record);
            }
            debug!("R2 finished reading");
        }
    });

    return ((r1_rx, r2_rx), (handle_r1, handle_r2));
}



/// 
/// Given R1 input path, spawn single-end readers
/// 
/// TODO is this a good way?
/// 
fn spawn_single_readers(
    vec_input: Vec<InputPath>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
) -> (
    (Receiver<fastq::Record>, Receiver<fastq::Record>),
    (JoinHandle<()>, JoinHandle<()>),
) {
    let (r1_tx, r1_rx) = crossbeam::channel::unbounded();
    let (r2_tx, r2_rx) = crossbeam::channel::unbounded();
    let arc_vec_input = Arc::new(vec_input);
    let countof_threads_read = (*budget.threads::<TRead>()).get();
    let stream_each_n_threads = BoundedU64::new_saturating(countof_threads_read / 2);
    let sizeof_stream_each_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);
    let r1_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
    //let r2_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));

    let input_r1 = Arc::clone(&arc_vec_input);
    let handle_r1 = budget.spawn::<TRead, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R1 reader");

        for input_r1 in &*input_r1 {
            let d1 = codec::bgzf::Bgzf::builder()
                .with_path(&**input_r1.path())
                .countof_threads(stream_each_n_threads)
                .build();
            let p1 = parse::Fastq::builder().build();

            let mut s1 = Stream::builder()
                .with_decoder(d1)
                .with_parser(p1)
                .with_opt_decode_arena_pool(Arc::clone(&r1_shared_alloc))
                .build();

            let mut q1 = s1.query::<fastq::Record>();

            while let Ok(Some(record)) = q1.next() {
                let _ = r1_tx.send(record);
                let dummy_record_r2 = bascet_io::parse::fastq::Record::empty();
                let _ = r2_tx.send(dummy_record_r2); 
            }
            debug!("R1 finished reading");
        }
    });

    let handle_r2 = budget.spawn::<TRead, _, _>(0, move || {
    });

    return ((r1_rx, r2_rx), (handle_r1, handle_r2));
}






///
/// Route inputs from two readers into a stream of paired end
/// 
fn spawn_debarcode_router(
    r1_rx: Receiver<fastq::Record>,
    r2_rx: Receiver<fastq::Record>,
    budget: &GetrawBudget,
) -> (Receiver<(fastq::Record, fastq::Record)>, JoinHandle<()>) {
    let (rp_tx, rp_rx) = crossbeam::channel::unbounded();
    let rt_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting debarcode router");

        loop {
            match (r1_rx.recv(), r2_rx.recv()) {
                (Ok(r1), Ok(r2)) => {
                    let _ = rp_tx.send((r1, r2));
                }
                (Err(_), Err(_)) => {
                    debug!("Both R1 and R2 channels closed, router finishing");
                    break;
                }
                (Ok(_), Err(_)) => {
                    warn!("R2 channel closed but R1 still has data");
                    break;
                }
                (Err(_), Ok(_)) => {
                    warn!("R1 channel closed but R2 still has data");
                    break;
                }
            }
        }
    });

    return (rp_rx, rt_handle);
}



/// 
/// Sample a couple of reads for the purpose of analyzing the content
/// 
fn sample_reads(
    input_path: &InputPath, 
    s: &GetRawCMD, 
    budget: &GetrawBudget, 
    readname: &str
) -> Vec<fastq::OwnedRecord> {

    // NOTE fine to use all threads briefly. Nothing else does work yet.
    let countof_threads_total = (*budget.threads::<Total>()).get();
    // prepare chemistry using r2
    let decoder = codec::BBGZDecoder::builder()
        .with_path(input_path.path().path())
        // SAFETY   budget.threads::<Total>() is 7..
        .countof_threads(unsafe { BoundedU64::new_unchecked(countof_threads_total) })
        .build();

    let p1 = parse::Fastq::builder().build();

    let mut streamer = Stream::builder()
        .with_decoder(decoder)
        .with_parser(p1)
        .sizeof_decode_arena(s.sizeof_stream_arena)
        .sizeof_decode_buffer(*budget.mem::<MStreamBuffer>())
        .build();

    let mut q1 = streamer.query::<fastq::Record>();

    let mut list_reads: Vec<fastq::OwnedRecord> = Vec::with_capacity(10000);
    while let Ok(Some(token)) = q1.next() {
        list_reads.push(token.into());

        if list_reads.len() >= 10000 {
            break;
        }
    }

    info!("Finished reading first 10000 reads of {}...", readname);
    unsafe {
        streamer.shutdown();
    }
    list_reads    
}




///
/// Spawn workers, receiving readpairs and debarcoding/trimming them all
/// 
fn spawn_debarcode_workers(
    rp_rx: Receiver<(fastq::Record, fastq::Record)>,
    chemistry: GetRawChemistry,
    budget: &GetrawBudget,
) -> (
    Receiver<(u32, DebarcodedRecord)>,
    Vec<JoinHandle<()>>,
    GetRawChemistry,
) {
    let countof_threads_debarcode = (*budget.threads::<TDebarcode>()).get();
    let mut thread_handles = Vec::with_capacity(countof_threads_debarcode as usize);
    let (ct_tx, ct_rx) = crossbeam::channel::unbounded();

    let atomic_total_counter = Arc::new(AtomicUsize::new(0));
    let atomic_success_counter = Arc::new(AtomicUsize::new(0));

    for thread_idx in 0..countof_threads_debarcode {
        let mut chemistry = chemistry.clone();
        let rp_rx = rp_rx.clone();
        let ct_tx = ct_tx.clone();

        let thread_atomic_total_counter = Arc::clone(&atomic_total_counter);
        let thread_atomic_success_counter = Arc::clone(&atomic_success_counter);

        let thread_handle = budget.spawn::<TDebarcode, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, "Starting debarcode worker");

            while let Ok((r1, r2)) = rp_rx.recv() {
                // TODO: optimisation: barcodes are fixed-size if represented in a non-string way (e.g as u64)
                let (bc_index, rp) = chemistry.detect_barcode_and_trim(
                    r1.get_ref::<R0>(),
                    r1.get_ref::<Q0>(),
                    r2.get_ref::<R0>(),
                    r2.get_ref::<Q0>(),
                );

                let thread_total_counter =
                    thread_atomic_total_counter.fetch_add(1, Ordering::Relaxed) + 1;

                //Keep read if ok
                if bc_index != u32::MAX {
                    let thread_success_counter =
                        thread_atomic_success_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    if thread_success_counter % 1_000_000 == 0 {
                        info!(
                            "{:.2}M/{:.2}M reads successfully debarcoded",
                            thread_success_counter as f64 / 1_000_000.0,
                            thread_total_counter as f64 / 1_000_000.0
                        );
                    }

                    // SAFETY: safe since these are slices into the same data
                    let mut db_record = unsafe {
                        DebarcodedRecord {
                            id: &[],
                            r1: std::mem::transmute(rp.r1),
                            r2: std::mem::transmute(rp.r2),
                            q1: std::mem::transmute(rp.q1),
                            q2: std::mem::transmute(rp.q2),
                            umi: std::mem::transmute(rp.umi),
                            arena_backing: smallvec::SmallVec::new(),
                        }
                    };
                    bascet_core::PushBacking::<fastq::Record, _>::push_backing(
                        &mut db_record,
                        r1.take_backing(),
                    );
                    bascet_core::PushBacking::<fastq::Record, _>::push_backing(
                        &mut db_record,
                        r2.take_backing(),
                    );
                    let _ = ct_tx.send((bc_index, db_record));
                }
            }
        });

        thread_handles.push(thread_handle);
    }

    drop(ct_tx);
    return (ct_rx, thread_handles, chemistry);
}



///
/// Spawn collector, taking debarcoded/trimmed readers and collecting them for the next step
/// 
fn spawn_collector(
    db_rx: Receiver<(u32, DebarcodedRecord)>,
    budget: &GetrawBudget,
) -> (Receiver<Vec<(u32, DebarcodedRecord)>>, JoinHandle<()>) {
    let (ct_tx, ct_rx) = crossbeam::channel::unbounded();
    let sizeof_each_sort_alloc = ByteSize::mib(512);
    let mut countof_each_sort_alloc = 0;

    debug!(sizeof_each_sort_alloc = %sizeof_each_sort_alloc, "sizeof_each_sort_alloc");
    let ct_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting collector");

        let mut collection_buffer: Vec<(u32, DebarcodedRecord)> =
            Vec::with_capacity(countof_each_sort_alloc);
        let mut sizeof_sort_alloc = ByteSize(0);
        let timeout = std::time::Duration::from_secs(4);

        loop {
            match db_rx.recv_timeout(timeout) {
                Ok((bc_index, db_record)) => {
                    let cell_mem_size = ByteSize(
                        (db_record.get_ref::<Id>().len()
                            + db_record.get_ref::<R1>().len()
                            + db_record.get_ref::<R2>().len()
                            + db_record.get_ref::<Q1>().len()
                            + db_record.get_ref::<Q2>().len()
                            + db_record.get_ref::<Umi>().len()) as u64,
                    );

                    if cell_mem_size + sizeof_sort_alloc > sizeof_each_sort_alloc {
                        let sizeof_mean_sort_alloc =
                            sizeof_sort_alloc.as_u64() / collection_buffer.len() as u64;
                        let _ = ct_tx.send(collection_buffer);
                        countof_each_sort_alloc =
                            (sizeof_sort_alloc.as_u64() / sizeof_mean_sort_alloc) as usize;

                        collection_buffer = Vec::with_capacity(countof_each_sort_alloc);
                        sizeof_sort_alloc = ByteSize(0);
                    }
                    // NOTE 80-90% of time spent in this thread is spent on pushing data
                    // TODO [GOOD FIRST ISSUE] improve performance by recycling memory
                    collection_buffer.push((bc_index, db_record));
                    sizeof_sort_alloc += cell_mem_size;
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !collection_buffer.is_empty() {
                        let sizeof_mean_sort_alloc =
                            sizeof_sort_alloc.as_u64() / collection_buffer.len() as u64;
                        let _ = ct_tx.send(collection_buffer);
                        countof_each_sort_alloc =
                            (sizeof_sort_alloc.as_u64() / sizeof_mean_sort_alloc) as usize;

                        collection_buffer = Vec::with_capacity(countof_each_sort_alloc);
                        sizeof_sort_alloc = ByteSize(0);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }

        if !collection_buffer.is_empty() {
            let _ = ct_tx.send(collection_buffer);
        }
    });

    return (ct_rx, ct_handle);
}

///
/// Spawn sorters. By sorting chunks of reads while they are already in memory, the first major sort pass will already be done in the first write to disk
/// 
fn spawn_sort_workers(
    ct_rx: Receiver<Vec<(u32, DebarcodedRecord)>>,
    chemistry: GetRawChemistry,
    budget: &GetrawBudget,
) -> (
    Receiver<Vec<(Vec<u8>, DebarcodedRecord)>>,
    Vec<JoinHandle<()>>,
) {
    let countof_threads_sort = (*budget.threads::<TSort>()).get();
    let mut thread_handles = Vec::with_capacity(countof_threads_sort as usize);
    let (st_tx, st_rx) = crossbeam::channel::unbounded();

    for thread_idx in 0..countof_threads_sort {
        let ct_rx = ct_rx.clone();
        let st_tx = st_tx.clone();
        let thread_chemistry = chemistry.clone();

        let thread_handle = budget.spawn::<TSort, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, "Starting sort worker");

            while let Ok(vec_bc_indices_db_records) = ct_rx.recv() {
                // HACK: Convert barcode before sorting for correct ordering
                // NOTE: sort in descending order to be able to pop off the end (O(1) rather than O(n))
                // NOTE: to save memory conversion to owned cells is NOT done via map but rather by popping
                let mut records_with_bc: Vec<(Vec<u8>, DebarcodedRecord)> =
                    IntoIterator::into_iter(vec_bc_indices_db_records)
                        .map(|(bc_index, db_record)| {
                            let id_as_bc = thread_chemistry.bcindexu32_to_bcu8(&bc_index).to_vec();
                            (id_as_bc, db_record)
                        })
                        .collect();

                glidesort::sort_by(&mut records_with_bc, |(bc_a, _), (bc_b, _)| {
                    Ord::cmp(bc_a, bc_b)
                });

                let _ = st_tx.send(records_with_bc);
            }
        });
        thread_handles.push(thread_handle);
    }

    drop(st_tx);
    return (st_rx, thread_handles);
}



fn spawn_chunk_writers(
    st_rx: Receiver<Vec<(Vec<u8>, DebarcodedRecord)>>,
    timestamp_temp_files: String,
    path_temp_dir: PathBuf,
    budget: &GetrawBudget,
    compression_level: Compression,
    library: &str,
) -> Vec<JoinHandle<Vec<InputPath>>> {
    let countof_write_threads = (*budget.threads::<TWrite>()).get();
    let countof_compress_threads = (*budget.threads::<TCompress>()).get();
    let countof_write_each_compress_threads =
        BoundedU64::new_saturating(countof_compress_threads / countof_write_threads);
    let sizeof_write_each_compress_buffer =
        ByteSize(budget.mem::<MCompressBuffer>().as_u64() / countof_write_threads);
    let sizeof_write_each_compress_raw_buffer =
        ByteSize(budget.mem::<MCompressRawBuffer>().as_u64() / countof_write_threads);
    let mut thread_handles = Vec::with_capacity(countof_write_threads as usize);
    let atomic_counter = Arc::new(AtomicUsize::new(0));

    let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
    let arc_library: Arc<str> = Arc::from(library);
    for thread_idx in 0..countof_write_threads {
        let st_rx = st_rx.clone();

        let thread_counter = Arc::clone(&atomic_counter);
        let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
        let thread_path_temp_dir = path_temp_dir.clone();
        let thread_library = Arc::clone(&arc_library);
        let mut thread_vec_temp_written = Vec::new();
        let thread_handle = budget.spawn::<TWrite, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, "Starting chunk writer");

            // Reuse arena pools across all chunks in this thread
            let thread_shared_raw_arena = Arc::new(ArenaPool::new(sizeof_write_each_compress_raw_buffer, codec::bbgz::MAX_SIZEOF_BLOCK));
            let thread_shared_compression_arena = Arc::new(ArenaPool::new(sizeof_write_each_compress_buffer, codec::bbgz::MAX_SIZEOF_BLOCK));

            while let Ok(sorted_record_list) = st_rx.recv() {
                let thread_counter = thread_counter.fetch_add(1, Ordering::Relaxed);
                let temp_fname = format!(
                    "{}_merge_0_{thread_counter}",
                    *thread_timestamp_temp_files
                );
                let temp_pathbuf = thread_path_temp_dir
                    .join(temp_fname)
                    .with_extension("tirp.bbgz");

                let temp_output_path = match OutputPath::try_from(&temp_pathbuf) {
                    Ok(path) => path,
                    Err(e) => {
                        error!(path = ?temp_pathbuf, error = %e, "Failed to create output path");
                        panic!("Failed to create output path");
                    }
                };

                let temp_output_file = match temp_output_path.create() {
                    Ok(file) => {
                        file
                    },
                    Err(e) => {
                        error!(path = ?temp_pathbuf, error = %e, "Failed to create output file");
                        panic!("Failed to create output file");
                    }
                };

                let mut bbgzwriter = BBGZWriter::builder()
                    .countof_threads(countof_write_each_compress_threads)
                    .compression_level(compression_level)
                    .with_opt_raw_arena_pool(Arc::clone(&thread_shared_raw_arena))
                    .with_opt_compression_arena_pool(Arc::clone(&thread_shared_compression_arena))
                    .with_writer(temp_output_file)
                    .build();

                let mut records_writen = 0;
                let mut last_id: SmallVec<[u8; 16]> = SmallVec::new();
                let mut blockwriter_opt: Option<BBGZWriteBlock<'_>> = None;

                let library_bytes = thread_library.as_bytes();
                let library_sep = if thread_library.is_empty() { ""} else { "_" };
                let library_sep_bytes = library_sep.as_bytes();

                for (id, mut record) in sorted_record_list {
                    if *id != *last_id {
                        if let Some(ref mut blockwriter) = blockwriter_opt {
                            blockwriter.flush().unwrap();
                        }
                        last_id = id.to_smallvec();

                        let mut prefixed_id = Vec::with_capacity(library_bytes.len() + library_sep_bytes.len() + id.len());
                        prefixed_id.extend_from_slice(library_bytes);
                        prefixed_id.extend_from_slice(library_sep_bytes);
                        prefixed_id.extend_from_slice(&id);

                        let mut bbgzheader = BBGZHeader::new();
                        unsafe {
                            bbgzheader.add_extra_unchecked(b"ID", prefixed_id);
                        }
                        blockwriter_opt = Some(bbgzwriter.begin(bbgzheader));
                    }

                    // SAFETY: safe because blockwriter is COW
                    *record.get_mut::<Id>() = unsafe { std::mem::transmute(last_id.as_slice()) };
                    if let Some(ref mut blockwriter) = blockwriter_opt {
                        let id_bytes = record.as_bytes::<Id>();
                        let r1_bytes = record.as_bytes::<R1>();
                        let r2_bytes = record.as_bytes::<R2>();
                        let q1_bytes = record.as_bytes::<Q1>();
                        let q2_bytes = record.as_bytes::<Q2>();
                        let umi_bytes = record.as_bytes::<Umi>();

                        // Reserve space for entire record to prevent splitting across blocks
                        let record_size = 11 + // 8x '\t' + '1' + '1' + '\n'
                            library_bytes.len() +
                            library_sep_bytes.len() + 
                            id_bytes.len() +
                            r1_bytes.len() +
                            r2_bytes.len() +
                            q1_bytes.len() +
                            q2_bytes.len() +
                            umi_bytes.len();
                        blockwriter.reserve(record_size);

                        let _ = blockwriter.write_all(library_bytes);
                        let _ = blockwriter.write_all(library_sep_bytes);
                        let _ = blockwriter.write_all(id_bytes);
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(b"1");
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(b"1");
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(r1_bytes);
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(r2_bytes);
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(q1_bytes);
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(q2_bytes);
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(umi_bytes);
                        let _ = blockwriter.write_all(b"\n");
                        records_writen += 1;
                    }
                }

                if let Some(ref mut blockwriter) = blockwriter_opt {
                    blockwriter.flush().unwrap();
                }

                let temp_input_path = match InputPath::try_from(&temp_pathbuf) {
                    Ok(path) => path,
                    Err(e) => panic!("{}", e)
                };
                debug!(path = ?temp_pathbuf, records_written = records_writen, "Wrote debarcoded cell chunk");
                thread_vec_temp_written.push(temp_input_path);
            }
            return thread_vec_temp_written;
        });
        thread_handles.push(thread_handle);
    }

    return thread_handles;
}

fn spawn_mergesort_workers(
    paths_in: Vec<InputPath>,
    path_out: OutputPath,
    path_temp: PathBuf,
    budget: &GetrawBudget,
    sizeof_stream_arena: ByteSize,
) {
    let mut shardify_cmd = ShardifyCMD {
        paths_in,
        paths_out: vec![path_out],
        path_include: None,
        path_temp: Some(path_temp),
        total_threads: Some(BoundedU64::new_saturating(
            (*budget.threads::<Total>()).get(),
        )),
        numof_threads_write: None,
        total_mem: *budget.mem::<Total>(),
        sizeof_stream_buffer: None,
        sizeof_stream_arena,

        show_filter_warning: false,
        show_startup_message: true,
    };

    if let Err(e) = shardify_cmd.try_execute() {
        error!(error = %e, "Shardify merge failed");
        panic!("Shardify merge failed");
    }
}

///
/// Spawn workers that generate histograms of cellid vs 
/// 
fn spawn_histogram_workers(
    output_hist_pairs: Vec<(OutputPath, OutputPath)>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
) -> Vec<JoinHandle<()>> {
    let countof_histograms = output_hist_pairs.len();
    if countof_histograms == 0 {
        return Vec::new();
    }

    let countof_threads_total: u64 = (*budget.threads::<Total>()).get();
    let countof_worker_threads = (countof_histograms as u64).min(countof_threads_total);
    let countof_threads_per_worker =
        BoundedU64::new_saturating(countof_threads_total / countof_worker_threads);

    let sizeof_stream_each_buffer =
        ByteSize(budget.mem::<MStreamBuffer>().as_u64() / countof_worker_threads);
    let mut thread_handles = Vec::with_capacity(countof_worker_threads as usize);

    for (thread_idx, (output_path, hist_path)) in output_hist_pairs.into_iter().enumerate() {
        let thread_shared_arena = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
        let thread_countof_threads = countof_threads_per_worker;

        let worker_handle = budget.spawn::<Total, _, _>(thread_idx as u64, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, processing_histogram_for = %output_path, "Starting histogram worker");
            let mut hist_hashmap: gxhash::HashMap<Vec<u8>, u64> = gxhash::HashMap::new();

            let decoder = codec::BBGZDecoder::builder()
                .with_path(&**output_path.path())
                .countof_threads(thread_countof_threads)
                .build();
            let parser = parse::Tirp::builder().build();

            let mut stream = Stream::builder()
                .with_decoder(decoder)
                .with_parser(parser)
                .with_opt_decode_arena_pool(thread_shared_arena)
                .build();

            let mut query = stream
                .query::<tirp::Record>()
                .assert_with_context::<Id, Id, _>(
                    |id_current: &&'static [u8], id_context: &&'static [u8]| {
                        id_current >= id_context
                    },
                    "id_current < id_context",
                );

            while let Ok(Some(record)) = query.next() {
                let id = record.get_ref::<Id>();
                if let Some(count) = hist_hashmap.get_mut(*id) {
                    *count += 1;
                } else {
                    hist_hashmap.insert(id.to_vec(), 1);
                }
            }

            let hist_file = match hist_path.clone().create() {
                Ok(file) => file,
                Err(e) => {
                    error!(path = ?hist_path, error = %e, "Failed to create output file");
                    panic!("Failed to create output file");
                }
            };

            let mut bufwriter = BufWriter::new(hist_file);
            for (id, count) in hist_hashmap.iter() {
                bufwriter.write_all(&id).unwrap();
                bufwriter.write_all(b"\t").unwrap();
                bufwriter.write_all(count.to_string().as_bytes()).unwrap();
                bufwriter.write_all(b"\n").unwrap();
            }

            bufwriter.flush().unwrap();
            debug!("Wrote histogram at {}", hist_path);
        });
        thread_handles.push(worker_handle);
    }

    thread_handles
}

#[derive(Composite, Default, Serialize)]
#[bascet(attrs = (Id, R1, R2, Q1, Q2, Umi), backing = ArenaBacking, marker = AsRecord)]
pub struct DebarcodedRecord {
    id: &'static [u8],
    r1: &'static [u8],
    r2: &'static [u8],
    q1: &'static [u8],
    q2: &'static [u8],
    umi: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    #[serde(skip)]
    arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}