use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bascet_io::fastq::fastq;
use bascet_io::tirp::tirp;
use bascet_io::{BBGZHeaderBase, BBGZTrailer, BBGZWriteBlock, MAX_SIZEOF_BLOCKusize, SIZEOF_MARKER_DEFLATE_ALIGN_BYTESusize};
use blart::AsBytes;
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::{Args, Subcommand};
use clio::{InputPath, OutputPath};
use crossbeam::channel::{Receiver, RecvTimeoutError};
use gxhash::HashMapExt;
use itertools::{izip, Itertools};

use bascet_core::*;
use bascet_derive::Budget;
use bascet_io::{
    codec::{self, bbgz},
    parse, BBGZHeader, BBGZWriter,
};
use serde::Serialize;
use smallvec::{SmallVec, ToSmallVec};

use crate::barcode::{Chemistry, CombinatorialBarcode8bp, ParseBioChemistry3};
use crate::{bbgz_compression_parser, bounded_parser};
use crate::{common, log_critical, log_info, log_warning};

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
        long = "countof-threads-mergesort",
        help = "Number of second-phase sorting threads",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    countof_threads_mergesort: Option<BoundedU64<2, { u64::MAX }>>,

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
    // 1 prev 3634s
    // 2 prev 3698s
    // 3 prev 
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
        long = "skip-debarcode",
        num_args = 1..,
        value_delimiter = ',',
        help = "Skip debarcoding phase and merge existing chunk files (comma-separated list of chunk files)"
    )]
    pub skip_debarcode: Option<Vec<InputPath>>,

    #[command(subcommand)]
    pub chemistry: GetRawChemistryCMD,
}

#[derive(Subcommand)]
pub enum GetRawChemistryCMD {
    /// AtrandiWGS chemistry, uses combinatorial 8bp barcodes for debarcoding
    AtrandiWGS,
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
    AtrandiWGS(DebarcodeAtrandiWGSChemistry),
    ParseBio(ParseBioChemistry3),
}

#[derive(Budget, Debug)]
struct GetrawBudget {
    #[threads(Total)]
    threads: BoundedU64<6, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.25) as u64))]
    countof_threads_read: BoundedU64<2, { u64::MAX }>,

    #[threads(TDebarcode, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.1) as u64))]
    countof_threads_debarcode: BoundedU64<1, { u64::MAX }>,

    #[threads(TSort, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.3) as u64))]
    countof_threads_sort: BoundedU64<1, { u64::MAX }>,
    #[threads(TMergeSort, |_, _| bounded_integer::BoundedU64::const_new::<4>())]
    countof_threads_mergesort: BoundedU64<2, { u64::MAX }>,

    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.05) as u64))]
    countof_threads_write: BoundedU64<1, { u64::MAX }>,
    #[threads(TCompress, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.3) as u64))]
    countof_threads_compress: BoundedU64<1, { u64::MAX }>,

    #[mem(MStreamBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.5) as u64))]
    sizeof_stream_buffer: ByteSize,

    #[mem(MSortBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.1) as u64))]
    sizeof_sort_buffer: ByteSize,

    #[mem(MCompressBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.2) as u64))]
    sizeof_compress_buffer: ByteSize,
    #[mem(MCompressRawBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.2) as u64))]
    sizeof_compress_raw_buffer: ByteSize,
}

impl GetRawCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let budget = GetrawBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        log_warning!("Failed to determine available parallelism, using 6 threads"; "error" => %e);
                        6
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        log_warning!("Failed to convert parallelism to valid thread count, using 6 threads"; "error" => %e);
                        6.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_countof_threads_read(self.countof_threads_read)
            .maybe_countof_threads_debarcode(self.countof_threads_debarcode)
            .maybe_countof_threads_sort(self.countof_threads_sort)
            .maybe_countof_threads_mergesort(self.countof_threads_mergesort)
            .maybe_countof_threads_write(self.countof_threads_write)
            .maybe_countof_threads_compress(self.countof_threads_compress)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .maybe_sizeof_sort_buffer(self.sizeof_sort_buffer)
            .maybe_sizeof_compress_buffer(self.sizeof_compress_buffer)
            .maybe_sizeof_compress_raw_buffer(self.sizeof_compress_raw_buffer)
            .build();

        budget.validate();

        log_info!(
            "Starting GetRaw";
            "using" => %budget,
        );
        if self.compression_level.level() == 0 {
            log_warning!("Compression level is 0 (uncompressed)")
        }

        let mut vec_input_debarcode_merge = self.skip_debarcode.clone().unwrap_or(Vec::new());

        if self.paths_out.is_empty() {
            log_critical!(
                "No valid output file paths specified. All output paths failed verification."
            );
        }

        if self.paths_hist.is_some()
            && self.paths_hist.as_ref().unwrap().len() != self.paths_out.len()
        {
            let n_hist = self.paths_hist.as_ref().unwrap().len();
            let n_out = self.paths_out.len();
            log_critical!(
                "Number of histogram paths ({n_hist}) does not match number of output paths ({n_out})"
            );
        }

        let timestamp_temp_files = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timestamp_temp_files = timestamp_temp_files.to_string();

        let path_temp_dir = if let Some(temp_path) = self.path_temp.clone() {
            temp_path
        } else {
            self.paths_out
                .first()
                .unwrap()
                .path()
                .parent()
                .unwrap_or_else(|| {
                    log_critical!("No valid output parent directory found.");
                })
                .to_path_buf()
        };

        if vec_input_debarcode_merge.is_empty() {
            let vec_input: Vec<(InputPath, InputPath)> =
                izip!(self.paths_r1.clone(), self.paths_r2.clone()).collect();

            if vec_input.is_empty() {
                log_critical!(
                    "No valid input files found. All input files failed to open or do not exist."
                );
            }

            let mut chemistry = match &self.chemistry {
                GetRawChemistryCMD::AtrandiWGS { .. } => {
                    GetRawChemistry::AtrandiWGS(DebarcodeAtrandiWGSChemistry::new())
                }
                GetRawChemistryCMD::ParseBio { subchemistry, .. } => {
                    GetRawChemistry::ParseBio(ParseBioChemistry3::new(&subchemistry))
                }
            };

            {
                log_info!("Preparing chemistry...");
                let (input_r1, input_r2) = &vec_input.first().unwrap();
                // NOTE fine to use all threads briefly. Nothing else does work yet.
                let countof_threads_total = (*budget.threads::<Total>()).get();
                // prepare chemistry using r2
                let d1 = codec::BBGZDecoder::builder()
                    .with_path(input_r1.path().path())
                    // SAFETY   budget.threads::<Total>() is 7..
                    .countof_threads(unsafe { BoundedU64::new_unchecked(countof_threads_total) })
                    .build();

                let p1 = parse::Fastq::builder().build();

                let mut s1 = Stream::builder()
                    .with_decoder(d1)
                    .with_parser(p1)
                    .sizeof_decode_arena(self.sizeof_stream_arena)
                    .sizeof_decode_buffer(*budget.mem::<MStreamBuffer>())
                    .build();

                let mut q1 = s1.query::<fastq::Record>();

                let mut b1: Vec<fastq::OwnedRecord> = Vec::with_capacity(10000);
                while let Ok(Some(token)) = q1.next() {
                    b1.push(token.into());

                    if b1.len() >= 10000 {
                        break;
                    }
                }

                log_info!("Finished reading first 10000 reads of R1...");
                unsafe {
                    s1.shutdown();
                }

                let d2 = codec::BBGZDecoder::builder()
                    .with_path(input_r2.path().path())
                    // SAFETY   budget.threads::<Total>() is 7..
                    .countof_threads(unsafe { BoundedU64::new_unchecked(countof_threads_total) })
                    .build();
                let p2 = parse::Fastq::builder().build();

                let mut s2 = Stream::builder()
                    .with_decoder(d2)
                    .with_parser(p2)
                    .sizeof_decode_arena(self.sizeof_stream_arena)
                    .sizeof_decode_buffer(*budget.mem::<MStreamBuffer>())
                    .build();

                let mut q2 = s2.query::<fastq::Record>();

                let mut b2: Vec<fastq::OwnedRecord> = Vec::with_capacity(10000);
                while let Ok(Some(token)) = q2.next() {
                    b2.push(token.into());

                    if b2.len() >= 10000 {
                        break;
                    }
                }

                log_info!("Finished reading first 10000 reads of R2...");
                unsafe {
                    s2.shutdown();
                }

                let _ = chemistry.prepare_using_rp_vecs(b1, b2);
            }
            log_info!("Finished preparing chemistry...");
            // std::process::exit(0);
            let ((r1_rx, r2_rx), (r1_handle, r2_handle)) =
                spawn_paired_readers(vec_input, &budget, self.sizeof_stream_arena);

            let (rp_rx, rt_handle) = spawn_debarcode_router(r1_rx, r2_rx, &budget);
            let (db_rx, db_handles, chemistry) = spawn_debarcode_workers(rp_rx, chemistry, &budget);

            let (ct_rx, ct_handle) = spawn_collector(db_rx, &budget);
            let (st_rx, st_handles) = spawn_sort_workers(ct_rx, chemistry, &budget);

            let wt_handles = spawn_chunk_writers(
                st_rx,
                timestamp_temp_files.clone(),
                path_temp_dir.clone(),
                &budget,
            );

            log_info!("Waiting for R1 and R2 reader threads to finish...");
            r1_handle.join().expect("R1 reader thread panicked");
            r2_handle.join().expect("R2 reader thread panicked");
            log_info!("R1 and R2 reader threads finished");

            log_info!("Waiting for router thread to finish...");
            rt_handle.join().expect("Router thread panicked");
            log_info!("Router thread finished");

            log_info!(
                "Waiting for {} debarcode worker threads to finish...",
                db_handles.len()
            );
            for (i, handle) in IntoIterator::into_iter(db_handles).enumerate() {
                handle
                    .join()
                    .expect(&format!("Worker thread {} panicked", i));
            }
            log_info!("All debarcode worker threads finished");

            log_info!("Waiting for collector thread to finish...");
            ct_handle.join().expect("Collector thread panicked");
            log_info!("Collector thread finished");

            log_info!(
                "Waiting for {} sort worker threads to finish...",
                st_handles.len()
            );
            for (i, handle) in IntoIterator::into_iter(st_handles).enumerate() {
                handle
                    .join()
                    .expect(&format!("Sort worker thread {} panicked", i));
            }
            log_info!("All sort worker threads finished");

            log_info!(
                "Waiting for {} chunk writer threads to finish...",
                wt_handles.len()
            );
            for (i, handle) in wt_handles.into_iter().enumerate() {
                let paths: Vec<InputPath> = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i));

                vec_input_debarcode_merge.extend(paths);
            }
            log_info!(
                "All chunk writer threads finished. Total chunks: {}",
                vec_input_debarcode_merge.len()
            );
        }

        let mergeround_target_count = self.paths_out.len();
        let mut mergeround_counter = 1;
        let mut mergeround_merge_next = vec_input_debarcode_merge;

        while mergeround_merge_next.len() > mergeround_target_count {
            let current_count = mergeround_merge_next.len();
            let files_to_merge = current_count - mergeround_target_count;

            log_info!(
                "Mergesort round {mergeround_counter}";
                "Starting with" => format!("{:?} files", current_count),
                "Target" => format!("{:?} files", mergeround_target_count),
                "Remaining merges" => files_to_merge / 2
            );

            let (files_to_merge, files_to_keep): (
                Vec<(usize, InputPath)>,
                Vec<(usize, InputPath)>,
            ) = IntoIterator::into_iter(mergeround_merge_next)
                .enumerate()
                .partition(|(i, _)| *i < files_to_merge * 2);

            let files_to_merge: Vec<InputPath> =
                files_to_merge.into_iter().map(|(_, file)| file).collect();
            let files_to_keep: Vec<InputPath> =
                files_to_keep.into_iter().map(|(_, file)| file).collect();

            let (ms_rx, ms_handles) =
                spawn_mergesort_workers(files_to_merge, &budget, self.sizeof_stream_arena);

            let wt_handles = spawn_mergesort_writers(
                ms_rx,
                timestamp_temp_files.clone(),
                mergeround_counter,
                path_temp_dir.clone(),
                &budget,
            );

            for handle in ms_handles {
                handle.join().unwrap();
            }

            // Collect outputs from current round
            mergeround_merge_next = files_to_keep; // Start with passthrough files
            for (i, handle) in IntoIterator::into_iter(wt_handles).enumerate() {
                let paths: Vec<InputPath> = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i))
                    .into_iter()
                    .collect();

                mergeround_merge_next.extend(paths);
            }

            log_info!(
                "Mergesort round {}: Finished with {} files",
                mergeround_counter,
                mergeround_merge_next.len()
            );
            mergeround_counter += 1;
        }

        let mut output_paths = Vec::new();
        for (final_path, output_path) in izip!(&mergeround_merge_next, &self.paths_out) {
            match std::fs::rename(&**final_path.path(), &**output_path.path()) {
                Ok(_) => {
                    log_info!("Moved {final_path} -> {output_path}");
                    output_paths.push(output_path.clone());
                }
                Err(e) => {
                    log_warning!("Failed moving {final_path:?} > {output_path:?}"; "error" => %e);
                    let output_path = match OutputPath::try_from(&**final_path.path()) {
                        Ok(path) => path,
                        Err(e) => panic!("{e}"),
                    };
                    output_paths.push(output_path);
                }
            }
        }

        // Build (output_path, hist_path) pairs for histogram workers
        let output_hist_pairs: Vec<(OutputPath, OutputPath)> = output_paths
            .into_iter()
            .enumerate()
            .map(|(i, output_path)| {
                let hist_path = if let Some(ref hist_paths) = self.paths_hist {
                    hist_paths[i].clone()
                } else {
                    match OutputPath::try_from(&format!("{}.hist", output_path.path().path().display()))
                    {
                        Ok(path) => path,
                        Err(e) => panic!("{e}, {:?}.hist", output_path.path().path().display()),
                    }
                };
                (output_path, hist_path)
            })
            .collect();

        let hist_handles = spawn_histogram_workers(output_hist_pairs, &budget, self.sizeof_stream_arena);

        log_info!(
            "Waiting for {} histogram worker threads to finish...",
            hist_handles.len()
        );
        for (i, handle) in hist_handles.into_iter().enumerate() {
            handle
                .join()
                .expect(&format!("Histogram worker thread {} panicked", i));
        }
        log_info!("All histogram worker threads finished");

        Ok(())
    }
}

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
    let stream_each_sizeof_arena = ByteSize(stream_arena.as_u64() / 2);
    let stream_each_sizeof_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);

    let input_r1 = Arc::clone(&arc_vec_input);
    let handle_r1 = budget.spawn::<TRead, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting R1 reader"; "thread" => thread_name);

        // Reuse arena pool across all input files in this thread
        let thread_shared_stream_arena = Arc::new(ArenaPool::new(stream_each_sizeof_buffer, stream_each_sizeof_arena));

        for (input_r1, _) in &*input_r1 {
            let d1 = codec::bgzf::Bgzf::builder()
                .with_path(input_r1.path().path())
                .countof_threads(stream_each_n_threads)
                .build();
            let p1 = parse::Fastq::builder().build();

            let mut s1 = Stream::builder()
                .with_decoder(d1)
                .with_parser(p1)
                .with_opt_decode_arena_pool(Arc::clone(&thread_shared_stream_arena))
                .build();

            let mut q1 = s1.query::<fastq::Record>();

            while let Ok(Some(record)) = q1.next() {
                let _ = r1_tx.send(record);
            }
            log_info!("R1 finished reading");
        }
    });

    // let r2_tx = r2_tx.clone();
    let input_r2 = Arc::clone(&arc_vec_input);
    let handle_r2 = budget.spawn::<TRead, _, _>(1, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting R2 reader"; "thread" => thread_name);

        // Reuse arena pool across all input files in this thread
        let thread_shared_stream_arena = Arc::new(ArenaPool::new(stream_each_sizeof_buffer, stream_each_sizeof_arena));

        for (_, input_r2) in &*input_r2 {
            let d2 = codec::bgzf::Bgzf::builder()
                .with_path(input_r2.path().path())
                .countof_threads(stream_each_n_threads)
                .build();
            let p2 = parse::Fastq::builder().build();

            let mut s2 = Stream::builder()
                .with_decoder(d2)
                .with_parser(p2)
                .with_opt_decode_arena_pool(Arc::clone(&thread_shared_stream_arena))
                .build();

            let mut q2 = s2.query::<fastq::Record>();

            while let Ok(Some(record)) = q2.next() {
                let _ = r2_tx.send(record);
            }
            log_info!("R2 finished reading");
        }
    });

    return ((r1_rx, r2_rx), (handle_r1, handle_r2));
}

fn spawn_debarcode_router(
    r1_rx: Receiver<fastq::Record>,
    r2_rx: Receiver<fastq::Record>,
    budget: &GetrawBudget,
) -> (Receiver<(fastq::Record, fastq::Record)>, JoinHandle<()>) {
    let (rp_tx, rp_rx) = crossbeam::channel::unbounded();
    let rt_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting debarcode router"; "thread" => thread_name);

        loop {
            match (r1_rx.recv(), r2_rx.recv()) {
                (Ok(r1), Ok(r2)) => {
                    let _ = rp_tx.send((r1, r2));
                }
                (Err(_), Err(_)) => {
                    log_info!("Both R1 and R2 channels closed, router finishing");
                    break;
                }
                (Ok(_), Err(_)) => {
                    log_warning!("R2 channel closed but R1 still has data");
                    break;
                }
                (Err(_), Ok(_)) => {
                    log_warning!("R1 channel closed but R2 still has data");
                    break;
                }
            }
        }
    });

    return (rp_rx, rt_handle);
}

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
            log_info!("Starting debarcode worker"; "thread" => thread_name);

            while let Ok((r1, r2)) = rp_rx.recv() {
                // TODO: optimisation: barcodes are fixed-size if represented in a non string way (e.g as u64)
                let (bc_index, rp) = chemistry.detect_barcode_and_trim(
                    r1.get_ref::<R0>(),
                    r1.get_ref::<Q0>(),
                    r2.get_ref::<R0>(),
                    r2.get_ref::<Q0>(),
                );

                let thread_total_counter =
                    thread_atomic_total_counter.fetch_add(1, Ordering::Relaxed) + 1;

                if bc_index != u32::MAX {
                    let thread_success_counter =
                        thread_atomic_success_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    if thread_success_counter % 1_000_000 == 0 {
                        log_info!(
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

fn spawn_collector(
    db_rx: Receiver<(u32, DebarcodedRecord)>,
    budget: &GetrawBudget,
) -> (Receiver<Vec<(u32, DebarcodedRecord)>>, JoinHandle<()>) {
    let (ct_tx, ct_rx) = crossbeam::channel::unbounded();
    let countof_threads_sort = (*budget.threads::<TSort>()).get();
    let sizeof_buffer_sort = budget.mem::<MSortBuffer>().as_u64();
    let sizeof_each_sort_alloc = ByteSize(sizeof_buffer_sort / countof_threads_sort);
    let mut countof_each_sort_alloc = 0;

    log_info!("sizeof_each_sort_alloc"; "sizeof_each_sort_alloc" => %sizeof_each_sort_alloc);
    let ct_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting collector"; "thread" => thread_name);

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
            log_info!("Starting sort worker"; "thread" => thread_name);

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
    for thread_idx in 0..countof_write_threads {
        let st_rx = st_rx.clone();

        let thread_counter = Arc::clone(&atomic_counter);
        let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
        let thread_path_temp_dir = path_temp_dir.clone();
        let mut thread_vec_temp_written = Vec::new();
        let thread_handle = budget.spawn::<TWrite, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            log_info!("Starting chunk writer"; "thread" => thread_name);

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
                        log_critical!("Failed to create output path"; "path" => ?temp_pathbuf, "error" => %e);
                    }
                };

                let temp_output_file = match temp_output_path.create() {
                    Ok(file) => {
                        file
                    },
                    Err(e) => {
                        log_critical!("Failed to create output file"; "path" => ?temp_pathbuf, "error" => %e);
                    }
                };

                let bufwriter = BufWriter::with_capacity(
                    ByteSize::mib(1).as_u64() as usize,
                    temp_output_file.clone()
                );
                let mut bbgzwriter = BBGZWriter::builder()
                    .countof_threads(countof_write_each_compress_threads)
                    .with_opt_raw_arena_pool(Arc::clone(&thread_shared_raw_arena))
                    .with_opt_compression_arena_pool(Arc::clone(&thread_shared_compression_arena))
                    .with_writer(bufwriter)
                    .build();

                let mut records_writen = 0;
                let mut last_id: SmallVec<[u8; 16]> = SmallVec::new();
                let mut blockwriter_opt: Option<BBGZWriteBlock<'_>> = None;

                for (id, mut record) in sorted_record_list {
                    if *id != *last_id {
                        if let Some(ref mut blockwriter) = blockwriter_opt {
                            blockwriter.flush().unwrap();
                        }
                        last_id = id.to_smallvec();

                        let mut bbgzheader = BBGZHeader::new();
                        unsafe { 
                            bbgzheader.add_extra_unchecked(b"ID", id.clone());
                        }
                        blockwriter_opt = Some(bbgzwriter.begin(bbgzheader));
                    }

                    // SAFETY: safe because blockwriter is COW
                    *record.get_mut::<Id>() = unsafe { std::mem::transmute(last_id.as_slice()) };
                    if let Some(ref mut blockwriter) = blockwriter_opt {
                        let _ = blockwriter.write_all(record.as_bytes::<Id>());
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(b"1");
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(b"1");
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(record.as_bytes::<R1>());
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(record.as_bytes::<R2>());
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(record.as_bytes::<Q1>());
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(record.as_bytes::<Q2>());
                        let _ = blockwriter.write_all(b"\t");
                        let _ = blockwriter.write_all(record.as_bytes::<Umi>());
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
                log_info!("Wrote debarcoded cell chunk"; "path" => ?temp_pathbuf, "records written" => records_writen);
                thread_vec_temp_written.push(temp_input_path);
            }
            return thread_vec_temp_written;
        });
        thread_handles.push(thread_handle);
    }

    return thread_handles;
}

fn spawn_mergesort_workers(
    debarcode_merge: Vec<InputPath>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
) -> (Receiver<Receiver<parse::bbgz::Block>>, Vec<JoinHandle<()>>) {
    let (fp_tx, fp_rx) = crossbeam::channel::unbounded();
    let (ms_tx, ms_rx) = crossbeam::channel::unbounded();
    let countof_threads_sort: u64 = (*budget.threads::<TMergeSort>()).get() / 2;

    let sizeof_stream_each_arena = stream_arena;
    // NOTE no other job running at this time
    let sizeof_stream_each_buffer =
        ByteSize((budget.mem::<Total>().as_u64()) / (countof_threads_sort * 2));

    let mut thread_handles = Vec::new();

    let producer_ms_tx = ms_tx.clone();
    let producer_handle = budget.spawn::<TMergeSort, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting mergesort producer"; "thread" => thread_name);

        // Handle odd file case by copying the last file directly
        if debarcode_merge.len() % 2 == 1 {
            let last_file = debarcode_merge.last().unwrap();
            log_info!("Producer handling odd file: {}", last_file);
            // HACK: Use bounded channel to prevent memory accumulation when writer is slow
            let (mc_tx, mc_rx) = crossbeam::channel::unbounded();
            let _ = producer_ms_tx.send(mc_rx);

            let d1 = codec::plain::PlaintextDecoder::builder()
                .with_path(&**last_file.path())
                .build()
                .expect("Failed to mmap file");

            let p1 = parse::bbgz::parser();

            let mut s1 = Stream::builder()
                .with_decoder(d1)
                .with_parser(p1)
                .sizeof_decode_arena(sizeof_stream_each_arena)
                .sizeof_decode_buffer(sizeof_stream_each_buffer)
                .build();

            let mut q1 = s1
                .query::<parse::bbgz::Block>()
                .assert_with_context::<Id, Id, _>(
                    |id_current: &&'static [u8], id_context: &&'static [u8]| {
                        id_current >= id_context
                    },
                    "id_current < id_context",
                );

            while let Ok(Some(cell)) = q1.next() {
                let _ = mc_tx.send(cell);
            }

            // NOTE must drop mc_tx BEFORE Streams drop, so writer can finish
            drop(mc_tx);

            if let Err(e) = std::fs::remove_file(&**last_file.path()) {
                log_critical!("Failed to delete odd file."; "path" => ?last_file, "error" => %e);
            }
        }

        let debarcode_merge_paired = debarcode_merge.into_iter().tuples();
        for (a, b) in debarcode_merge_paired {
            let _ = fp_tx.send((a, b));
        }
    });
    thread_handles.push(producer_handle);

    for thread_idx in 0..countof_threads_sort {
        let fp_rx = fp_rx.clone();
        let ms_tx = ms_tx.clone();

        let thread_handle = budget.spawn::<TMergeSort, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            log_info!("Starting mergesort worker"; "thread" => thread_name);

            // Reuse arena pool across all merges in this thread
            let a_thread_shared_stream_arena = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, sizeof_stream_each_arena));
            let b_thread_shared_stream_arena = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, sizeof_stream_each_arena));

            while let Ok((ia, ib)) = fp_rx.recv() {
                log_info!("Merging pair: {} + {}", &ia, &ib);
                // HACK: Use bounded channel to prevent memory accumulation when writer is slow
                let (mc_tx, mc_rx) = crossbeam::channel::unbounded();
                let _ = ms_tx.send(mc_rx);

                let fa = match ia.clone().open() {
                    Ok(file_handle) => file_handle,
                    Err(e) => panic!("{e}"),
                };

                let ba = BufReader::with_capacity(
                    ByteSize::mib(2).as_u64() as usize,
                    fa
                );
                let da = codec::plain::PlaintextDecoder::builder()
                    .with_reader(ba)
                    .build();

                let pa = parse::bbgz::parser();

                let mut sa = Stream::builder()
                    .with_decoder(da)
                    .with_parser(pa)
                    .with_opt_decode_arena_pool(Arc::clone(&a_thread_shared_stream_arena))
                    .build();

                let mut qa = sa
                    .query::<parse::bbgz::Block>()
                    .assert_with_context::<Id, Id, _>(
                        |id_current: &&'static [u8], id_context: &&'static [u8]| {
                            id_current >= id_context
                        },
                        "id_current < id_context",
                    );

                let fb = match ib.clone().open() {
                    Ok(file_handle) => file_handle,
                    Err(e) => panic!("{e}"),
                };

                let bb = BufReader::with_capacity(
                    ByteSize::mib(2).as_u64() as usize,
                    fb
                );
                let db = codec::plain::PlaintextDecoder::builder()
                    .with_reader(bb)
                    .build();

                let pb = parse::bbgz::parser();

                let mut sb = Stream::builder()
                    .with_decoder(db)
                    .with_parser(pb)
                    .with_opt_decode_arena_pool(Arc::clone(&b_thread_shared_stream_arena))
                    .build();

                let mut qb = sb
                    .query::<parse::bbgz::Block>()
                    .assert_with_context::<Id, Id, _>(
                        |id_current: &&'static [u8], id_context: &&'static [u8]| {
                            id_current >= id_context
                        },
                        "id_current < id_context",
                    );

                let mut cell_a = qa.next().ok().flatten();
                let mut cell_b = qb.next().ok().flatten();

                while let (Some(ref ca), Some(ref cb)) = (&cell_a, &cell_b) {
                    if ca.get_ref::<Id>() <= cb.get_ref::<Id>() {
                        let _ = mc_tx.send(cell_a.take().unwrap());
                        cell_a = qa.next().ok().flatten();
                    } else {
                        let _ = mc_tx.send(cell_b.take().unwrap());
                        cell_b = qb.next().ok().flatten();
                    }
                }

                while let Some(ca) = cell_a {
                    let _ = mc_tx.send(ca);
                    cell_a = qa.next().ok().flatten();
                }
                while let Some(cb) = cell_b {
                    let _ = mc_tx.send(cb);
                    cell_b = qb.next().ok().flatten();
                }

                // NOTE must drop mc_tx BEFORE Streams drop, so writer can finish
                drop(mc_tx);

                if let Err(e) = std::fs::remove_file(&**ia.path()) {
                    log_warning!("Failed to delete merged file."; "path" => ?&ia.path(), "error" => %e);
                }
                if let Err(e) = std::fs::remove_file(&**ib.path()) {
                    log_warning!("Failed to delete merged file."; "path" => ?&ib.path(), "error" => %e);
                }
            }
        });
        thread_handles.push(thread_handle);
    }

    return (ms_rx, thread_handles);
}

fn spawn_mergesort_writers(
    ms_rx: Receiver<Receiver<parse::bbgz::Block>>,
    timestamp_temp_files: String,
    mergeround_temp_files: usize,
    path_temp_dir: PathBuf,
    budget: &GetrawBudget,
) -> Vec<JoinHandle<Vec<InputPath>>> {
    let mut thread_handles = Vec::new();
    let countof_write_threads: u64 = (*budget.threads::<TWrite>()).get();
    let atomic_countof_merges = Arc::new(AtomicUsize::new(0));

    let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
    for thread_idx in 0..countof_write_threads {
        let ms_rx = ms_rx.clone();
        let thread_countof_merges = Arc::clone(&atomic_countof_merges);
        let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
        let thread_path_temp_dir = path_temp_dir.clone();
        let mut thread_vec_temp_written = Vec::new();

        thread_handles.push(budget.spawn::<TWrite, _, _>(thread_idx as u64, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread"); 
            log_info!("Starting worker"; "thread" => thread_name);

            while let Ok(mc_rx) = ms_rx.recv() {
                let countof_merges = thread_countof_merges.fetch_add(1, Ordering::Relaxed);
                let temp_fname = format!(
                    "{thread_timestamp_temp_files}_merge_{mergeround_temp_files}_{countof_merges}"
                );
                let temp_pathbuf = thread_path_temp_dir
                    .join(temp_fname)
                    .with_extension("tirp.bbgz");
                let temp_output_path = match OutputPath::try_from(&temp_pathbuf) {
                    Ok(path) => path,
                    Err(e) => {
                        log_critical!("Failed to create output path"; "path" => ?temp_pathbuf, "error" => %e);
                    }
                };
                let temp_output_file = match temp_output_path.create() {
                    Ok(file) => file,
                    Err(e) => {
                        log_critical!("Failed to create output file"; "path" => ?temp_pathbuf, "error" => %e);
                    }
                };

                let mut bufwriter = BufWriter::with_capacity(ByteSize::mib(8).as_u64() as usize, temp_output_file);
                let mut merge_id: SmallVec<[u8; 16]> = SmallVec::new();
                let mut merge_blocks: SmallVec<[parse::bbgz::Block; 8]> = SmallVec::new();
                let mut merge_csize = 0;
                let mut merge_hsize = 0;
                
                while let Ok(block) = mc_rx.recv() {
                    let id_bytes = block.as_bytes::<Id>();
                    let header_bytes = block.as_bytes::<Header>();
                    let compressed_bytes = block.as_bytes::<Compressed>();

                    let csize = compressed_bytes.len();
                    let hsize = header_bytes.len() + csize;

                    if merge_hsize + hsize + BBGZTrailer::SSIZE > MAX_SIZEOF_BLOCKusize {
                        // SAFETY at this point we will always have at least 1 merge block
                        let (new_header_bytes, new_trailer_bytes) = unsafe { 
                            let merge_first = merge_blocks.get_unchecked(0);
                            (
                                merge_first.as_bytes::<Header>(),
                                merge_first.as_bytes::<Trailer>()
                            ) 
                        };

                        let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                        let mut new_trailer = BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                        for merge_block in merge_blocks.iter().skip(1) {
                            let merge_header_bytes = merge_block.as_bytes::<Header>();
                            let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                            let merge_header = BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                            let merge_trailer = BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                            new_header.merge(merge_header).unwrap();
                            new_trailer.merge(merge_trailer).unwrap();
                        }

                        new_header.write_with_csize(&mut bufwriter, merge_csize).unwrap();
                        let last_idx = merge_blocks.len() - 1;
                        for i in 0..last_idx {
                            let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                            let merge_raw_bytes_len = merge_raw_bytes.len();
                            bufwriter.write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)]).unwrap();
                        }
                        // Write last block with BFINAL=1 intact
                        let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }.as_bytes::<Compressed>();
                        let last_raw_bytes_len = last_raw_bytes.len();
                        bufwriter.write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)]).unwrap();
                        bufwriter.write_all(&[0x03, 00]).unwrap();
                        new_trailer.write_with(&mut bufwriter).unwrap();
                        let bsize = new_header.BC.BSIZE as usize;
                        assert_eq!(bsize, new_header.size() + merge_csize + BBGZTrailer::SSIZE - 1);
                        merge_blocks.clear();
                        merge_csize = 0;
                        merge_hsize = 0;
                    }

                    if *id_bytes == *merge_id {
                        match merge_blocks.len() {
                            0 => {
                                merge_blocks.push(block);
                                merge_csize = csize;
                                merge_hsize = hsize;
                            } 
                            1.. => {
                                merge_blocks.push(block);
                                merge_csize += csize - 2;
                                merge_hsize += hsize;
                            }
                        }
                    } else {
                        match merge_blocks.len() {
                            0 => {
                                merge_id = id_bytes.to_smallvec();
                                merge_blocks.push(block);
                                merge_csize = csize;
                                merge_hsize = hsize;
                            } 
                            1.. => {
                                // SAFETY merge_blocks.len() > 0
                                let (new_header_bytes, new_trailer_bytes) = unsafe { 
                                    let merge_first = merge_blocks.get_unchecked(0);
                                    (
                                        merge_first.as_bytes::<Header>(),
                                        merge_first.as_bytes::<Trailer>()
                                    ) 
                                };

                                let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                                let mut new_trailer = BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                                for merge_block in merge_blocks.iter().skip(1) {
                                    let merge_header_bytes = merge_block.as_bytes::<Header>();
                                    let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                                    let merge_header = BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                                    let merge_trailer = BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                                    new_header.merge(merge_header).unwrap();
                                    new_trailer.merge(merge_trailer).unwrap();
                                }

                                new_header.write_with_csize(&mut bufwriter, merge_csize).unwrap();
                                let last_idx = merge_blocks.len() - 1;
                                for i in 0..last_idx {
                                    let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                                    let merge_raw_bytes_len = merge_raw_bytes.len();
                                    bufwriter.write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)]).unwrap();
                                }
                                // Write last block with BFINAL=1 intact
                                let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }.as_bytes::<Compressed>();
                                let last_raw_bytes_len = last_raw_bytes.len();
                                bufwriter.write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)]).unwrap();
                                bufwriter.write_all(&[0x03, 00]).unwrap();
                                new_trailer.write_with(&mut bufwriter).unwrap();
                                let bsize = new_header.BC.BSIZE as usize;
                                assert_eq!(bsize, new_header.size() + merge_csize + BBGZTrailer::SSIZE - 1);
                                merge_id = id_bytes.to_smallvec();
                                merge_blocks.clear();
                                merge_blocks.push(block);
                                merge_csize = csize;
                                merge_hsize = hsize;
                            }
                        }
                    } 
                }

                if merge_blocks.len() > 0 {
                    // SAFETY at this point we will always have at least 1 merge block
                    let (new_header_bytes, new_trailer_bytes) = unsafe { 
                        let merge_first = merge_blocks.get_unchecked(0);
                        (
                            merge_first.as_bytes::<Header>(),
                            merge_first.as_bytes::<Trailer>()
                        ) 
                    };

                    let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                    let mut new_trailer = BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                    for merge_block in merge_blocks.iter().skip(1) {
                        let merge_header_bytes = merge_block.as_bytes::<Header>();
                        let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                        let merge_header = BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                        let merge_trailer = BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                        new_header.merge(merge_header).unwrap();
                        new_trailer.merge(merge_trailer).unwrap();
                    }

                    new_header.write_with_csize(&mut bufwriter, merge_csize).unwrap();
                    let last_idx = merge_blocks.len() - 1;
                    for i in 0..last_idx {
                        let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                        let merge_raw_bytes_len = merge_raw_bytes.len();
                        bufwriter.write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)]).unwrap();
                    }
                    // Write last block with BFINAL=1 intact
                    let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }.as_bytes::<Compressed>();
                    let last_raw_bytes_len = last_raw_bytes.len();
                    bufwriter.write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)]).unwrap();
                    bufwriter.write_all(&[0x03, 00]).unwrap();
                    new_trailer.write_with(&mut bufwriter).unwrap();
                    let bsize = new_header.BC.BSIZE as usize;
                    assert_eq!(bsize, new_header.size() + merge_csize + BBGZTrailer::SSIZE - 1);
                }

                bufwriter.write_all(&bbgz::MARKER_EOF).unwrap();
                bufwriter.flush().unwrap();

                let temp_input_path = match InputPath::try_from(&temp_pathbuf) {
                    Ok(path) => path,
                    Err(e) => panic!("{}", e)
                };
                log_info!("Wrote sorted cell chunk"; "path" => ?temp_pathbuf);
                thread_vec_temp_written.push(temp_input_path);
            }
            return thread_vec_temp_written;
        }));
    }

    return thread_handles;
}

fn spawn_histogram_workers(
    output_hist_pairs: Vec<(OutputPath, OutputPath)>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
) -> Vec<JoinHandle<()>> {
    let countof_threads_total: u64 = (*budget.threads::<Total>()).get();
    let (pair_tx, pair_rx) = crossbeam::channel::unbounded();
    let mut thread_handles = Vec::new();

    // Producer thread that sends (output_path, hist_path) pairs
    let producer_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        log_info!("Starting histogram producer"; "thread" => thread_name);

        for pair in output_hist_pairs {
            let _ = pair_tx.send(pair);
        }
    });
    thread_handles.push(producer_handle);

    // Determine how many worker threads to use (leave 1 for producer)
    let countof_worker_threads = (countof_threads_total - 1).max(1);
    let countof_threads_per_worker = BoundedU64::new_saturating(countof_threads_total / countof_worker_threads);
    let sizeof_stream_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / countof_worker_threads);

    for thread_idx in 0..countof_worker_threads {
        let pair_rx = pair_rx.clone();
        let thread_stream_arena = stream_arena;
        let thread_stream_buffer = sizeof_stream_buffer;
        let thread_countof_threads = countof_threads_per_worker;

        let worker_handle = budget.spawn::<Total, _, _>(thread_idx + 1, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            log_info!("Starting histogram worker"; "thread" => thread_name);

            while let Ok((output_path, hist_path)) = pair_rx.recv() {
                log_info!("Processing histogram for {}", output_path);

                let mut hist_hashmap: gxhash::HashMap<Vec<u8>, u64> = gxhash::HashMap::new();

                let decoder = codec::BBGZDecoder::builder()
                    .with_path(output_path.path().path())
                    .countof_threads(thread_countof_threads)
                    .build();
                let parser = parse::Tirp::builder().build();

                let mut stream = Stream::builder()
                    .with_decoder(decoder)
                    .with_parser(parser)
                    .sizeof_decode_arena(thread_stream_arena)
                    .sizeof_decode_buffer(thread_stream_buffer)
                    .build();

                let mut query = stream
                    .query::<tirp::Cell>()
                    .group_relaxed_with_context::<Id, Id, _>(
                        |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
                            std::cmp::Ordering::Less => panic!("Unordered record list\nCurrent ID: {:?}\nContext ID: {:?}\nCurrent (lossy): {}\nContext (lossy): {}",
                                id, id_ctx, String::from_utf8_lossy(id), String::from_utf8_lossy(id_ctx)),
                            std::cmp::Ordering::Equal => QueryResult::Keep,
                            std::cmp::Ordering::Greater => QueryResult::Emit,
                        },
                    );

                while let Ok(Some(cell)) = query.next() {
                    let n = cell.get_ref::<R1>().len() as u64;
                    let _ = *hist_hashmap
                        .entry(cell.get_ref::<Id>().to_vec())
                        .and_modify(|c| *c += n)
                        .or_insert(n);
                }

                let hist_file = match hist_path.clone().create() {
                    Ok(file) => file,
                    Err(e) => {
                        log_critical!("Failed to create output file"; "path" => ?hist_path, "error" => %e);
                    }
                };

                let mut bufwriter = BufWriter::new(hist_file);
                for (id, count) in hist_hashmap.iter() {
                    bufwriter.write_all(&id);
                    bufwriter.write_all(b"\t");
                    bufwriter.write_all(count.to_string().as_bytes());
                    bufwriter.write_all(b"\n");
                }

                bufwriter.flush();
                log_info!("Wrote histogram at {}", hist_path);
            }
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

#[derive(Clone)]
pub struct DebarcodeAtrandiWGSChemistry {
    barcode: CombinatorialBarcode8bp,
}
impl DebarcodeAtrandiWGSChemistry {
    pub fn new() -> Self {
        let mut result = DebarcodeAtrandiWGSChemistry {
            barcode: CombinatorialBarcode8bp::new(),
        };

        let reader = Cursor::new(include_bytes!("../barcode/atrandi_barcodes.tsv"));
        for (index, line) in reader.lines().enumerate() {
            if index == 0 {
                continue;
            }

            let line = line.unwrap();
            let parts: Vec<&str> = line.split('\t').collect();
            result.barcode.add_bc(parts[1], parts[0], parts[2]);
        }

        result.barcode.pools[3].pos_anchor = (8 + 4) * 0;
        result.barcode.pools[3].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[2].pos_anchor = (8 + 4) * 1;
        result.barcode.pools[2].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[1].pos_anchor = (8 + 4) * 2;
        result.barcode.pools[1].pos_rel_anchor = vec![0, 1];

        result.barcode.pools[0].pos_anchor = (8 + 4) * 3;
        result.barcode.pools[0].pos_rel_anchor = vec![0, 1];

        result
    }
}
impl crate::barcode::Chemistry for DebarcodeAtrandiWGSChemistry {
    fn prepare_using_rp_vecs<C: bascet_core::Composite>(
        &mut self,
        _vec_r1: Vec<C>,
        _vec_r2: Vec<C>,
    ) -> anyhow::Result<()>
    where
        C: bascet_core::Get<bascet_core::R0>,
        <C as bascet_core::Get<bascet_core::R0>>::Value: AsRef<[u8]>,
    {
        Ok(())
    }
    fn detect_barcode_and_trim<'a>(
        &mut self,
        r1_seq: &'a [u8],
        r1_qual: &'a [u8],
        r2_seq: &'a [u8],
        r2_qual: &'a [u8],
    ) -> (u32, crate::common::ReadPair<'a>) {
        //Detect barcode, which here is in R2
        let total_distance_cutoff = 4;
        let part_distance_cutoff = 1;

        let (bc, score) =
            self.barcode
                .detect_barcode(r2_seq, true, total_distance_cutoff, part_distance_cutoff);

        match score {
            0.. => {
                //R2 need to have the first part with barcodes removed. Figure out total size!
                let r2_from = self.barcode.trim_bcread_len;
                let r2_to = r2_seq.len();

                //Get UMI position
                let umi_from = self.barcode.umi_from;
                let umi_to = self.barcode.umi_to;
                (
                    bc,
                    common::ReadPair {
                        r1: &r1_seq,
                        r2: &r2_seq[r2_from..r2_to],
                        q1: &r1_qual,
                        q2: &r2_qual[r2_from..r2_to],
                        umi: &r2_seq[umi_from..umi_to],
                    },
                )
            }
            ..0 => {
                //Just return the sequence as-is
                (
                    u32::MAX,
                    common::ReadPair {
                        r1: &r1_seq,
                        r2: &r2_seq,
                        q1: &r1_qual,
                        q2: &r2_qual,
                        umi: &[],
                    },
                )
            }
        }
    }

    fn bcindexu32_to_bcu8(&self, index32: &u32) -> Vec<u8> {
        let mut result = Vec::new();
        let bytes = index32.as_bytes();
        result.extend_from_slice(
            self.barcode.pools[0].barcode_name_list[bytes[3] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[1].barcode_name_list[bytes[2] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[2].barcode_name_list[bytes[1] as usize].as_bytes(),
        );
        result.push(b'_');
        result.extend_from_slice(
            self.barcode.pools[3].barcode_name_list[bytes[0] as usize].as_bytes(),
        );

        return result;
    }
}
