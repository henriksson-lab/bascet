use std::cell::UnsafeCell;
use std::io::{BufRead, BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use bgzip::write::BGZFMultiThreadWriter;
use bgzip::Compression;
use blart::AsBytes;
use clap::{Args, Subcommand};
use crossbeam::channel::{Receiver, RecvTimeoutError};
use gxhash::HashMapExt;
use itertools::{izip, Itertools};
use smallvec::SmallVec;

use crate::barcode::{Chemistry, CombinatorialBarcode8bp, ParseBioChemistry3};
use crate::io::traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream, BascetWrite};
use crate::{
    common, log_critical, log_info, log_warning, support_which_stream, support_which_writer,
    threading,
};

support_which_stream! {
    DebarcodeReadsInput => DebarcodeReadsStream<T: BascetCell>
    for formats [fastq_gz]
}

support_which_stream! {
    DebarcodeMergeInput => DebarcodeMergeStream<T: BascetCell>
    for formats [tirp_bgzf]
}
support_which_writer! {
    DebarcodeMergeOutput => DebarcodeMergeWriter<W: Write>
    for formats [tirp_bgzf]
}

support_which_writer! {
    DebarcodeHistOutput => DebarcodeHistWriter<W: Write>
    for formats [hist]
}

#[derive(Args)]
pub struct GetRawCMD {
    #[arg(short = '1', long = "r1", num_args = 1.., required = true, value_delimiter = ',', help = "List of input R1 FASTQ files (comma-separated)")]
    pub paths_r1: Vec<PathBuf>,
    #[arg(short = '2', long = "r2", num_args = 1.., required = true, value_delimiter = ',', help = "List of input R2 FASTQ files (comma-separated)")]
    pub paths_r2: Vec<PathBuf>,
    #[arg(short = 'o', long = "out", num_args = 1.., required = true, value_delimiter = ',', help = "List of output file paths (comma-separated)")]
    pub paths_out: Vec<PathBuf>,
    #[arg(
        long = "hist",
        help = "Histogram file paths. Defaults to <path_out>.hist"
    )]
    pub paths_hist: Option<Vec<PathBuf>>,
    #[arg(
        long = "temp",
        help = "Temporary storage directory. Defaults to <path_out>"
    )]
    pub path_temp: Option<PathBuf>,

    #[arg(
        short = '@',
        help = "Total threads to use. Defaults to auto-detection via std::thread::available_parralelism()"
    )]
    threads_total: Option<usize>,
    #[arg(
        long = "threads-read",
        help = "Number of reader threads (default: total / 4)"
    )]
    threads_read: Option<usize>,
    #[arg(
        long = "threads-debarcode",
        help = "Number of debarcoding threads (default: total / 8)"
    )]
    threads_debarcode: Option<usize>,
    #[arg(
        long = "threads-sort",
        help = "Number of sorting threads (default: total / 2)"
    )]
    threads_sort: Option<usize>,
    #[arg(
        long = "threads-write",
        help = "Number of writer threads (default: total / 4)"
    )]
    threads_write: Option<usize>,

    #[arg(
        long = "buffer-size",
        default_value_t = 1024 * 12,
        help = "Total stream buffer size in MB"
    )]
    pub buffer_size_mb: usize,
    #[arg(
        long = "page-size",
        default_value_t = 32,
        help = "Stream page size in MB"
    )]
    pub page_size_mb: usize,
    #[arg(
        long = "sort-buffer-size",
        default_value_t = 1024 * 12,
        help = "Total sort buffer size in MB"
    )]
    pub sort_buffer_size_mb: usize,

    #[arg(long = "skip-debarcode", num_args = 1.., value_delimiter = ',', help = "Skip debarcoding phase and merge existing chunk files (comma-separated list of chunk files)")]
    pub skip_debarcode: Vec<PathBuf>,

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

struct ThreadConfig {
    total: usize,
    read: usize,
    debarcode: usize,
    sort: usize,
    write: usize,
}

impl GetRawCMD {
    fn resolve_threads(&self) -> ThreadConfig {
        let total_threads_desired = self.threads_total.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or_else(|e| {
                    log_critical!(
                        "Failed to detect available CPUs. Please specify thread count manually with -@=<cpus>."; "error" => %e
                    );
                })
        });

        let mut thread_config = ThreadConfig {
            total: 0,
            sort: self.threads_sort.unwrap_or(total_threads_desired / 2),
            read: self.threads_read.unwrap_or(total_threads_desired / 4),
            write: self.threads_write.unwrap_or(total_threads_desired / 8),
            debarcode: self.threads_debarcode.unwrap_or(total_threads_desired / 8),
        };

        let total_threads_actual = 0
            + thread_config.read
            + thread_config.write
            + thread_config.debarcode
            + thread_config.sort;

        thread_config.total = total_threads_actual;

        log_info!(
            "Using {total_threads_actual} threads";
            "read" => thread_config.read,
            "debarcode" => thread_config.debarcode,
            "sort" => thread_config.sort,
            "write" => thread_config.write
        );

        if total_threads_actual != total_threads_desired {
            log_warning!(
                "Thread count mismatch: requested {total_threads_desired} but using {total_threads_actual}"
            );
        }

        return thread_config;
    }

    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let thread_config = self.resolve_threads();

        let stream_buffer_size_bytes = self.buffer_size_mb * 1024 * 1024;
        let stream_page_size_bytes = self.page_size_mb * 1024 * 1024;
        let stream_n_pages = stream_buffer_size_bytes / stream_page_size_bytes;

        let sort_buffer_size_bytes = self.sort_buffer_size_mb * 1024 * 1024;

        let mut vec_input_debarcode_merge: Vec<DebarcodeMergeInput> = self.skip_debarcode.iter()
            .filter_map(|p| {
                match DebarcodeMergeInput::try_from_path(p) {
                    Ok(file) => Some(file),
                    Err(e) => {
                        log_warning!("Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
                        None
                    }
                }})
            .collect();

        let vec_output: Vec<DebarcodeMergeOutput> = self.paths_out.iter().filter_map(|path_out| {
            match DebarcodeMergeOutput::try_from_path(path_out) {
                Ok(out) => Some(out),
                Err(e) => {
                    log_warning!("Failed to verify output file, skipping"; "path" => ?path_out, "error" => %e);
                    None
                }
            }
        }).collect();

        if vec_output.is_empty() {
            log_critical!(
                "No valid output file paths specified. All output paths failed verification."
            );
        }

        if self.paths_hist.is_some() && self.paths_hist.as_ref().unwrap().len() != vec_output.len()
        {
            let n_hist = self.paths_hist.as_ref().unwrap().len();
            let n_out = vec_output.len();
            log_critical!(
                "Number of histogram paths ({n_hist}) does not match number of output paths ({n_out})"
            );
        }

        let timestamp_temp_files = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            / 60;
        let timestamp_temp_files = timestamp_temp_files.to_string();

        let path_temp_dir = self
            .path_temp
            .clone()
            .unwrap_or(vec_output.first().unwrap().path().to_path_buf())
            .parent()
            .unwrap_or_else(|| {
                log_critical!("No valid temp path specified.");
            })
            .to_path_buf();

        if vec_input_debarcode_merge.is_empty() {
            let vec_input: Vec<(DebarcodeReadsInput, DebarcodeReadsInput)> = izip!(self.paths_r1.clone(), self.paths_r2.clone())
                .filter_map(|(path_r1, path_r2)| {
                    match (
                        DebarcodeReadsInput::try_from_path(&path_r1),
                        DebarcodeReadsInput::try_from_path(&path_r2)
                    ) {
                        (Ok(r1), Ok(r2)) => Some((r1, r2)),
                        (Err(e), _) | (_, Err(e)) => {
                            log_warning!("Failed to open file pair, skipping"; "r1" => ?path_r1, "r2" => ?path_r2, "error" => %e);
                            None
                        }
                    }
                })
                .collect();

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
                // prepare chemistry using r2
                let mut s1 =
                    DebarcodeReadsStream::<DebarcodeReadCell>::try_from_input(input_r1).unwrap();
                s1.set_reader_threads(thread_config.read);
                s1.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);

                let mut b1 = Vec::with_capacity(10000);
                for token in s1 {
                    let token = token.unwrap();
                    b1.push(token.into_owned());

                    if b1.len() >= 10000 {
                        break;
                    }
                }
                log_info!("Finished reading first 10000 reads of R1...");
                let mut s2 =
                    DebarcodeReadsStream::<DebarcodeReadCell>::try_from_input(input_r2).unwrap();
                s2.set_reader_threads(thread_config.read);
                s2.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);

                let mut b2 = Vec::with_capacity(10000);
                for token in s2 {
                    let token = token.unwrap();
                    b2.push(token.into_owned());

                    if b2.len() >= 10000 {
                        break;
                    }
                }
                log_info!("Finished reading first 10000 reads of R2...");
                let _ = chemistry.prepare_using_rp_vecs(b1, b2);
            }
            // std::process::exit(0);
            let ((r1_rx, r2_rx), (r1_handle, r2_handle)) = spawn_paired_readers(
                vec_input,
                thread_config.read,
                stream_page_size_bytes,
                stream_n_pages,
            );

            let (rp_rx, rt_handle) = spawn_debarcode_router(r1_rx, r2_rx);
            let (db_rx, db_handles, chemistry) =
                spawn_debarcode_workers(rp_rx, chemistry, thread_config.debarcode);

            let (ct_rx, ct_handle) =
                spawn_collector(db_rx, thread_config.sort, sort_buffer_size_bytes);
            let (st_rx, st_handles) = spawn_sort_workers(ct_rx, chemistry, thread_config.sort);

            let wt_handles = spawn_chunk_writers(
                st_rx,
                timestamp_temp_files.clone(),
                path_temp_dir.clone(),
                thread_config.write,
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
            for (i, handle) in db_handles.into_iter().enumerate() {
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
            for (i, handle) in st_handles.into_iter().enumerate() {
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
                let paths: Vec<DebarcodeMergeInput> = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i))
                    .iter()
                    .filter_map(|p| {
                        match DebarcodeMergeInput::try_from_path(p) {
                            Ok(file) => Some(file),
                            Err(e) => {
                                log_warning!("Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
                                None
                            }
                        }})
                    .collect();

                vec_input_debarcode_merge.extend(paths);
            }
            log_info!(
                "All chunk writer threads finished. Total chunks: {}",
                vec_input_debarcode_merge.len()
            );
        }

        let mut mergeround_counter = 1;
        let mut mergeround_merge_next = vec_input_debarcode_merge;

        while mergeround_merge_next.len() > vec_output.len() {
            log_info!(
                "Mergesort round {mergeround_counter}: Starting with {} files, target: {} files",
                mergeround_merge_next.len(),
                vec_output.len()
            );

            let current_count = mergeround_merge_next.len();
            let target_count = vec_output.len();
            let files_to_merge = current_count - target_count;

            let (files_to_merge, files_to_keep): (
                Vec<(usize, DebarcodeMergeInput)>,
                Vec<(usize, DebarcodeMergeInput)>,
            ) = mergeround_merge_next
                .into_iter()
                .enumerate()
                .partition(|(i, _)| *i < files_to_merge * 2);

            let files_to_merge: Vec<DebarcodeMergeInput> =
                files_to_merge.into_iter().map(|(_, file)| file).collect();
            let files_to_keep: Vec<DebarcodeMergeInput> =
                files_to_keep.into_iter().map(|(_, file)| file).collect();

            let (ms_rx, ms_handles) = spawn_mergesort_workers(
                files_to_merge,
                stream_buffer_size_bytes + sort_buffer_size_bytes,
                stream_page_size_bytes,
                thread_config.read,
                thread_config.sort,
            );

            let wt_handles = spawn_mergesort_writers(
                ms_rx,
                timestamp_temp_files.clone(),
                mergeround_counter,
                path_temp_dir.clone(),
                thread_config.write,
            );

            log_info!(
                "Mergesort round {mergeround_counter}: Waiting for {} mergesort threads to finish...",
                ms_handles.len()
            );
            for handle in ms_handles {
                handle.join().unwrap();
            }
            log_info!(
                "Mergesort round {mergeround_counter}: All mergesort worker threads finished"
            );

            log_info!(
                "Mergesort round {mergeround_counter}: Waiting for {} sorted cell writer threads to finish...",
                wt_handles.len()
            );

            // Collect outputs from current round
            mergeround_merge_next = files_to_keep; // Start with passthrough files
            for (i, handle) in wt_handles.into_iter().enumerate() {
                let paths: Vec<DebarcodeMergeInput> = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i))
                    .iter()
                    .filter_map(|p| {
                        match DebarcodeMergeInput::try_from_path(p) {
                            Ok(file) => Some(file),
                            Err(e) => {
                                log_warning!("Mergesort round {mergeround_counter}: Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
                                None
                            }
                        }})
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
        for (final_file, output_file) in mergeround_merge_next.iter().zip(vec_output.iter()) {
            let final_path = final_file.path();
            let output_path = output_file.path();
            match std::fs::rename(final_file.path(), output_file.path()) {
                Ok(_) => {
                    log_info!("Moved {final_path:?} -> {output_path:?}");
                    output_paths.push(output_file.path());
                }
                Err(e) => {
                    log_warning!("Failed moving {final_path:?} -> {output_path:?}"; "error" => %e);
                    output_paths.push(final_file.path());
                }
            }
        }

        let mut hist_hashmap: gxhash::HashMap<Vec<u8>, u64> = gxhash::HashMap::new();
        for (i, output_path) in output_paths.iter().enumerate() {
            let input = match DebarcodeMergeInput::try_from_path(output_path) {
                Ok(i) => i,
                Err(e) => {
                    log_critical!("Failed to verify hist input file"; "path" => ?output_path, "error" => %e);
                }
            };

            let mut stream =
                DebarcodeMergeStream::<DebarcodeMergeCell>::try_from_input(&input).unwrap();
            stream.set_reader_threads(thread_config.total);
            stream.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);

            for token in stream {
                let token = token.unwrap();
                let n = token.reads.len() as u64;
                let _ = *hist_hashmap
                    .entry(token.cell.to_vec())
                    .and_modify(|c| *c += n)
                    .or_insert(n);
                drop(token);
            }

            let hist_path = if let Some(ref hist_paths) = self.paths_hist {
                &hist_paths[i]
            } else {
                &PathBuf::from(format!("{}.hist", output_paths[i].display()))
            };

            let hist_output = match DebarcodeHistOutput::try_from_path(&hist_path) {
                Ok(out) => out,
                Err(e) => {
                    log_critical!("Failed to verify hist output file"; "path" => ?hist_path, "error" => %e)
                }
            };

            let hist_file = match std::fs::File::create(hist_path) {
                Ok(file) => file,
                Err(e) => {
                    log_critical!("Failed to create output file"; "path" => ?hist_path, "error" => %e);
                }
            };

            let mut hist_writer = match DebarcodeHistWriter::try_from_output(&hist_output) {
                Ok(w) => w,
                Err(e) => {
                    log_critical!("Failed to create hist output writer"; "path" => ?hist_path, "error" => %e);
                }
            };

            hist_writer = hist_writer.set_writer(BufWriter::new(hist_file));
            let _ = hist_writer.write_hist(&hist_hashmap);
            if let Some(mut writer) = hist_writer.get_writer() {
                let _ = writer.flush();
            }
            hist_hashmap.clear();
        }

        Ok(())
    }
}

fn spawn_paired_readers(
    vec_input: Vec<(DebarcodeReadsInput, DebarcodeReadsInput)>,
    stream_n_threads: usize,
    stream_page_size_bytes: usize,
    stream_n_pages: usize,
) -> (
    (Receiver<DebarcodeReadCell>, Receiver<DebarcodeReadCell>),
    (JoinHandle<()>, JoinHandle<()>),
) {
    let (r1_tx, r1_rx) = crossbeam::channel::unbounded();
    let (r2_tx, r2_rx) = crossbeam::channel::unbounded();
    let arc_vec_input = Arc::new(vec_input);
    let stream_n_threads = (stream_n_threads / 2).min(1);
    let stream_n_pages = stream_n_pages / 2;

    let input_r1 = Arc::clone(&arc_vec_input);
    let handle_r1 = std::thread::spawn(move || {
        for (input_r1, _) in &*input_r1 {
            let mut stream =
                DebarcodeReadsStream::<DebarcodeReadCell>::try_from_input(input_r1).unwrap();
            stream.set_reader_threads(stream_n_threads);
            stream.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);

            for token in stream {
                let token = token.unwrap();
                let _ = r1_tx.send(token);
            }
            log_info!("R1 finished reading");
        }
    });

    // let r2_tx = r2_tx.clone();
    let input_r2 = Arc::clone(&arc_vec_input);
    let handle_r2 = std::thread::spawn(move || {
        for (_, input_r2) in &*input_r2 {
            let mut stream =
                DebarcodeReadsStream::<DebarcodeReadCell>::try_from_input(input_r2).unwrap();
            stream.set_reader_threads(stream_n_threads);
            stream.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);

            for token in stream {
                let token = token.unwrap();
                let _ = r2_tx.send(token);
            }
            log_info!("R2 finished reading");
        }
    });

    return ((r1_rx, r2_rx), (handle_r1, handle_r2));
}

fn spawn_debarcode_router(
    r1_rx: Receiver<DebarcodeReadCell>,
    r2_rx: Receiver<DebarcodeReadCell>,
) -> (
    Receiver<(DebarcodeReadCell, DebarcodeReadCell)>,
    JoinHandle<()>,
) {
    let (rp_tx, rp_rx) = crossbeam::channel::unbounded();
    let rt_handle = std::thread::spawn(move || loop {
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
    });

    return (rp_rx, rt_handle);
}

fn spawn_debarcode_workers(
    rp_rx: Receiver<(DebarcodeReadCell, DebarcodeReadCell)>,
    chemistry: GetRawChemistry,
    debarcode_n_threads: usize,
) -> (
    Receiver<(u32, (DebarcodeReadCell, DebarcodeReadCell))>,
    Vec<JoinHandle<()>>,
    GetRawChemistry,
) {
    let mut thread_handles = Vec::with_capacity(debarcode_n_threads);
    let (ct_tx, ct_rx) = crossbeam::channel::unbounded();

    let atomic_total_counter = Arc::new(AtomicUsize::new(0));
    let atomic_success_counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..debarcode_n_threads {
        let mut chemistry = chemistry.clone();
        let rp_rx = rp_rx.clone();
        let ct_tx = ct_tx.clone();

        let thread_atomic_total_counter = Arc::clone(&atomic_total_counter);
        let thread_atomic_success_counter = Arc::clone(&atomic_success_counter);

        let thread_handle = std::thread::spawn(move || {
            while let Ok((mut r1, mut r2)) = rp_rx.recv() {
                // TODO: optimisation: barcodes are fixed-size if represented in a non string way (e.g as u64)
                let (bcindex, rp) =
                    chemistry.detect_barcode_and_trim(r1.read, r1.quality, r2.read, r2.quality);

                let thread_total_counter =
                    thread_atomic_total_counter.fetch_add(1, Ordering::Relaxed) + 1;

                if bcindex != u32::MAX {
                    let thread_success_counter =
                        thread_atomic_success_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    if thread_success_counter % 1_000_000 == 0 {
                        log_info!(
                            "{:.2}M/{:.2}M reads successfully debarcoded",
                            thread_success_counter as f64 / 1_000_000.0,
                            thread_total_counter as f64 / 1_000_000.0
                        );
                    }

                    // SAFETY: should be safe since these are slices into the same data
                    r1.read = unsafe { std::mem::transmute(rp.r1) };
                    r1.quality = unsafe { std::mem::transmute(rp.q1) };
                    r1.umi = unsafe { std::mem::transmute(rp.umi) };
                    r2.read = unsafe { std::mem::transmute(rp.r2) };
                    r2.quality = unsafe { std::mem::transmute(rp.q2) };
                    r2.umi = unsafe { std::mem::transmute(rp.umi) };

                    let _ = ct_tx.send((bcindex, (r1, r2)));
                }
            }
        });

        thread_handles.push(thread_handle);
    }

    drop(ct_tx);
    return (ct_rx, thread_handles, chemistry);
}

fn spawn_collector(
    db_rx: Receiver<(u32, (DebarcodeReadCell, DebarcodeReadCell))>,
    sort_n_threads: usize,
    sort_buffer_size_bytes: usize,
) -> (
    Receiver<Vec<(u32, (DebarcodeReadCell, DebarcodeReadCell))>>,
    JoinHandle<()>,
) {
    let (ct_tx, ct_rx) = crossbeam::channel::unbounded();
    let ct_handle = std::thread::spawn(move || {
        let mut collection_buffer: Vec<(u32, (DebarcodeReadCell, DebarcodeReadCell))> = Vec::new();
        let mut collection_cloned_size_bytes: usize = 0;
        // HACK: / 8 is a magic number. this really shouldnt allocate this much extra but it does ¯\_(ツ)_/¯
        // NOTE: removed / 8 for now?
        let collection_target_cloned_size_bytes = sort_buffer_size_bytes / sort_n_threads;
        let timeout = std::time::Duration::from_millis(500);
        loop {
            match db_rx.recv_timeout(timeout) {
                Ok((id, (r1, r2))) => {
                    let cell_mem_size = 0
                        + std::mem::size_of::<u32>()
                        + std::mem::size_of::<DebarcodeReadCell>() * 2
                        + r1.read.len()
                        + r1.quality.len()
                        + r1.umi.len()
                        + r2.read.len()
                        + r2.quality.len()
                        + r2.umi.len();

                    if cell_mem_size + collection_cloned_size_bytes
                        > collection_target_cloned_size_bytes
                    {
                        let _ = ct_tx.send(collection_buffer);
                        collection_buffer = Vec::new();
                        collection_cloned_size_bytes = 0;
                    }

                    collection_buffer.push((id, (r1, r2)));
                    collection_cloned_size_bytes += cell_mem_size;
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !collection_buffer.is_empty() {
                        let _ = ct_tx.send(collection_buffer);
                        collection_buffer = Vec::new();
                        collection_cloned_size_bytes = 0;
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
    ct_rx: Receiver<Vec<(u32, (DebarcodeReadCell, DebarcodeReadCell))>>,
    chemistry: GetRawChemistry,
    sort_n_threads: usize,
) -> (Receiver<Vec<DebarcodeMergeCell>>, Vec<JoinHandle<()>>) {
    let mut thread_handles = Vec::with_capacity(sort_n_threads);
    let (st_tx, st_rx) = crossbeam::channel::bounded(sort_n_threads);

    for _ in 0..sort_n_threads {
        let ct_rx = ct_rx.clone();
        let st_tx = st_tx.clone();
        let thread_chemistry = chemistry.clone();

        let thread_handle = std::thread::spawn(move || {
            while let Ok(cell_list) = ct_rx.recv() {
                // HACK: Convert barcode before sorting for correct ordering
                // NOTE: sort in descending order to be able to pop off the end (O(1) rather than O(n))
                // NOTE: to save memory conversion to owned cells is NOT done via map but rather by popping
                let mut cell_list_with_bc: Vec<(
                    u32,
                    Vec<u8>,
                    (DebarcodeReadCell, DebarcodeReadCell),
                )> = cell_list
                    .into_iter()
                    .map(|(id, reads)| {
                        let id_as_bc = thread_chemistry.bcindexu32_to_bcu8(&id).to_vec();
                        (id, id_as_bc, reads)
                    })
                    .collect();

                glidesort::sort_by(&mut cell_list_with_bc, |(_, bc_a, _), (_, bc_b, _)| {
                    Ord::cmp(bc_b, bc_a)
                });
                let mut owned_list: Vec<DebarcodeMergeCell> =
                    Vec::with_capacity(cell_list_with_bc.len());
                let halfway = cell_list_with_bc.len() / 2;

                while let Some((id, id_as_bc, (r1, r2))) = cell_list_with_bc.pop() {
                    let metadata = format!(
                        "{:?} from {:?} ({:?})",
                        String::from_utf8_lossy(&id_as_bc),
                        id.to_string(),
                        id.to_be_bytes()
                    )
                    .into_bytes();

                    owned_list.push(
                        DebarcodeMergeCell::builder()
                            .add_cell_id_owned(id_as_bc)
                            .add_rp_owned(r1.read.to_vec(), r2.read.to_vec())
                            .add_qp_owned(r1.quality.to_vec(), r2.quality.to_vec())
                            .add_umi_owned(r1.umi.to_vec())
                            .add_metadata_owned(metadata)
                            .build(),
                    );

                    if cell_list_with_bc.len() == halfway {
                        cell_list_with_bc.shrink_to_fit();
                    }
                }

                let _ = st_tx.send(owned_list);
            }
        });
        thread_handles.push(thread_handle);
    }

    drop(st_tx);
    return (st_rx, thread_handles);
}

fn spawn_chunk_writers(
    st_rx: Receiver<Vec<DebarcodeMergeCell>>,
    timestamp_temp_files: String,
    path_temp_dir: PathBuf,
    write_n_threads: usize,
) -> Vec<JoinHandle<Vec<PathBuf>>> {
    let mut thread_handles = Vec::with_capacity(write_n_threads);
    let atomic_counter = Arc::new(AtomicUsize::new(0));

    let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
    for _ in 0..write_n_threads {
        let st_rx = st_rx.clone();

        let thread_counter = Arc::clone(&atomic_counter);
        let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
        let thread_path_temp_dir = path_temp_dir.clone();
        let mut thread_vec_temp_written = Vec::new();
        let thread_handle = std::thread::spawn(move || {
            while let Ok(sorted_cell_list) = st_rx.recv() {
                let thread_counter = thread_counter.fetch_add(1, Ordering::Relaxed) + 1;
                let temp_path = thread_path_temp_dir
                    .join(format!(
                        "{thread_timestamp_temp_files}_merge_0_{thread_counter}"
                    ))
                    .with_extension("tirp.gz");

                let temp_output = match DebarcodeMergeOutput::try_from_path(&temp_path) {
                    Ok(out) => out,
                    Err(e) => {
                        log_critical!("Failed to verify temp output file"; "path" => ?temp_path, "error" => %e);
                    }
                };
                let temp_file = match std::fs::File::create(&temp_path) {
                    Ok(file) => file,
                    Err(e) => {
                        log_critical!("Failed to create output file"; "path" => ?temp_path, "error" => %e);
                    }
                };
                let mut temp_writer = match DebarcodeMergeWriter::try_from_output(&temp_output) {
                    Ok(w) => w,
                    Err(e) => {
                        log_critical!("Failed to create hist output writer"; "path" => ?temp_path, "error" => %e);
                    }
                };
                temp_writer = temp_writer.set_writer(BGZFMultiThreadWriter::new(
                    BufWriter::with_capacity(1024 * 1024 * 32, temp_file),
                    Compression::fast(),
                ));

                for cell in sorted_cell_list {
                    let _ = temp_writer.write_cell(&cell);
                }

                if let Some(mut writer) = temp_writer.get_writer() {
                    let _ = writer.flush();
                }
                // log_info!("Wrote debarcoded cell chunk"; "path" => ?temp_path, "cells" => sorted_cell_list.len());
                thread_vec_temp_written.push(temp_path);
            }
            return thread_vec_temp_written;
        });
        thread_handles.push(thread_handle);
    }

    return thread_handles;
}

fn spawn_mergesort_workers(
    debarcode_merge: Vec<DebarcodeMergeInput>,
    stream_buffer_size_bytes: usize,
    stream_page_size_bytes: usize,
    read_n_threads: usize,
    sort_n_threads: usize,
) -> (Receiver<Receiver<DebarcodeMergeCell>>, Vec<JoinHandle<()>>) {
    let (fp_tx, fp_rx) = crossbeam::channel::unbounded();
    let (ms_tx, ms_rx) = crossbeam::channel::unbounded();
    let stream_n_threads = (read_n_threads / sort_n_threads).max(1);
    let stream_n_pages = stream_buffer_size_bytes / stream_page_size_bytes;
    let stream_n_pages = (stream_n_pages / sort_n_threads) / 2;

    let mut thread_handles = Vec::new();

    let producer_ms_tx = ms_tx.clone();
    let producer_handle = std::thread::spawn(move || {
        // Handle odd file case by copying the last file directly
        if debarcode_merge.len() % 2 == 1 {
            let last_file = debarcode_merge.last().unwrap();
            // HACK: Use bounded channel to prevent memory accumulation when writer is slow
            let (mc_tx, mc_rx) = crossbeam::channel::bounded(4096);
            let _ = producer_ms_tx.send(mc_rx);

            match DebarcodeMergeStream::try_from_input(last_file) {
                Ok(mut stream) => {
                    stream.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);
                    stream.set_reader_threads(stream_n_threads);

                    // This should be an std::mem::cpy call but would somewhat complicate other steps
                    while let Ok(Some(cell)) = stream.next_cell() {
                        let _ = mc_tx.send(cell);
                    }

                    if let Err(e) = std::fs::remove_file(last_file.path()) {
                        log_critical!("Failed to delete odd file."; "path" => ?last_file.path(), "error" => %e);
                    }
                }
                Err(e) => {
                    log_warning!(
                        "Failed to create stream for odd file. Skipping.";
                        "path" => ?last_file.path(),
                        "error" => %e
                    );
                }
            }
        }

        // Process pairs normally
        let debarcode_merge_paired = debarcode_merge.into_iter().tuples();
        for (a, b) in debarcode_merge_paired {
            let _ = fp_tx.send((a, b));
        }
    });
    thread_handles.push(producer_handle);

    for _ in 0..sort_n_threads {
        let fp_rx = fp_rx.clone();
        let ms_tx = ms_tx.clone();

        let thread_handle = std::thread::spawn(move || {
            while let Ok((fa, fb)) = fp_rx.recv() {
                log_info!("Merging pair: {:?} + {:?}", &fa.path(), &fb.path());
                // HACK: Use bounded channel to prevent memory accumulation when writer is slow
                let (mc_tx, mc_rx) = crossbeam::channel::bounded(4096);
                let _ = ms_tx.send(mc_rx);

                let mut stream_a: DebarcodeMergeStream<DebarcodeMergeCell> =
                    match DebarcodeMergeStream::try_from_input(&fa) {
                        Ok(a) => a,
                        Err(e) => {
                            log_warning!(
                                "Failed to create merge stream a. Skipping pair.";
                                "path a" => ?&fa.path(), "path b" => ?&fb.path(),
                                "error" => %e
                            );
                            continue;
                        }
                    };
                stream_a.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);
                stream_a.set_reader_threads(stream_n_threads);

                let mut stream_b: DebarcodeMergeStream<DebarcodeMergeCell> =
                    match DebarcodeMergeStream::try_from_input(&fb) {
                        Ok(b) => b,
                        Err(e) => {
                            log_warning!(
                                "Failed to create merge stream b. Skipping pair.";
                                "path a" => ?&fa.path(), "path b" => ?&fb.path(),
                                "error" => %e
                            );
                            continue;
                        }
                    };
                stream_b.set_pagebuffer_config(stream_n_pages, stream_page_size_bytes);
                stream_b.set_reader_threads(stream_n_threads);

                let mut cell_a = stream_a.next_cell().ok().flatten();
                let mut cell_b = stream_b.next_cell().ok().flatten();

                while let (Some(ref ca), Some(ref cb)) = (&cell_a, &cell_b) {
                    if ca.get_cell() <= cb.get_cell() {
                        let _ = mc_tx.send(cell_a.take().unwrap());
                        cell_a = stream_a.next_cell().ok().flatten();
                    } else {
                        let _ = mc_tx.send(cell_b.take().unwrap());
                        cell_b = stream_b.next_cell().ok().flatten();
                    }
                }

                while let Some(ca) = cell_a {
                    let _ = mc_tx.send(ca);
                    cell_a = stream_a.next_cell().ok().flatten();
                }
                while let Some(cb) = cell_b {
                    let _ = mc_tx.send(cb);
                    cell_b = stream_b.next_cell().ok().flatten();
                }

                if let Err(e) = std::fs::remove_file(fa.path()) {
                    log_critical!("Failed to delete merged file."; "path" => ?fa.path(), "error" => %e);
                }
                if let Err(e) = std::fs::remove_file(fb.path()) {
                    log_critical!("Failed to delete merged file."; "path" => ?fb.path(), "error" => %e);
                }
            }
        });
        thread_handles.push(thread_handle);
    }

    return (ms_rx, thread_handles);
}

fn spawn_mergesort_writers(
    ms_rx: Receiver<Receiver<DebarcodeMergeCell>>,
    timestamp_temp_files: String,
    mergeround_temp_files: usize,
    path_temp_dir: PathBuf,
    write_n_threads: usize,
) -> Vec<JoinHandle<Vec<PathBuf>>> {
    let mut thread_handles = Vec::new();
    let atomic_counter = Arc::new(AtomicUsize::new(0));

    let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
    for _ in 0..write_n_threads {
        let ms_rx = ms_rx.clone();
        let thread_counter = Arc::clone(&atomic_counter);
        let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
        let thread_path_temp_dir = path_temp_dir.clone();
        let mut thread_vec_temp_written = Vec::new();

        let thread_handle = std::thread::spawn(move || {
            while let Ok(mc_rx) = ms_rx.recv() {
                let mut cells_written = 0;
                let thread_counter = thread_counter.fetch_add(1, Ordering::Relaxed) + 1;
                let temp_path = thread_path_temp_dir
                    .join(format!(
                        "{thread_timestamp_temp_files}_merge_{mergeround_temp_files}_{thread_counter}"
                    ))
                    .with_extension("tirp.gz");

                let temp_output = match DebarcodeMergeOutput::try_from_path(&temp_path) {
                    Ok(out) => out,
                    Err(e) => {
                        log_critical!("Failed to verify temp output file"; "path" => ?temp_path, "error" => %e);
                    }
                };
                let temp_file = match std::fs::File::create(&temp_path) {
                    Ok(file) => file,
                    Err(e) => {
                        log_critical!("Failed to create output file"; "path" => ?temp_path, "error" => %e);
                    }
                };
                let mut temp_writer = match DebarcodeMergeWriter::try_from_output(&temp_output) {
                    Ok(w) => w,
                    Err(e) => {
                        log_critical!("Failed to create merge output writer"; "path" => ?temp_path, "error" => %e);
                    }
                };
                temp_writer = temp_writer.set_writer(BGZFMultiThreadWriter::new(
                    BufWriter::with_capacity(1024 * 1024 * 32, temp_file),
                    Compression::fast(),
                ));

                while let Ok(cell) = mc_rx.recv() {
                    cells_written += 1;
                    let _ = temp_writer.write_cell(&cell);
                }

                if let Some(mut writer) = temp_writer.get_writer() {
                    let _ = writer.flush();
                }

                log_info!("Wrote sorted cell chunk"; "path" => ?temp_path, "cells" => cells_written);
                thread_vec_temp_written.push(temp_path);
            }
            return thread_vec_temp_written;
        });
        thread_handles.push(thread_handle);
    }

    return thread_handles;
}

/*
HACK: collect first 1000 read pairs from r2
    // prepare chemistry using r2
    let input = TrimExperimentalInput::try_from_path(&path_r2).unwrap();
    let mut stream =
        TrimExperimentalStream::<TrimExperimentalCell>::try_from_input(input).unwrap();
    stream.set_reader_threads(threads_stream);
    stream.set_pagebuffer_config(num_pages, page_size_bytes);

    let mut buffer = Vec::with_capacity(1000);
    for token in stream {
        let token = token.unwrap();
        buffer.push(token.read.to_vec());

        if buffer.len() >= 1000 {
            break;
        }
    }
*/

// Cell for reading single fastq records (exactly one read)
struct DebarcodeReadCell {
    cell: &'static [u8],
    read: &'static [u8],
    reads: [(&'static [u8], &'static [u8]); 1],
    quality: &'static [u8],
    umi: &'static [u8],
    metadata: &'static [u8],

    // theoretically possible for this to be more than 1 but very unlikely
    _page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 1]>,
    _owned: Vec<Vec<u8>>,
}

impl DebarcodeReadCell {
    #[inline(always)]
    pub fn into_owned(self) -> DebarcodeReadCell {
        Self::builder()
            .add_cell_id_owned(self.cell.to_vec())
            .add_sequence_owned(self.read.to_vec())
            .add_quality_owned(self.quality.to_vec())
            .add_umi_owned(self.umi.to_vec())
            .add_metadata_owned(self.metadata.to_vec())
            .build()
    }
}

impl Drop for DebarcodeReadCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for page_ptr in &self._page_refs {
                (***page_ptr).dec_ref();
            }
        }
    }
}

impl BascetCell for DebarcodeReadCell {
    type Builder = DebarcodeReadCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }

    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        None
    }

    fn get_umis(&self) -> Option<&[&[u8]]> {
        None
    }

    fn get_metadata(&self) -> Option<&[u8]> {
        Some(self.metadata)
    }
}

struct DebarcodeReadCellBuilder {
    cell: Option<&'static [u8]>,
    read: Option<&'static [u8]>,
    quality: Option<&'static [u8]>,
    umi: Option<&'static [u8]>,
    metadata: Option<&'static [u8]>,

    page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 1]>,
    owned: Vec<Vec<u8>>,
}

impl DebarcodeReadCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            read: None,
            quality: None,
            umi: None,
            metadata: None,

            page_refs: SmallVec::new(),
            owned: Vec::new(),
        }
    }
}

impl BascetCellBuilder for DebarcodeReadCellBuilder {
    type Token = DebarcodeReadCell;

    #[inline(always)]
    fn add_page_ref(mut self, page_ptr: threading::UnsafePtr<common::PageBuffer<u8>>) -> Self {
        unsafe {
            (**page_ptr).inc_ref();
        }
        self.page_refs.push(page_ptr);
        self
    }

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        self.owned.push(id);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        // and the CountsketchCell holds the _owned field to maintain this invariant
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        self.owned.push(seq);
        let slice = self.owned.last().unwrap().as_slice();
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.read = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_quality_owned(mut self, qual: Vec<u8>) -> Self {
        self.owned.push(qual);
        let slice = self.owned.last().unwrap().as_slice();
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.quality = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_umi_owned(mut self, umi: Vec<u8>) -> Self {
        self.owned.push(umi);
        let slice = self.owned.last().unwrap().as_slice();
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.umi = Some(slice_with_lifetime);
        self
    }

    fn add_metadata_owned(mut self, metadata: Vec<u8>) -> Self {
        self.owned.push(metadata);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        // and the CountsketchCell holds the _owned field to maintain this invariant
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.metadata = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        self.read = Some(slice);
        self
    }

    #[inline(always)]
    fn add_quality_slice(mut self, slice: &'static [u8]) -> Self {
        self.quality = Some(slice);
        self
    }

    #[inline(always)]
    fn add_umi_slice(mut self, umi: &'static [u8]) -> Self {
        self.umi = Some(umi);
        self
    }

    #[inline(always)]
    fn build(self) -> DebarcodeReadCell {
        DebarcodeReadCell {
            cell: self.cell.expect("cell is required"),
            read: self.read.unwrap_or(&[]),
            reads: [(self.read.unwrap_or(&[]), &[])],
            quality: self.quality.unwrap_or(&[]),
            umi: self.umi.unwrap_or(&[]),
            metadata: self.metadata.unwrap_or(&[]),

            _page_refs: self.page_refs,
            _owned: self.owned,
        }
    }
}

struct DebarcodeMergeCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    metadata: &'static [u8],

    _page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 2]>,
    _owned: Vec<Vec<u8>>,
}

impl Drop for DebarcodeMergeCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for page_ptr in &self._page_refs {
                (***page_ptr).dec_ref();
            }
        }
    }
}

impl BascetCell for DebarcodeMergeCell {
    type Builder = DebarcodeMergeCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }

    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.qualities)
    }

    fn get_umis(&self) -> Option<&[&[u8]]> {
        Some(&self.umis)
    }

    fn get_metadata(&self) -> Option<&[u8]> {
        Some(&self.metadata)
    }
}
struct DebarcodeMergeCellBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    metadata: Option<&'static [u8]>,

    page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 2]>,
    owned: Vec<Vec<u8>>,
}

impl DebarcodeMergeCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            qualities: Vec::new(),
            umis: Vec::new(),
            metadata: None,

            page_refs: smallvec::SmallVec::new(),
            owned: Vec::new(),
        }
    }
}

impl BascetCellBuilder for DebarcodeMergeCellBuilder {
    type Token = DebarcodeMergeCell;

    #[inline(always)]
    fn add_page_ref(mut self, page_ptr: threading::UnsafePtr<common::PageBuffer<u8>>) -> Self {
        unsafe {
            (**page_ptr).inc_ref();
        }
        self.page_refs.push(page_ptr);
        self
    }

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        self.owned.push(id);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        // and the CountsketchCell holds the _owned field to maintain this invariant
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        self.owned.push(seq);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push((slice_with_lifetime, &[]));
        self
    }
    #[inline(always)]
    fn add_rp_owned(mut self, r1: Vec<u8>, r2: Vec<u8>) -> Self {
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        self.owned.push(r1);
        let r1 = self.owned.last().unwrap().as_slice();
        let r1_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(r1) };
        self.owned.push(r2);
        let r2 = self.owned.last().unwrap().as_slice();
        let r2_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(r2) };
        self.reads.push((r1_with_lifetime, r2_with_lifetime));
        self
    }

    #[inline(always)]
    fn add_quality_owned(mut self, qual: Vec<u8>) -> Self {
        self.owned.push(qual);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.qualities.push((slice_with_lifetime, &[]));
        self
    }
    #[inline(always)]
    fn add_qp_owned(mut self, q1: Vec<u8>, q2: Vec<u8>) -> Self {
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        self.owned.push(q1);
        let q1 = self.owned.last().unwrap().as_slice();
        let q1_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(q1) };
        self.owned.push(q2);
        let q2 = self.owned.last().unwrap().as_slice();
        let q2_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(q2) };
        self.qualities.push((q1_with_lifetime, q2_with_lifetime));
        self
    }

    fn add_umi_owned(mut self, umi: Vec<u8>) -> Self {
        self.owned.push(umi);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.umis.push(slice_with_lifetime);
        self
    }

    fn add_metadata_owned(mut self, metadata: Vec<u8>) -> Self {
        self.owned.push(metadata);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        // and the CountsketchCell holds the _owned field to maintain this invariant
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.metadata = Some(slice_with_lifetime);
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by refcounts. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_rp_slice(mut self, r1: &'static [u8], r2: &'static [u8]) -> Self {
        self.reads.push((r1, r2));
        self
    }
    #[inline(always)]
    fn add_qp_slice(mut self, q1: &'static [u8], q2: &'static [u8]) -> Self {
        self.qualities.push((q1, q2));
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        self.reads.push((slice, &[]));
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &'static [u8]) -> Self {
        self.qualities.push((slice, &[]));
        self
    }

    fn add_umi_slice(mut self, umi: &'static [u8]) -> Self {
        self.umis.push(umi);
        self
    }

    #[inline(always)]
    fn build(self) -> DebarcodeMergeCell {
        DebarcodeMergeCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            qualities: self.qualities,
            umis: self.umis,
            metadata: self.metadata.unwrap_or(&[]),

            _page_refs: self.page_refs,
            _owned: self.owned,
        }
    }
}

// Convenience iterators
impl<T> Iterator for DebarcodeMergeStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}

impl<T> Iterator for DebarcodeReadsStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
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
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &'static [u8],
        r1_qual: &'static [u8],
        r2_seq: &'static [u8],
        r2_qual: &'static [u8],
    ) -> (u32, crate::common::ReadPair) {
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
