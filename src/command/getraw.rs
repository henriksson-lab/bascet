use std::cell::UnsafeCell;
use std::io::{BufRead, BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use bascet_io::fastq::fastq;
use bascet_io::tirp::tirp;
use bgzip::write::BGZFMultiThreadWriter;
use bgzip::Compression;
use blart::AsBytes;
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::{Args, Subcommand};
use clio::{InputPath, OutputPath};
use crossbeam::channel::{Receiver, RecvTimeoutError};
use gxhash::HashMapExt;
use itertools::izip;

use bascet_core::*;
use bascet_derive::Budget;
use bascet_io::{decode, parse};

use crate::barcode::{Chemistry, CombinatorialBarcode8bp};
use crate::bounded_parser;
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
    pub path_temp: Option<InputPath>,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use",
        value_name = "6..",
        value_parser = bounded_parser!(BoundedU64<6, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<6, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-read",
        help = "Number of reader threads",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-debarcode",
        help = "Number of debarcoding threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_debarcode: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-sort",
        help = "Number of sorting threads",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    numof_threads_sort: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-write",
        help = "Number of writer threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_write: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(16),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size",
        value_name = "50%",
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

// #[derive(Clone)]
// #[enum_dispatch::enum_dispatch(Chemistry)]
// pub enum GetRawChemistry {
//     AtrandiWGS(DebarcodeAtrandiWGSChemistry),
//     ParseBio(ParseBioChemistry3),
// }

#[derive(Budget, Debug)]
struct GetrawBudget {
    #[threads(Total)]
    threads: BoundedU64<6, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.25) as u64).unwrap())]
    numof_threads_read: BoundedU64<2, { u64::MAX }>,

    #[threads(TDebarcode, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.125) as u64).unwrap())]
    numof_threads_debarcode: BoundedU64<1, { u64::MAX }>,

    #[threads(TSort, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.5) as u64).unwrap())]
    numof_threads_sort: BoundedU64<2, { u64::MAX }>,

    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.125) as u64).unwrap())]
    numof_threads_write: BoundedU64<1, { u64::MAX }>,

    #[mem(MStreamBuffer, |_, total_mem| bytesize::ByteSize(total_mem / 2))]
    sizeof_stream_buffer: ByteSize,

    #[mem(MSortBuffer, |_, total_mem| bytesize::ByteSize(total_mem / 2))]
    sizeof_sort_buffer: ByteSize,
}
impl GetRawCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
// impl GetRawCMD {
//     pub fn try_execute(&mut self) -> anyhow::Result<()> {
//         let budget = GetrawBudget::builder()
//             .threads(self.total_threads.unwrap_or_else(|| {
//                 std::thread::available_parallelism()
//                     .map(|p| p.get())
//                     .unwrap_or_else(|e| {
//                         log_warning!("Failed to determine available parallelism, using 6 threads"; "error" => %e);
//                         6
//                     })
//                     .try_into()
//                     .unwrap_or_else(|e| {
//                         log_warning!("Failed to convert parallelism to valid thread count, using 6 threads"; "error" => %e);
//                         6.try_into().unwrap()
//                     })
//             }))
//             .memory(self.total_mem)
//             .maybe_numof_threads_read(self.numof_threads_read)
//             .maybe_numof_threads_debarcode(self.numof_threads_debarcode)
//             .maybe_numof_threads_sort(self.numof_threads_sort)
//             .maybe_numof_threads_write(self.numof_threads_write)
//             .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
//             .maybe_sizeof_sort_buffer(self.sizeof_sort_buffer)
//             .build();
//         budget.validate();

//         log_info!(
//             "Starting GetRaw";
//             "using" => %budget,
//         );

//         let mut vec_input_debarcode_merge: Vec<DebarcodeMergeInput> = self.skip_debarcode.iter()
//             .filter_map(|p| {
//                 match DebarcodeMergeInput::try_from_path(p) {
//                     Ok(file) => Some(file),
//                     Err(e) => {
//                         log_warning!("Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
//                         None
//                     }
//                 }})
//             .collect();

//         let vec_output: Vec<DebarcodeMergeOutput> = self.paths_out.iter().filter_map(|path_out| {
//             match DebarcodeMergeOutput::try_from_path(path_out) {
//                 Ok(out) => Some(out),
//                 Err(e) => {
//                     log_warning!("Failed to verify output file, skipping"; "path" => ?path_out, "error" => %e);
//                     None
//                 }
//             }
//         }).collect();

//         if vec_output.is_empty() {
//             log_critical!(
//                 "No valid output file paths specified. All output paths failed verification."
//             );
//         }

//         if self.paths_hist.is_some() && self.paths_hist.as_ref().unwrap().len() != vec_output.len()
//         {
//             let n_hist = self.paths_hist.as_ref().unwrap().len();
//             let n_out = vec_output.len();
//             log_critical!(
//                 "Number of histogram paths ({n_hist}) does not match number of output paths ({n_out})"
//             );
//         }

//         let timestamp_temp_files = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .unwrap()
//             .as_secs()
//             / 60;
//         let timestamp_temp_files = timestamp_temp_files.to_string();

//         let path_temp_dir = if let Some(temp_path) = self.path_temp.clone() {
//             temp_path
//         } else {
//             vec_output
//                 .first()
//                 .unwrap()
//                 .path()
//                 .parent()
//                 .unwrap_or_else(|| {
//                     log_critical!("No valid output parent directory found.");
//                 })
//                 .to_path_buf()
//         };

//         if vec_input_debarcode_merge.is_empty() {
//             let vec_input: Vec<(DebarcodeReadsInput, DebarcodeReadsInput)> = izip!(self.paths_r1.clone(), self.paths_r2.clone())
//                 .filter_map(|(path_r1, path_r2)| {
//                     match (
//                         DebarcodeReadsInput::try_from_path(&path_r1),
//                         DebarcodeReadsInput::try_from_path(&path_r2)
//                     ) {
//                         (Ok(r1), Ok(r2)) => Some((r1, r2)),
//                         (Err(e), _) | (_, Err(e)) => {
//                             log_warning!("Failed to open file pair, skipping"; "r1" => ?path_r1, "r2" => ?path_r2, "error" => %e);
//                             None
//                         }
//                     }
//                 })
//                 .collect();

//             if vec_input.is_empty() {
//                 log_critical!(
//                     "No valid input files found. All input files failed to open or do not exist."
//                 );
//             }

//             let mut chemistry = match &self.chemistry {
//                 GetRawChemistryCMD::AtrandiWGS { .. } => {
//                     GetRawChemistry::AtrandiWGS(DebarcodeAtrandiWGSChemistry::new())
//                 }
//                 GetRawChemistryCMD::ParseBio { subchemistry, .. } => {
//                     GetRawChemistry::ParseBio(ParseBioChemistry3::new(&subchemistry))
//                 }
//             };

//             {
//                 log_info!("Preparing chemistry...");
//                 let (input_r1, input_r2) = &vec_input.first().unwrap();
//                 // prepare chemistry using r2
//                 let d1 = decode::Bgzf::builder()
//                     .path(input_r1.path())
//                     .num_threads(*budget.threads::<TRead>())
//                     .build()
//                     .unwrap();
//                 let p1 = parse::Fastq::builder().build().unwrap();

//                 let mut s1 = Stream::builder()
//                     .with_decoder(d1)
//                     .with_parser(p1)
//                     .sizeof_arena(self.sizeof_stream_arena)
//                     .sizeof_buffer(*budget.mem::<MStreamBuffer>())
//                     .build()
//                     .unwrap();

//                 let mut q1 = s1.query::<fastq::Record>();

//                 let mut b1: Vec<fastq::OwnedRecord> = Vec::with_capacity(10000);
//                 while let Ok(Some(token)) = q1.next() {
//                     b1.push(token.into());

//                     if b1.len() >= 10000 {
//                         break;
//                     }
//                 }

//                 log_info!("Finished reading first 10000 reads of R1...");
//                 unsafe {
//                     s1.shutdown();
//                 }

//                 let d2 = decode::Bgzf::builder()
//                     .path(input_r2.path())
//                     .num_threads(*budget.threads::<TRead>())
//                     .build()
//                     .unwrap();
//                 let p2 = parse::Fastq::builder().build().unwrap();

//                 let mut s2 = Stream::builder()
//                     .with_decoder(d2)
//                     .with_parser(p2)
//                     .sizeof_arena(self.sizeof_stream_arena)
//                     .sizeof_buffer(*budget.mem::<MStreamBuffer>())
//                     .build()
//                     .unwrap();

//                 let mut q2 = s2.query::<fastq::Record>();

//                 let mut b2: Vec<fastq::OwnedRecord> = Vec::with_capacity(10000);
//                 while let Ok(Some(token)) = q2.next() {
//                     b2.push(token.into());

//                     if b2.len() >= 10000 {
//                         break;
//                     }
//                 }

//                 log_info!("Finished reading first 10000 reads of R2...");
//                 unsafe {
//                     s2.shutdown();
//                 }

//                 let _ = chemistry.prepare_using_rp_vecs(b1, b2);
//             }
//             log_info!("Finished preparing chemistry...");
//             // std::process::exit(0);
//             let ((r1_rx, r2_rx), (r1_handle, r2_handle)) =
//                 spawn_paired_readers(vec_input, &budget, self.sizeof_stream_arena);

//             let (rp_rx, rt_handle) = spawn_debarcode_router(r1_rx, r2_rx);
//             let (db_rx, db_handles, chemistry) = spawn_debarcode_workers(rp_rx, chemistry, &budget);

//             let (ct_rx, ct_handle) = spawn_collector(db_rx, &budget);
//             let (st_rx, st_handles) = spawn_sort_workers(ct_rx, chemistry, &budget);

//             let wt_handles = spawn_chunk_writers(
//                 st_rx,
//                 timestamp_temp_files.clone(),
//                 path_temp_dir.clone(),
//                 &budget,
//             );

//             log_info!("Waiting for R1 and R2 reader threads to finish...");
//             r1_handle.join().expect("R1 reader thread panicked");
//             r2_handle.join().expect("R2 reader thread panicked");
//             log_info!("R1 and R2 reader threads finished");

//             log_info!("Waiting for router thread to finish...");
//             rt_handle.join().expect("Router thread panicked");
//             log_info!("Router thread finished");

//             log_info!(
//                 "Waiting for {} debarcode worker threads to finish...",
//                 db_handles.len()
//             );
//             for (i, handle) in IntoIterator::into_iter(db_handles).enumerate() {
//                 handle
//                     .join()
//                     .expect(&format!("Worker thread {} panicked", i));
//             }
//             log_info!("All debarcode worker threads finished");

//             log_info!("Waiting for collector thread to finish...");
//             ct_handle.join().expect("Collector thread panicked");
//             log_info!("Collector thread finished");

//             log_info!(
//                 "Waiting for {} sort worker threads to finish...",
//                 st_handles.len()
//             );
//             for (i, handle) in IntoIterator::into_iter(st_handles).enumerate() {
//                 handle
//                     .join()
//                     .expect(&format!("Sort worker thread {} panicked", i));
//             }
//             log_info!("All sort worker threads finished");

//             log_info!(
//                 "Waiting for {} chunk writer threads to finish...",
//                 wt_handles.len()
//             );
//             for (i, handle) in IntoIterator::into_iter(wt_handles).enumerate() {
//                 let paths: Vec<DebarcodeMergeInput> = handle
//                     .join()
//                     .expect(&format!("Writer thread {} panicked", i))
//                     .iter()
//                     .filter_map(|p| {
//                         match DebarcodeMergeInput::try_from_path(p) {
//                             Ok(file) => Some(file),
//                             Err(e) => {
//                                 log_warning!("Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
//                                 None
//                             }
//                         }})
//                     .collect();

//                 vec_input_debarcode_merge.extend(paths);
//             }
//             log_info!(
//                 "All chunk writer threads finished. Total chunks: {}",
//                 vec_input_debarcode_merge.len()
//             );
//         }

//         let mut mergeround_counter = 1;
//         let mut mergeround_merge_next = vec_input_debarcode_merge;

//         while mergeround_merge_next.len() > vec_output.len() {
//             log_info!(
//                 "Mergesort round {mergeround_counter}: Starting with {} files, target: {} files",
//                 mergeround_merge_next.len(),
//                 vec_output.len()
//             );

//             let current_count = mergeround_merge_next.len();
//             let target_count = vec_output.len();
//             let files_to_merge = current_count - target_count;

//             let (files_to_merge, files_to_keep): (
//                 Vec<(usize, DebarcodeMergeInput)>,
//                 Vec<(usize, DebarcodeMergeInput)>,
//             ) = IntoIterator::into_iter(mergeround_merge_next)
//                 .enumerate()
//                 .partition(|(i, _)| *i < files_to_merge * 2);

//             let files_to_merge: Vec<DebarcodeMergeInput> = IntoIterator::into_iter(files_to_merge)
//                 .map(|(_, file)| file)
//                 .collect();
//             let files_to_keep: Vec<DebarcodeMergeInput> = IntoIterator::into_iter(files_to_keep)
//                 .into_iter()
//                 .map(|(_, file)| file)
//                 .collect();

//             let (ms_rx, ms_handles) =
//                 spawn_mergesort_workers(files_to_merge, &budget, self.sizeof_stream_arena);

//             let wt_handles = spawn_mergesort_writers(
//                 ms_rx,
//                 timestamp_temp_files.clone(),
//                 mergeround_counter,
//                 path_temp_dir.clone(),
//                 &budget,
//             );

//             log_info!(
//                 "Mergesort round {mergeround_counter}: Waiting for {} mergesort threads to finish...",
//                 ms_handles.len()
//             );
//             for handle in ms_handles {
//                 handle.join().unwrap();
//             }
//             log_info!(
//                 "Mergesort round {mergeround_counter}: All mergesort worker threads finished"
//             );

//             log_info!(
//                 "Mergesort round {mergeround_counter}: Waiting for {} sorted cell writer threads to finish...",
//                 wt_handles.len()
//             );

//             // Collect outputs from current round
//             mergeround_merge_next = files_to_keep; // Start with passthrough files
//             for (i, handle) in IntoIterator::into_iter(wt_handles).enumerate() {
//                 let paths: Vec<DebarcodeMergeInput> = handle
//                     .join()
//                     .expect(&format!("Writer thread {} panicked", i))
//                     .iter()
//                     .filter_map(|p| {
//                         match DebarcodeMergeInput::try_from_path(p) {
//                             Ok(file) => Some(file),
//                             Err(e) => {
//                                 log_warning!("Mergesort round {mergeround_counter}: Failed to open merge file, skipping"; "path" => ?p, "error" => %e);
//                                 None
//                             }
//                         }})
//                     .collect();

//                 mergeround_merge_next.extend(paths);
//             }

//             log_info!(
//                 "Mergesort round {}: Finished with {} files",
//                 mergeround_counter,
//                 mergeround_merge_next.len()
//             );
//             mergeround_counter += 1;
//         }

//         let mut output_paths = Vec::new();
//         for (final_file, output_file) in mergeround_merge_next.iter().zip(vec_output.iter()) {
//             let final_path = final_file.path();
//             let output_path = output_file.path();
//             match std::fs::rename(final_file.path(), output_file.path()) {
//                 Ok(_) => {
//                     log_info!("Moved {final_path:?} -> {output_path:?}");
//                     output_paths.push(output_file.path());
//                 }
//                 Err(e) => {
//                     log_warning!("Failed moving {final_path:?} -> {output_path:?}"; "error" => %e);
//                     output_paths.push(final_file.path());
//                 }
//             }
//         }

//         let mut hist_hashmap: gxhash::HashMap<Vec<u8>, u64> = gxhash::HashMap::new();
//         for (i, output_path) in output_paths.iter().enumerate() {
//             let input = match DebarcodeMergeInput::try_from_path(output_path) {
//                 Ok(i) => i,
//                 Err(e) => {
//                     log_critical!("Failed to verify hist input file"; "path" => ?output_path, "error" => %e);
//                 }
//             };

//             let decoder = decode::Bgzf::builder()
//                 .path(input.path())
//                 .num_threads(*budget.threads::<Total>())
//                 .build()
//                 .unwrap();
//             let parser = parse::Tirp::builder().build().unwrap();

//             let mut stream = Stream::builder()
//                 .with_decoder(decoder)
//                 .with_parser(parser)
//                 .sizeof_arena(self.sizeof_stream_arena)
//                 .sizeof_buffer(*budget.mem::<MStreamBuffer>())
//                 .build()
//                 .unwrap();
//             let mut query = stream
//                 .query::<DebarcodedPartialCell>()
//                 .group_relaxed_with_context::<Id, Id, _>(
//                     |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
//                         std::cmp::Ordering::Less => panic!("Unordered record list\nCurrent ID: {:?}\nContext ID: {:?}\nCurrent (lossy): {}\nContext (lossy): {}",
//                             id, id_ctx, String::from_utf8_lossy(id), String::from_utf8_lossy(id_ctx)),
//                         std::cmp::Ordering::Equal => QueryResult::Keep,
//                         std::cmp::Ordering::Greater => QueryResult::Emit,
//                     },
//                 );

//             while let Ok(Some(cell)) = query.next() {
//                 let n = cell.len() as u64;
//                 let _ = *hist_hashmap
//                     .entry(cell.get_bytes::<Id>().to_vec())
//                     .and_modify(|c| *c += n)
//                     .or_insert(n);
//             }

//             let hist_path = if let Some(ref hist_paths) = self.paths_hist {
//                 &hist_paths[i]
//             } else {
//                 &PathBuf::from(format!("{}.hist", output_paths[i].display()))
//             };

//             let hist_output = match DebarcodeHistOutput::try_from_path(&hist_path) {
//                 Ok(out) => out,
//                 Err(e) => {
//                     log_critical!("Failed to verify hist output file"; "path" => ?hist_path, "error" => %e)
//                 }
//             };

//             let hist_file = match std::fs::File::create(hist_path) {
//                 Ok(file) => file,
//                 Err(e) => {
//                     log_critical!("Failed to create output file"; "path" => ?hist_path, "error" => %e);
//                 }
//             };

//             let mut hist_writer = match DebarcodeHistWriter::try_from_output(&hist_output) {
//                 Ok(w) => w,
//                 Err(e) => {
//                     log_critical!("Failed to create hist output writer"; "path" => ?hist_path, "error" => %e);
//                 }
//             };

//             hist_writer = hist_writer.set_writer(BufWriter::new(hist_file));
//             let _ = hist_writer.write_hist(&hist_hashmap);
//             if let Some(mut writer) = hist_writer.get_writer() {
//                 let _ = writer.flush();
//             }
//             hist_hashmap.clear();
//         }

//         Ok(())
//     }
// }

// fn spawn_paired_readers(
//     vec_input: Vec<(DebarcodeReadsInput, DebarcodeReadsInput)>,
//     budget: &GetrawBudget,
//     stream_arena: ByteSize,
// ) -> (
//     (Receiver<fastq::Record>, Receiver<fastq::Record>),
//     (JoinHandle<()>, JoinHandle<()>),
// ) {
//     let (r1_tx, r1_rx) = crossbeam::channel::unbounded();
//     let (r2_tx, r2_rx) = crossbeam::channel::unbounded();
//     let arc_vec_input = Arc::new(vec_input);
//     let stream_each_n_threads = BoundedU64::new_saturating(budget.threads::<TRead>().get() / 2);
//     let stream_each_sizeof_arena = ByteSize(stream_arena.as_u64() / 2);
//     let stream_each_sizeof_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);

//     let input_r1 = Arc::clone(&arc_vec_input);
//     let handle_r1 = std::thread::spawn(move || {
//         for (input_r1, _) in &*input_r1 {
//             let d1 = decode::Bgzf::builder()
//                 .path(input_r1.path())
//                 .num_threads(stream_each_n_threads)
//                 .build()
//                 .unwrap();
//             let p1 = parse::Fastq::builder().build().unwrap();

//             let mut s1 = Stream::builder()
//                 .with_decoder(d1)
//                 .with_parser(p1)
//                 .sizeof_arena(stream_each_sizeof_arena)
//                 .sizeof_buffer(stream_each_sizeof_buffer)
//                 .build()
//                 .unwrap();

//             let mut q1 = s1.query::<fastq::Record>();

//             while let Ok(Some(record)) = q1.next() {
//                 let _ = r1_tx.send(record);
//             }
//             log_info!("R1 finished reading");
//         }
//     });

//     // let r2_tx = r2_tx.clone();
//     let input_r2 = Arc::clone(&arc_vec_input);
//     let handle_r2 = std::thread::spawn(move || {
//         for (_, input_r2) in &*input_r2 {
//             let d2 = decode::Bgzf::builder()
//                 .path(input_r2.path())
//                 .num_threads(stream_each_n_threads)
//                 .build()
//                 .unwrap();
//             let p2 = parse::Fastq::builder().build().unwrap();

//             let mut s2 = Stream::builder()
//                 .with_decoder(d2)
//                 .with_parser(p2)
//                 .sizeof_arena(stream_each_sizeof_arena)
//                 .sizeof_buffer(stream_each_sizeof_buffer)
//                 .build()
//                 .unwrap();

//             let mut q2 = s2.query::<fastq::Record>();

//             while let Ok(Some(record)) = q2.next() {
//                 let _ = r2_tx.send(record);
//             }
//             log_info!("R2 finished reading");
//         }
//     });

//     return ((r1_rx, r2_rx), (handle_r1, handle_r2));
// }

// fn spawn_debarcode_router(
//     r1_rx: Receiver<fastq::Record>,
//     r2_rx: Receiver<fastq::Record>,
// ) -> (Receiver<(fastq::Record, fastq::Record)>, JoinHandle<()>) {
//     let (rp_tx, rp_rx) = crossbeam::channel::unbounded();
//     let rt_handle = std::thread::spawn(move || loop {
//         match (r1_rx.recv(), r2_rx.recv()) {
//             (Ok(r1), Ok(r2)) => {
//                 let _ = rp_tx.send((r1, r2));
//             }
//             (Err(_), Err(_)) => {
//                 log_info!("Both R1 and R2 channels closed, router finishing");
//                 break;
//             }
//             (Ok(_), Err(_)) => {
//                 log_warning!("R2 channel closed but R1 still has data");
//                 break;
//             }
//             (Err(_), Ok(_)) => {
//                 log_warning!("R1 channel closed but R2 still has data");
//                 break;
//             }
//         }
//     });

//     return (rp_rx, rt_handle);
// }

// fn spawn_debarcode_workers(
//     rp_rx: Receiver<(fastq::Record, fastq::Record)>,
//     chemistry: GetRawChemistry,
//     budget: &GetrawBudget,
// ) -> (
//     Receiver<(u32, DebarcodedRecord)>,
//     Vec<JoinHandle<()>>,
//     GetRawChemistry,
// ) {
//     let mut thread_handles = Vec::with_capacity(budget.threads::<TDebarcode>().get() as usize);
//     let (ct_tx, ct_rx) = crossbeam::channel::unbounded();

//     let atomic_total_counter = Arc::new(AtomicUsize::new(0));
//     let atomic_success_counter = Arc::new(AtomicUsize::new(0));

//     for _ in 0..budget.threads::<TDebarcode>().get() {
//         let mut chemistry = chemistry.clone();
//         let rp_rx = rp_rx.clone();
//         let ct_tx = ct_tx.clone();

//         let thread_atomic_total_counter = Arc::clone(&atomic_total_counter);
//         let thread_atomic_success_counter = Arc::clone(&atomic_success_counter);

//         let thread_handle = std::thread::spawn(move || {
//             while let Ok((r1, r2)) = rp_rx.recv() {
//                 // TODO: optimisation: barcodes are fixed-size if represented in a non string way (e.g as u64)
//                 let (bc_index, rp) = chemistry.detect_barcode_and_trim(
//                     r1.get_bytes::<R0>(),
//                     r1.get_bytes::<Q0>(),
//                     r2.get_bytes::<R0>(),
//                     r2.get_bytes::<Q0>(),
//                 );

//                 let thread_total_counter =
//                     thread_atomic_total_counter.fetch_add(1, Ordering::Relaxed) + 1;

//                 if bc_index != u32::MAX {
//                     let thread_success_counter =
//                         thread_atomic_success_counter.fetch_add(1, Ordering::Relaxed) + 1;

//                     if thread_success_counter % 1_000_000 == 0 {
//                         log_info!(
//                             "{:.2}M/{:.2}M reads successfully debarcoded",
//                             thread_success_counter as f64 / 1_000_000.0,
//                             thread_total_counter as f64 / 1_000_000.0
//                         );
//                     }

//                     // SAFETY: safe since these are slices into the same data
//                     let mut db_record = unsafe {
//                         DebarcodedRecord {
//                             id: &[],
//                             r1: std::mem::transmute(rp.r1),
//                             r2: std::mem::transmute(rp.r2),
//                             q1: std::mem::transmute(rp.q1),
//                             q2: std::mem::transmute(rp.q2),
//                             umi: std::mem::transmute(rp.umi),
//                             arena_backing: smallvec::SmallVec::new(),
//                         }
//                     };
//                     bascet_core::PushBacking::<fastq::Record, _>::push_backing(
//                         &mut db_record,
//                         r1.take_backing(),
//                     );
//                     bascet_core::PushBacking::<fastq::Record, _>::push_backing(
//                         &mut db_record,
//                         r2.take_backing(),
//                     );
//                     let _ = ct_tx.send((bc_index, db_record));
//                 }
//             }
//         });

//         thread_handles.push(thread_handle);
//     }

//     drop(ct_tx);
//     return (ct_rx, thread_handles, chemistry);
// }

// fn spawn_collector(
//     db_rx: Receiver<(u32, DebarcodedRecord)>,
//     budget: &GetrawBudget,
// ) -> (Receiver<Vec<(u32, DebarcodedRecord)>>, JoinHandle<()>) {
//     let (ct_tx, ct_rx) = crossbeam::channel::unbounded();
//     let collection_each_target_cloned_size_bytes =
//         ByteSize(budget.mem::<MSortBuffer>().as_u64() / budget.threads::<TSort>().get());

//     let ct_handle = std::thread::spawn(move || {
//         let mut collection_buffer: Vec<(u32, DebarcodedRecord)> = Vec::new();
//         let mut collection_cloned_size_bytes = ByteSize(0);
//         let timeout = std::time::Duration::from_secs(4);
//         loop {
//             match db_rx.recv_timeout(timeout) {
//                 Ok((bc_index, db_record)) => {
//                     let cell_mem_size = ByteSize(
//                         (db_record.get_ref::<Id>().len()
//                             + db_record.get_ref::<R1>().len()
//                             + db_record.get_ref::<R2>().len()
//                             + db_record.get_ref::<Q1>().len()
//                             + db_record.get_ref::<Q2>().len()
//                             + db_record.get_ref::<Umi>().len()) as u64,
//                     );

//                     if cell_mem_size + collection_cloned_size_bytes
//                         > collection_each_target_cloned_size_bytes
//                     {
//                         let _ = ct_tx.send(collection_buffer);
//                         collection_buffer = Vec::new();
//                         collection_cloned_size_bytes = ByteSize(0);
//                     }

//                     collection_buffer.push((bc_index, db_record));
//                     collection_cloned_size_bytes += cell_mem_size;
//                 }
//                 Err(RecvTimeoutError::Timeout) => {
//                     if !collection_buffer.is_empty() {
//                         let _ = ct_tx.send(collection_buffer);
//                         collection_buffer = Vec::new();
//                         collection_cloned_size_bytes = ByteSize(0);
//                     }
//                 }
//                 Err(RecvTimeoutError::Disconnected) => {
//                     break;
//                 }
//             }
//         }

//         if !collection_buffer.is_empty() {
//             let _ = ct_tx.send(collection_buffer);
//         }
//     });

//     return (ct_rx, ct_handle);
// }

// fn spawn_sort_workers(
//     ct_rx: Receiver<Vec<(u32, DebarcodedRecord)>>,
//     chemistry: GetRawChemistry,
//     budget: &GetrawBudget,
// ) -> (Receiver<Vec<OwnedDebarcodedRecord>>, Vec<JoinHandle<()>>) {
//     let numof_sort_threads = budget.threads::<TSort>().get() as usize;
//     let mut thread_handles = Vec::with_capacity(numof_sort_threads);
//     let (st_tx, st_rx) = crossbeam::channel::bounded(numof_sort_threads);

//     for _ in 0..numof_sort_threads {
//         let ct_rx = ct_rx.clone();
//         let st_tx = st_tx.clone();
//         let thread_chemistry = chemistry.clone();

//         let thread_handle = std::thread::spawn(move || {
//             while let Ok(vec_bc_indices_db_records) = ct_rx.recv() {
//                 // HACK: Convert barcode before sorting for correct ordering
//                 // NOTE: sort in descending order to be able to pop off the end (O(1) rather than O(n))
//                 // NOTE: to save memory conversion to owned cells is NOT done via map but rather by popping
//                 let mut records_with_bc: Vec<(Vec<u8>, DebarcodedRecord)> =
//                     IntoIterator::into_iter(vec_bc_indices_db_records)
//                         .map(|(bc_index, db_record)| {
//                             let id_as_bc = thread_chemistry.bcindexu32_to_bcu8(&bc_index).to_vec();
//                             (id_as_bc, db_record)
//                         })
//                         .collect();

//                 glidesort::sort_by(&mut records_with_bc, |(bc_a, _), (bc_b, _)| {
//                     Ord::cmp(bc_b, bc_a)
//                 });
//                 let mut owned_list: Vec<OwnedDebarcodedRecord> =
//                     Vec::with_capacity(records_with_bc.len());
//                 let halfway = records_with_bc.len() / 2;

//                 while let Some((id_as_bc, record)) = records_with_bc.pop() {
//                     let mut owned: OwnedDebarcodedRecord = record.into();
//                     *owned.get_mut::<Id>() = id_as_bc;
//                     owned_list.push(owned);

//                     if records_with_bc.len() == halfway {
//                         records_with_bc.shrink_to_fit();
//                     }
//                 }

//                 let _ = st_tx.send(owned_list);
//             }
//         });
//         thread_handles.push(thread_handle);
//     }

//     drop(st_tx);
//     return (st_rx, thread_handles);
// }

// fn spawn_chunk_writers(
//     st_rx: Receiver<Vec<OwnedDebarcodedRecord>>,
//     timestamp_temp_files: String,
//     path_temp_dir: PathBuf,
//     budget: &GetrawBudget,
// ) -> Vec<JoinHandle<Vec<PathBuf>>> {
//     let numof_write_threads = budget.threads::<TWrite>().get() as usize;
//     let mut thread_handles = Vec::with_capacity(numof_write_threads);
//     let atomic_counter = Arc::new(AtomicUsize::new(0));

//     let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
//     for _ in 0..numof_write_threads {
//         let st_rx = st_rx.clone();

//         let thread_counter = Arc::clone(&atomic_counter);
//         let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
//         let thread_path_temp_dir = path_temp_dir.clone();
//         let mut thread_vec_temp_written = Vec::new();
//         let thread_handle = std::thread::spawn(move || {
//             while let Ok(sorted_cell_list) = st_rx.recv() {
//                 let thread_counter = thread_counter.fetch_add(1, Ordering::Relaxed) + 1;
//                 let temp_path = thread_path_temp_dir
//                     .join(format!(
//                         "{thread_timestamp_temp_files}_merge_0_{thread_counter}"
//                     ))
//                     .with_extension("tirp.gz");

//                 let temp_output = match DebarcodeMergeOutput::try_from_path(&temp_path) {
//                     Ok(out) => out,
//                     Err(e) => {
//                         log_critical!("Failed to verify temp output file"; "path" => ?temp_path, "error" => %e);
//                     }
//                 };
//                 let temp_file = match std::fs::File::create(&temp_path) {
//                     Ok(file) => file,
//                     Err(e) => {
//                         log_critical!("Failed to create output file"; "path" => ?temp_path, "error" => %e);
//                     }
//                 };

//                 let temp_writer = TsvWriter::with(BGZFMultiThreadWriter::new(
//                     BufWriter::new(temp_file),
//                     Compression::fast(),
//                 ));
//                 let mut temp_writer =
//                     temp_writer.with_fields::<<OwnedDebarcodedRecord as Composite>::Attrs>();

//                 for cell in sorted_cell_list {
//                     temp_writer.write(&cell);
//                 }
//                 temp_writer.flush();
//                 // log_info!("Wrote debarcoded cell chunk"; "path" => ?temp_path, "cells" => sorted_cell_list.len());
//                 thread_vec_temp_written.push(temp_path);
//             }
//             return thread_vec_temp_written;
//         });
//         thread_handles.push(thread_handle);
//     }

//     return thread_handles;
// }

// fn spawn_mergesort_workers(
//     debarcode_merge: Vec<DebarcodeMergeInput>,
//     budget: &GetrawBudget,
//     stream_arena: ByteSize,
// ) -> (
//     Receiver<Receiver<DebarcodedPartialCell>>,
//     Vec<JoinHandle<()>>,
// ) {
//     let (fp_tx, fp_rx) = crossbeam::channel::unbounded();
//     let (ms_tx, ms_rx) = crossbeam::channel::bounded(4);
//     let numof_sort_threads = budget.threads::<TSort>().get();
//     let numof_stream_each_threads =
//         BoundedU64::new_saturating(budget.threads::<TRead>().get() / numof_sort_threads + 1);
//     let sizeof_stream_each_arena = stream_arena;
//     let sizeof_stream_each_buffer =
//         ByteSize(budget.mem::<MStreamBuffer>().as_u64() / numof_stream_each_threads);

//     let mut thread_handles = Vec::new();

//     let producer_ms_tx = ms_tx.clone();
//     let producer_handle = std::thread::spawn(move || {
//         // Handle odd file case by copying the last file directly
//         if debarcode_merge.len() % 2 == 1 {
//             let last_file = debarcode_merge.last().unwrap();
//             // HACK: Use bounded channel to prevent memory accumulation when writer is slow
//             let (mc_tx, mc_rx) = crossbeam::channel::unbounded();
//             let _ = producer_ms_tx.send(mc_rx);

//             let d1 = decode::Bgzf::builder()
//                 .path(last_file.path())
//                 .num_threads(numof_stream_each_threads)
//                 .build()
//                 .unwrap();
//             let p1 = parse::Tirp::builder().build().unwrap();

//             let mut s1 = Stream::builder()
//                 .with_decoder(d1)
//                 .with_parser(p1)
//                 .sizeof_arena(sizeof_stream_each_arena)
//                 .sizeof_buffer(sizeof_stream_each_buffer)
//                 .build()
//                 .unwrap();

//             let mut q1 = s1
//                 .query::<DebarcodedPartialCell>()
//                 .group_relaxed_with_context::<Id, Id, _>(
//                     |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
//                         std::cmp::Ordering::Less => panic!(
//                             "Unordered record list: {:?}, id: {:?}, ctx: {:?}",
//                             last_file.path(),
//                             String::from_utf8_lossy(id),
//                             String::from_utf8_lossy(id_ctx)
//                         ),
//                         std::cmp::Ordering::Equal => QueryResult::Keep,
//                         std::cmp::Ordering::Greater => QueryResult::Emit,
//                     },
//                 );

//             while let Ok(Some(cell)) = q1.next() {
//                 let _ = mc_tx.send(cell);
//             }
//             if let Err(e) = std::fs::remove_file(last_file.path()) {
//                 log_critical!("Failed to delete odd file."; "path" => ?last_file.path(), "error" => %e);
//             }
//         }

//         // Process pairs normally
//         let debarcode_merge_paired = bascet_core::Collection::into_iter(debarcode_merge).tuples();
//         for (a, b) in debarcode_merge_paired {
//             let _ = fp_tx.send((a, b));
//         }
//     });
//     thread_handles.push(producer_handle);

//     for _ in 0..numof_sort_threads {
//         let fp_rx = fp_rx.clone();
//         let ms_tx = ms_tx.clone();

//         let thread_handle = std::thread::spawn(move || {
//             while let Ok((fa, fb)) = fp_rx.recv() {
//                 log_info!("Merging pair: {:?} + {:?}", &fa.path(), &fb.path());
//                 // HACK: Use bounded channel to prevent memory accumulation when writer is slow
//                 let (mc_tx, mc_rx) = crossbeam::channel::unbounded();
//                 let _ = ms_tx.send(mc_rx);

//                 let da = decode::Bgzf::builder()
//                     .path(fa.path())
//                     .num_threads(numof_stream_each_threads)
//                     .build()
//                     .unwrap();
//                 let pa = parse::Tirp::builder().build().unwrap();

//                 let mut sa = Stream::builder()
//                     .with_decoder(da)
//                     .with_parser(pa)
//                     .sizeof_arena(sizeof_stream_each_arena)
//                     .sizeof_buffer(sizeof_stream_each_buffer)
//                     .build()
//                     .unwrap();

//                 let mut qa = sa
//                     .query::<DebarcodedPartialCell>()
//                     .group_relaxed_with_context::<Id, Id, _>(
//                         |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
//                             std::cmp::Ordering::Less => panic!(
//                                 "Unordered record list: {:?}, id: {:?}, ctx: {:?}",
//                                 fa.path(),
//                                 String::from_utf8_lossy(id),
//                                 String::from_utf8_lossy(id_ctx)
//                             ),
//                             std::cmp::Ordering::Equal => QueryResult::Keep,
//                             std::cmp::Ordering::Greater => QueryResult::Emit,
//                         },
//                     );

//                 let db = decode::Bgzf::builder()
//                     .path(fb.path())
//                     .num_threads(numof_stream_each_threads)
//                     .build()
//                     .unwrap();
//                 let pb = parse::Tirp::builder().build().unwrap();

//                 let mut sb = Stream::builder()
//                     .with_decoder(db)
//                     .with_parser(pb)
//                     .sizeof_arena(sizeof_stream_each_arena)
//                     .sizeof_buffer(sizeof_stream_each_buffer)
//                     .build()
//                     .unwrap();

//                 let mut qb = sb
//                     .query::<DebarcodedPartialCell>()
//                     .group_relaxed_with_context::<Id, Id, _>(
//                         |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
//                             std::cmp::Ordering::Less => panic!(
//                                 "Unordered record list: {:?}, id: {:?}, ctx: {:?}",
//                                 fb.path(),
//                                 String::from_utf8_lossy(id),
//                                 String::from_utf8_lossy(id_ctx)
//                             ),
//                             std::cmp::Ordering::Equal => QueryResult::Keep,
//                             std::cmp::Ordering::Greater => QueryResult::Emit,
//                         },
//                     );

//                 let mut cell_a = qa.next().ok().flatten();
//                 let mut cell_b = qb.next().ok().flatten();

//                 while let (Some(ref ca), Some(ref cb)) = (&cell_a, &cell_b) {
//                     if ca.get_ref::<Id>() <= cb.get_ref::<Id>() {
//                         let _ = mc_tx.send(cell_a.take().unwrap());
//                         cell_a = qa.next().ok().flatten();
//                     } else {
//                         let _ = mc_tx.send(cell_b.take().unwrap());
//                         cell_b = qb.next().ok().flatten();
//                     }
//                 }

//                 while let Some(ca) = cell_a {
//                     let _ = mc_tx.send(ca);
//                     cell_a = qa.next().ok().flatten();
//                 }
//                 while let Some(cb) = cell_b {
//                     let _ = mc_tx.send(cb);
//                     cell_b = qb.next().ok().flatten();
//                 }

//                 if let Err(e) = std::fs::remove_file(fa.path()) {
//                     log_warning!("Failed to delete merged file."; "path" => ?fa.path(), "error" => %e);
//                 }
//                 if let Err(e) = std::fs::remove_file(fb.path()) {
//                     log_warning!("Failed to delete merged file."; "path" => ?fb.path(), "error" => %e);
//                 }
//             }
//         });
//         thread_handles.push(thread_handle);
//     }

//     return (ms_rx, thread_handles);
// }

// fn spawn_mergesort_writers(
//     ms_rx: Receiver<Receiver<DebarcodedPartialCell>>,
//     timestamp_temp_files: String,
//     mergeround_temp_files: usize,
//     path_temp_dir: PathBuf,
//     budget: &GetrawBudget,
// ) -> Vec<JoinHandle<Vec<PathBuf>>> {
//     let mut thread_handles = Vec::new();
//     let atomic_counter = Arc::new(AtomicUsize::new(0));

//     let arc_timestamp_temp_files = Arc::new(timestamp_temp_files);
//     for _ in 0..budget.threads::<TWrite>().get() {
//         let ms_rx = ms_rx.clone();
//         let thread_counter = Arc::clone(&atomic_counter);
//         let thread_timestamp_temp_files = Arc::clone(&arc_timestamp_temp_files);
//         let thread_path_temp_dir = path_temp_dir.clone();
//         let mut thread_vec_temp_written = Vec::new();

//         let thread_handle = std::thread::spawn(move || {
//             while let Ok(mc_rx) = ms_rx.recv() {
//                 let mut cells_written = 0;
//                 let thread_counter = thread_counter.fetch_add(1, Ordering::Relaxed) + 1;
//                 let temp_path = thread_path_temp_dir
//                     .join(format!(
//                         "{thread_timestamp_temp_files}_merge_{mergeround_temp_files}_{thread_counter}"
//                     ))
//                     .with_extension("tirp.gz");

//                 let temp_file = match std::fs::File::create(&temp_path) {
//                     Ok(file) => file,
//                     Err(e) => {
//                         log_critical!("Failed to create output file"; "path" => ?temp_path, "error" => %e);
//                     }
//                 };

//                 let temp_writer = TsvWriter::with(BGZFMultiThreadWriter::new(
//                     BufWriter::new(temp_file),
//                     Compression::fast(),
//                 ));
//                 let mut temp_writer =
//                     temp_writer.with_fields::<<DebarcodedPartialCell as Composite>::Attrs>();

//                 while let Ok(cell) = mc_rx.recv() {
//                     for record in cell.records() {
//                         temp_writer.write(&record);
//                     }
//                 }
//                 temp_writer.flush();

//                 log_info!("Wrote sorted cell chunk"; "path" => ?temp_path, "cells" => cells_written);
//                 thread_vec_temp_written.push(temp_path);
//             }
//             return thread_vec_temp_written;
//         });
//         thread_handles.push(thread_handle);
//     }

//     return thread_handles;
// }

// #[derive(Composite, Default)]
// #[bascet(attrs = (Id, R1, R2, Q1, Q2, Umi), backing = ArenaBacking, marker = AsRecord)]
// pub struct DebarcodedRecord {
//     id: &'static [u8],
//     r1: &'static [u8],
//     r2: &'static [u8],
//     q1: &'static [u8],
//     q2: &'static [u8],
//     umi: &'static [u8],

//     // SAFETY: exposed ONLY to allow conversion outside this crate.
//     //         be VERY careful modifying this at all
//     arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
// }

// impl Into<OwnedDebarcodedRecord> for DebarcodedRecord {
//     fn into(self) -> OwnedDebarcodedRecord {
//         OwnedDebarcodedRecord {
//             id: self.id.to_vec(),
//             r1: self.r1.to_vec(),
//             r2: self.r2.to_vec(),
//             q1: self.q1.to_vec(),
//             q2: self.q2.to_vec(),
//             umi: self.umi.to_vec(),
//             owned_backing: (),
//         }
//     }
// }

// #[derive(Composite, Default, Clone)]
// #[bascet(attrs = (Id, R1, R2, Q1, Q2, Umi), backing = OwnedBacking, marker = AsRecord)]
// pub struct OwnedDebarcodedRecord {
//     id: Vec<u8>,
//     r1: Vec<u8>,
//     r2: Vec<u8>,
//     q1: Vec<u8>,
//     q2: Vec<u8>,
//     umi: Vec<u8>,

//     owned_backing: (),
// }

// #[derive(Composite, Default)]
// #[bascet(
//     attrs = (Id, R1 = vec_r1, R2 = vec_r2, Q1 = vec_q1, Q2 = vec_q2, Umi = vec_umis),
//     backing = ArenaBacking,
//     marker = AsCell<Accumulate>,
//     intermediate = tirp::Record
// )]
// pub struct DebarcodedPartialCell {
//     id: &'static [u8],
//     #[collection]
//     vec_r1: Vec<&'static [u8]>,
//     #[collection]
//     vec_r2: Vec<&'static [u8]>,
//     #[collection]
//     vec_q1: Vec<&'static [u8]>,
//     #[collection]
//     vec_q2: Vec<&'static [u8]>,
//     #[collection]
//     vec_umis: Vec<&'static [u8]>,

//     // SAFETY: exposed ONLY to allow conversion outside this crate.
//     //         be VERY careful modifying this at all
//     pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
// }

// // base len off of R1 len. This could in theory be any collection field
// impl CompositeLen for DebarcodedPartialCell {
//     fn len(&self) -> usize {
//         self.get_ref::<R1>().len()
//     }
// }

// #[derive(Clone)]
// pub struct DebarcodeAtrandiWGSChemistry {
//     barcode: CombinatorialBarcode8bp,
// }
// impl DebarcodeAtrandiWGSChemistry {
//     pub fn new() -> Self {
//         let mut result = DebarcodeAtrandiWGSChemistry {
//             barcode: CombinatorialBarcode8bp::new(),
//         };

//         let reader = Cursor::new(include_bytes!("../barcode/atrandi_barcodes.tsv"));
//         for (index, line) in reader.lines().enumerate() {
//             if index == 0 {
//                 continue;
//             }

//             let line = line.unwrap();
//             let parts: Vec<&str> = line.split('\t').collect();
//             result.barcode.add_bc(parts[1], parts[0], parts[2]);
//         }

//         result.barcode.pools[3].pos_anchor = (8 + 4) * 0;
//         result.barcode.pools[3].pos_rel_anchor = vec![0, 1];

//         result.barcode.pools[2].pos_anchor = (8 + 4) * 1;
//         result.barcode.pools[2].pos_rel_anchor = vec![0, 1];

//         result.barcode.pools[1].pos_anchor = (8 + 4) * 2;
//         result.barcode.pools[1].pos_rel_anchor = vec![0, 1];

//         result.barcode.pools[0].pos_anchor = (8 + 4) * 3;
//         result.barcode.pools[0].pos_rel_anchor = vec![0, 1];

//         result
//     }
// }
// impl crate::barcode::Chemistry for DebarcodeAtrandiWGSChemistry {
//     fn prepare_using_rp_vecs<C: bascet_core::Composite>(
//         &mut self,
//         _vec_r1: Vec<C>,
//         _vec_r2: Vec<C>,
//     ) -> anyhow::Result<()>
//     where
//         C: bascet_core::Get<bascet_core::R0>,
//         <C as bascet_core::Get<bascet_core::R0>>::Value: AsRef<[u8]>,
//     {
//         Ok(())
//     }
//     fn detect_barcode_and_trim<'a>(
//         &mut self,
//         r1_seq: &'a [u8],
//         r1_qual: &'a [u8],
//         r2_seq: &'a [u8],
//         r2_qual: &'a [u8],
//     ) -> (u32, crate::common::ReadPair<'a>) {
//         //Detect barcode, which here is in R2
//         let total_distance_cutoff = 4;
//         let part_distance_cutoff = 1;

//         let (bc, score) =
//             self.barcode
//                 .detect_barcode(r2_seq, true, total_distance_cutoff, part_distance_cutoff);

//         match score {
//             0.. => {
//                 //R2 need to have the first part with barcodes removed. Figure out total size!
//                 let r2_from = self.barcode.trim_bcread_len;
//                 let r2_to = r2_seq.len();

//                 //Get UMI position
//                 let umi_from = self.barcode.umi_from;
//                 let umi_to = self.barcode.umi_to;
//                 (
//                     bc,
//                     common::ReadPair {
//                         r1: &r1_seq,
//                         r2: &r2_seq[r2_from..r2_to],
//                         q1: &r1_qual,
//                         q2: &r2_qual[r2_from..r2_to],
//                         umi: &r2_seq[umi_from..umi_to],
//                     },
//                 )
//             }
//             ..0 => {
//                 //Just return the sequence as-is
//                 (
//                     u32::MAX,
//                     common::ReadPair {
//                         r1: &r1_seq,
//                         r2: &r2_seq,
//                         q1: &r1_qual,
//                         q2: &r2_qual,
//                         umi: &[],
//                     },
//                 )
//             }
//         }
//     }

//     fn bcindexu32_to_bcu8(&self, index32: &u32) -> Vec<u8> {
//         let mut result = Vec::new();
//         let bytes = index32.as_bytes();
//         result.extend_from_slice(
//             self.barcode.pools[0].barcode_name_list[bytes[3] as usize].as_bytes(),
//         );
//         result.push(b'_');
//         result.extend_from_slice(
//             self.barcode.pools[1].barcode_name_list[bytes[2] as usize].as_bytes(),
//         );
//         result.push(b'_');
//         result.extend_from_slice(
//             self.barcode.pools[2].barcode_name_list[bytes[1] as usize].as_bytes(),
//         );
//         result.push(b'_');
//         result.extend_from_slice(
//             self.barcode.pools[3].barcode_name_list[bytes[0] as usize].as_bytes(),
//         );

//         return result;
//     }
// }
