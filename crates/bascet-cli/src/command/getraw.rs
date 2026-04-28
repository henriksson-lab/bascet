use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bascet_io::fastq::fastq;
use bascet_io::tirp::tirp;
use bascet_io::{BBGZFinishHandle, BBGZWriteBlock, Compression};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::{Args, Subcommand};
use clio::{InputPath, OutputPath};
use crossbeam::channel::{Receiver, RecvTimeoutError};
use itertools::izip;

use bascet_core::attr::{meta::*, quality::*, sequence::*};
use bascet_core::*;
use bascet_derive::Budget;
use bascet_io::{
    BBGZHeader, BBGZWriter,
    codec::{self, bbgz},
    parse,
};
use serde::Serialize;
use smallvec::{SmallVec, ToSmallVec};

use crate::barcode::atrandi_wgs_barcode_illumina::DebarcodeAtrandiWGSChemistryIllumina;
use crate::barcode::atrandi_wgs_barcode_longread::DebarcodeAtrandiWGSChemistryLongread;
use crate::barcode::{Chemistry, ParseBioChemistry3, TenxRNAChemistry};
use crate::command::shardify::ShardifyCMD;
use crate::utils::{atomic_temp_path, publish_atomic_output, rename_or_copy_across_filesystems};
use crate::{bbgz_compression_parser, bounded_parser};
use tracing::{debug, error, info, warn};

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
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    countof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

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
        long = "max-read-pairs",
        help = "Process at most this many read pairs from the input. Intended for benchmarking on large inputs.",
        hide_short_help = true
    )]
    max_read_pairs: Option<u64>,

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
    /// 10x chemistry, uses combinatorial 16bp barcodes for debarcoding.
    Tenx {},
}

#[derive(Clone)]
#[enum_dispatch::enum_dispatch(Chemistry)]
pub enum GetRawChemistry {
    AtrandiWGS(DebarcodeAtrandiWGSChemistryIllumina),
    AtrandiWGSLR(DebarcodeAtrandiWGSChemistryLongread),
    ParseBio(ParseBioChemistry3),
    Tenx(TenxRNAChemistry),
}

#[derive(Budget, Debug)]
struct GetrawBudget {
    #[threads(Total)]
    threads: BoundedU64<6, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.15) as u64))]
    countof_threads_read: BoundedU64<1, { u64::MAX }>,
    #[threads(TDebarcode, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.2) as u64))]
    countof_threads_debarcode: BoundedU64<1, { u64::MAX }>,

    #[threads(TSort, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.2) as u64))]
    countof_threads_sort: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.05) as u64))]
    countof_threads_write: BoundedU64<1, { u64::MAX }>,
    #[mem(MStreamBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.5) as u64))]
    sizeof_stream_buffer: ByteSize,

    #[mem(MSortBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.25) as u64))]
    sizeof_sort_buffer: ByteSize,

    #[mem(MCompressBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.25) as u64))]
    sizeof_compress_buffer: ByteSize,
    #[mem(MCompressRawBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.25) as u64))]
    sizeof_compress_raw_buffer: ByteSize,
}

fn record_queue_capacity(budget: &GetrawBudget) -> usize {
    ((*budget.threads::<Total>()).get() as usize)
        .saturating_mul(4096)
        .max(4096)
}

fn chunk_queue_capacity(budget: &GetrawBudget) -> usize {
    ((*budget.threads::<Total>()).get() as usize)
        .saturating_mul(2)
        .max(2)
}

struct ReadMemoryLimiter {
    cap: usize,
    used: Mutex<usize>,
    available: Condvar,
    wait_count: AtomicUsize,
    max_used: AtomicUsize,
}

impl ReadMemoryLimiter {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            used: Mutex::new(0),
            available: Condvar::new(),
            wait_count: AtomicUsize::new(0),
            max_used: AtomicUsize::new(0),
        }
    }

    fn acquire(self: &Arc<Self>, bytes: usize) -> ReadMemoryPermit {
        if bytes == 0 {
            return ReadMemoryPermit {
                bytes,
                limiter: Arc::clone(self),
            };
        }

        let charge = bytes.min(self.cap);
        let mut used = self.used.lock().unwrap();
        while *used + charge > self.cap {
            self.wait_count.fetch_add(1, Ordering::Relaxed);
            used = self.available.wait(used).unwrap();
        }
        *used += charge;
        self.max_used.fetch_max(*used, Ordering::Relaxed);

        ReadMemoryPermit {
            bytes: charge,
            limiter: Arc::clone(self),
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let mut used = self.used.lock().unwrap();
        *used = used.saturating_sub(bytes);
        self.available.notify_all();
    }

    fn stats(&self) -> (usize, usize, usize) {
        let used = *self.used.lock().unwrap();
        let max_used = self.max_used.load(Ordering::Relaxed);
        let wait_count = self.wait_count.load(Ordering::Relaxed);
        (used, max_used, wait_count)
    }
}

struct InFlightLimiter {
    available: Mutex<usize>,
    ready: Condvar,
}

impl InFlightLimiter {
    fn new(cap: usize) -> Self {
        Self {
            available: Mutex::new(cap.max(1)),
            ready: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> InFlightPermit {
        let mut available = self.available.lock().unwrap();
        while *available == 0 {
            available = self.ready.wait(available).unwrap();
        }
        *available -= 1;

        InFlightPermit {
            limiter: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut available = self.available.lock().unwrap();
        *available += 1;
        self.ready.notify_one();
    }
}

struct InFlightPermit {
    limiter: Arc<InFlightLimiter>,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

struct ReadMemoryPermit {
    bytes: usize,
    limiter: Arc<ReadMemoryLimiter>,
}

impl ReadMemoryPermit {
    fn merge(a: Self, b: Self) -> Self {
        let limiter = Arc::clone(&a.limiter);
        let bytes = a.bytes + b.bytes;
        std::mem::forget(a);
        std::mem::forget(b);
        Self { bytes, limiter }
    }
}

impl Drop for ReadMemoryPermit {
    fn drop(&mut self) {
        self.limiter.release(self.bytes);
    }
}

struct Budgeted<T> {
    value: T,
    permit: ReadMemoryPermit,
}

impl<T> Budgeted<T> {
    fn new(value: T, permit: ReadMemoryPermit) -> Self {
        Self { value, permit }
    }
}

impl<T> Deref for Budgeted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for Budgeted<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

type BudgetedFastqRecord = Budgeted<fastq::Record>;
type BudgetedFastqRecordBatch = Vec<BudgetedFastqRecord>;

struct BudgetedReadPairBatch {
    r1: BudgetedFastqRecordBatch,
    r2: BudgetedFastqRecordBatch,
}

impl BudgetedReadPairBatch {
    fn len(&self) -> usize {
        self.r1.len().min(self.r2.len())
    }

    fn is_empty(&self) -> bool {
        self.r1.is_empty() || self.r2.is_empty()
    }
}

type BudgetedDebarcodedRecord = Budgeted<DebarcodedRecord>;
type BudgetedDebarcodedRecordBatch = Vec<(u32, BudgetedDebarcodedRecord)>;
type HistogramCounts = BTreeMap<Vec<u8>, u64>;

struct ChunkWriterOutput {
    paths: Vec<InputPath>,
    histogram_counts: HistogramCounts,
    finish_handles: Vec<BBGZFinishHandle>,
}

#[derive(Default)]
struct GetRawBatchStats {
    read_pair_batches: AtomicUsize,
    read_pair_records: AtomicUsize,
    read_pair_max_batch: AtomicUsize,
    debarcoded_batches: AtomicUsize,
    debarcoded_records: AtomicUsize,
    debarcoded_max_batch: AtomicUsize,
    collector_flushes: AtomicUsize,
    collector_records: AtomicUsize,
    collector_max_flush: AtomicUsize,
}

impl GetRawBatchStats {
    fn record_read_pair_batch(&self, len: usize) {
        self.read_pair_batches.fetch_add(1, Ordering::Relaxed);
        self.read_pair_records.fetch_add(len, Ordering::Relaxed);
        self.read_pair_max_batch.fetch_max(len, Ordering::Relaxed);
    }

    fn record_debarcoded_batch(&self, len: usize) {
        self.debarcoded_batches.fetch_add(1, Ordering::Relaxed);
        self.debarcoded_records.fetch_add(len, Ordering::Relaxed);
        self.debarcoded_max_batch.fetch_max(len, Ordering::Relaxed);
    }

    fn record_collector_flush(&self, len: usize) {
        self.collector_flushes.fetch_add(1, Ordering::Relaxed);
        self.collector_records.fetch_add(len, Ordering::Relaxed);
        self.collector_max_flush.fetch_max(len, Ordering::Relaxed);
    }

    fn mean(records: usize, batches: usize) -> usize {
        if batches == 0 { 0 } else { records / batches }
    }

    fn log_summary(&self) {
        let read_pair_batches = self.read_pair_batches.load(Ordering::Relaxed);
        let read_pair_records = self.read_pair_records.load(Ordering::Relaxed);
        let debarcoded_batches = self.debarcoded_batches.load(Ordering::Relaxed);
        let debarcoded_records = self.debarcoded_records.load(Ordering::Relaxed);
        let collector_flushes = self.collector_flushes.load(Ordering::Relaxed);
        let collector_records = self.collector_records.load(Ordering::Relaxed);

        info!(
            read_pair_batches,
            read_pair_mean_batch = Self::mean(read_pair_records, read_pair_batches),
            read_pair_max_batch = self.read_pair_max_batch.load(Ordering::Relaxed),
            debarcoded_batches,
            debarcoded_mean_batch = Self::mean(debarcoded_records, debarcoded_batches),
            debarcoded_max_batch = self.debarcoded_max_batch.load(Ordering::Relaxed),
            collector_flushes,
            collector_mean_flush = Self::mean(collector_records, collector_flushes),
            collector_max_flush = self.collector_max_flush.load(Ordering::Relaxed),
            "GetRaw batch summary"
        );
    }
}

#[derive(Default)]
struct GetRawStageTimings {
    read_nanos: AtomicU64,
    debarcode_nanos: AtomicU64,
    collect_nanos: AtomicU64,
    sort_nanos: AtomicU64,
    write_nanos: AtomicU64,
    merge_nanos: AtomicU64,
    publish_nanos: AtomicU64,
    histogram_nanos: AtomicU64,
}

impl GetRawStageTimings {
    fn add_read(&self, duration: Duration) {
        Self::add_duration(&self.read_nanos, duration);
    }

    fn add_debarcode(&self, duration: Duration) {
        Self::add_duration(&self.debarcode_nanos, duration);
    }

    fn add_collect(&self, duration: Duration) {
        Self::add_duration(&self.collect_nanos, duration);
    }

    fn add_sort(&self, duration: Duration) {
        Self::add_duration(&self.sort_nanos, duration);
    }

    fn add_write(&self, duration: Duration) {
        Self::add_duration(&self.write_nanos, duration);
    }

    fn add_merge(&self, duration: Duration) {
        Self::add_duration(&self.merge_nanos, duration);
    }

    fn add_publish(&self, duration: Duration) {
        Self::add_duration(&self.publish_nanos, duration);
    }

    fn add_histogram(&self, duration: Duration) {
        Self::add_duration(&self.histogram_nanos, duration);
    }

    fn add_duration(counter: &AtomicU64, duration: Duration) {
        counter.fetch_add(
            duration.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn load_duration(counter: &AtomicU64) -> Duration {
        Duration::from_nanos(counter.load(Ordering::Relaxed))
    }

    fn log_summary(&self) {
        info!(
            read = ?Self::load_duration(&self.read_nanos),
            debarcode = ?Self::load_duration(&self.debarcode_nanos),
            collect = ?Self::load_duration(&self.collect_nanos),
            sort = ?Self::load_duration(&self.sort_nanos),
            write = ?Self::load_duration(&self.write_nanos),
            merge = ?Self::load_duration(&self.merge_nanos),
            publish = ?Self::load_duration(&self.publish_nanos),
            histogram = ?Self::load_duration(&self.histogram_nanos),
            "GetRaw stage timing summary"
        );
    }
}

fn estimate_fastq_record_bytes(record: &fastq::Record) -> usize {
    record.get_ref::<Id>().len() + record.get_ref::<R0>().len() + record.get_ref::<Q0>().len() + 64
}

fn read_pair_batch_capacity(budget: &GetrawBudget) -> usize {
    let _ = budget;
    10_000
}

fn default_working_stream_buffer(total_mem: u64) -> ByteSize {
    ByteSize(((total_mem as f64 * 0.5) as u64).min(ByteSize::gib(5).as_u64()))
}

fn default_working_sort_buffer(sort_threads: u64) -> ByteSize {
    ByteSize(ByteSize::mib(512).as_u64() * sort_threads.max(1))
}

fn default_working_compress_buffer(total_mem: u64) -> ByteSize {
    ByteSize(((total_mem as f64 * 0.25) as u64).min(ByteSize::gib(3).as_u64()))
}

fn first_round_sort_chunk_size(budget: &GetrawBudget) -> ByteSize {
    let sort_threads = (*budget.threads::<TSort>()).get().max(1);
    let per_sort_worker = budget.mem::<MSortBuffer>().as_u64() / sort_threads;
    ByteSize(per_sort_worker.clamp(ByteSize::mib(256).as_u64(), ByteSize::gib(2).as_u64()))
}

impl GetRawCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let total_threads = self.total_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or_else(|e| {
                    warn!(
                        error = %e,
                        "Failed to determine available parallelism, using 6 threads"
                    );
                    6
                })
                .try_into()
                .unwrap_or_else(|e| {
                    warn!(
                        error = %e,
                        "Failed to convert parallelism to valid thread count, using 6 threads"
                    );
                    6.try_into().unwrap()
                })
        });
        let sort_threads_for_default = self
            .countof_threads_sort
            .unwrap_or_else(|| {
                bounded_integer::BoundedU64::new_saturating(
                    (total_threads.get() as f64 * 0.2) as u64,
                )
            })
            .get();
        let budget = GetrawBudget::builder()
            .threads(total_threads)
            .memory(self.total_mem)
            .maybe_countof_threads_read(self.countof_threads_read)
            .maybe_countof_threads_debarcode(self.countof_threads_debarcode)
            .maybe_countof_threads_sort(self.countof_threads_sort)
            .maybe_countof_threads_write(self.countof_threads_write)
            .sizeof_stream_buffer(
                self.sizeof_stream_buffer
                    .unwrap_or_else(|| default_working_stream_buffer(self.total_mem.as_u64())),
            )
            .sizeof_sort_buffer(
                self.sizeof_sort_buffer
                    .unwrap_or_else(|| default_working_sort_buffer(sort_threads_for_default)),
            )
            .sizeof_compress_buffer(
                self.sizeof_compress_buffer
                    .unwrap_or_else(|| default_working_compress_buffer(self.total_mem.as_u64())),
            )
            .sizeof_compress_raw_buffer(
                self.sizeof_compress_raw_buffer
                    .unwrap_or_else(|| default_working_compress_buffer(self.total_mem.as_u64())),
            )
            .build();

        budget.log();
        if self.compression_level.level() == 0 {
            warn!("Compression level is 0 (uncompressed)")
        }
        let read_memory_limiter = Arc::new(ReadMemoryLimiter::new(
            budget.mem::<MStreamBuffer>().as_u64() as usize,
        ));
        let batch_stats = Arc::new(GetRawBatchStats::default());
        let stage_timings = Arc::new(GetRawStageTimings::default());
        info!(
            read_memory_cap = %ByteSize(read_memory_limiter.cap as u64),
            "Read memory limiter enabled"
        );
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads((*budget.threads::<Total>()).get() as usize)
                .thread_name(|idx| format!("getraw-rayon@{idx}"))
                .build()?,
        );
        let debarcode_inflight_limiter =
            Arc::new(InFlightLimiter::new(rayon_pool.current_num_threads()));

        let mut vec_input_debarcode_merge = self.skip_debarcode.clone().unwrap_or(Vec::new());
        let mut histogram_counts = if self.skip_debarcode.is_none() && self.paths_out.len() == 1 {
            Some(HistogramCounts::new())
        } else {
            None
        };

        if self.paths_out.is_empty() {
            error!("No valid output file paths specified. All output paths failed verification.");
            panic!("No valid output file paths specified");
        }

        if self.paths_hist.is_some()
            && self.paths_hist.as_ref().unwrap().len() != self.paths_out.len()
        {
            let n_hist = self.paths_hist.as_ref().unwrap().len();
            let n_out = self.paths_out.len();
            error!(
                "Number of histogram paths ({n_hist}) does not match number of output paths ({n_out})"
            );
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
                GetRawChemistryCMD::Tenx { .. } => GetRawChemistry::Tenx(TenxRNAChemistry::new()),
            };

            //Check if we have single-end or paired-end data
            let paths_r1 = self.paths_r1.clone();
            let paths_r2 = self.paths_r2.clone();
            if paths_r1.is_empty() {
                error!(
                    "No valid input files found. All input files failed to open or do not exist."
                );
                panic!("No valid input files found");
            }

            let ((r1_rx, r2_rx), (r1_handle, r2_handle)) = if paths_r2.len() == 0 {
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
                spawn_single_readers(
                    paths_r1,
                    &budget,
                    self.sizeof_stream_arena,
                    Arc::clone(&read_memory_limiter),
                    Arc::clone(&stage_timings),
                    self.max_read_pairs,
                )
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
                spawn_paired_readers(
                    vec_input,
                    &budget,
                    self.sizeof_stream_arena,
                    Arc::clone(&read_memory_limiter),
                    Arc::clone(&stage_timings),
                    self.max_read_pairs,
                )
            };

            let (rp_rx, rt_handle) =
                spawn_debarcode_router(r1_rx, r2_rx, &budget, Arc::clone(&batch_stats));
            let (db_rx, db_handles, chemistry) = spawn_debarcode_workers(
                rp_rx,
                chemistry,
                &budget,
                Arc::clone(&rayon_pool),
                Arc::clone(&debarcode_inflight_limiter),
                Arc::clone(&batch_stats),
                Arc::clone(&stage_timings),
            );

            let (ct_rx, ct_handle) = spawn_collector(
                db_rx,
                &budget,
                Arc::clone(&batch_stats),
                Arc::clone(&stage_timings),
            );
            let (st_rx, st_handles) =
                spawn_sort_workers(ct_rx, chemistry, &budget, Arc::clone(&stage_timings));

            let wt_handles = spawn_chunk_writers(
                st_rx,
                timestamp_temp_files.clone(),
                path_temp_dir.clone(),
                &budget,
                self.compression_level,
                &library,
                Arc::clone(&rayon_pool),
                Arc::clone(&stage_timings),
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
            batch_stats.log_summary();

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
                let output: ChunkWriterOutput = handle
                    .join()
                    .expect(&format!("Writer thread {} panicked", i));

                for (j, finish_handle) in output.finish_handles.into_iter().enumerate() {
                    let finish_started = Instant::now();
                    finish_handle
                        .join()
                        .expect(&format!("BBGZ finish thread {j} from writer {i} panicked"));
                    stage_timings.add_write(finish_started.elapsed());
                }
                vec_input_debarcode_merge.extend(output.paths);
                if let Some(ref mut histogram_counts) = histogram_counts {
                    merge_histogram_counts(histogram_counts, output.histogram_counts);
                }
            }
            debug!(
                "All chunk writer threads finished. Total chunks: {}",
                vec_input_debarcode_merge.len()
            );
        }

        let (read_memory_used, read_memory_max_used, read_memory_wait_count) =
            read_memory_limiter.stats();
        info!(
            read_memory_used = %ByteSize(read_memory_used as u64),
            read_memory_max_used = %ByteSize(read_memory_max_used as u64),
            read_memory_wait_count,
            "Read memory limiter summary"
        );

        do_merging(
            &self,
            &budget,
            &path_temp_dir,
            &timestamp_temp_files,
            &vec_input_debarcode_merge,
            histogram_counts,
            Arc::clone(&stage_timings),
        )?;
        stage_timings.log_summary();

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
    vec_input_debarcode_merge: &Vec<InputPath>,
    histogram_counts: Option<HistogramCounts>,
    stage_timings: Arc<GetRawStageTimings>,
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

            let temp_fname = format!("{}_{mergeround_counter}_{batch_idx}", timestamp_temp_files);
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

            let merge_started = Instant::now();
            spawn_mergesort_workers(
                vec_batch,
                temp_output_path,
                path_temp_dir.clone(),
                &budget,
                s.sizeof_stream_arena,
            );
            stage_timings.add_merge(merge_started.elapsed());

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
        let publish_started = Instant::now();
        match rename_or_copy_across_filesystems(&**final_path.path(), &**output_path.path()) {
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
        stage_timings.add_publish(publish_started.elapsed());
    }

    let output_hist_pairs: Vec<(OutputPath, OutputPath)> = output_paths
        .into_iter()
        .enumerate()
        .map(|(i, output_path)| {
            let hist_path = if let Some(ref hist_paths) = s.paths_hist {
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

    if let (Some(histogram_counts), [(output_path, hist_path)]) =
        (histogram_counts, output_hist_pairs.as_slice())
    {
        let _ = output_path;
        let histogram_started = Instant::now();
        write_histogram_counts(hist_path, &histogram_counts)?;
        stage_timings.add_histogram(histogram_started.elapsed());
    } else {
        let histogram_started = Instant::now();
        let hist_handles =
            spawn_histogram_workers(output_hist_pairs, &budget, s.sizeof_stream_arena);

        for (i, handle) in hist_handles.into_iter().enumerate() {
            handle
                .join()
                .expect(&format!("Histogram worker thread {} panicked", i));
        }
        stage_timings.add_histogram(histogram_started.elapsed());
        debug!("All histogram worker threads finished");
    }

    Ok(())
}

fn merge_histogram_counts(into: &mut HistogramCounts, from: HistogramCounts) {
    for (cell_id, count) in from {
        *into.entry(cell_id).or_insert(0) += count;
    }
}

fn write_histogram_counts(
    hist_path: &OutputPath,
    histogram_counts: &HistogramCounts,
) -> anyhow::Result<()> {
    let hist_final_path = hist_path.path().path().to_path_buf();
    let hist_tmp_path = atomic_temp_path(&hist_final_path);
    let hist_file = File::create(&hist_tmp_path)?;
    let mut bufwriter = BufWriter::new(hist_file);

    for (cell_id, count) in histogram_counts {
        bufwriter.write_all(cell_id)?;
        bufwriter.write_all(b"\t")?;
        bufwriter.write_all(count.to_string().as_bytes())?;
        bufwriter.write_all(b"\n")?;
    }

    bufwriter.flush()?;
    drop(bufwriter);
    publish_atomic_output(&hist_tmp_path, &hist_final_path)?;
    debug!("Wrote histogram at {}", hist_path);
    Ok(())
}

///
/// Given R1 and R2 input paths, spawn paired readers
///
fn spawn_paired_readers(
    vec_input: Vec<(InputPath, InputPath)>,
    budget: &GetrawBudget,
    stream_arena: ByteSize,
    read_memory_limiter: Arc<ReadMemoryLimiter>,
    stage_timings: Arc<GetRawStageTimings>,
    max_read_pairs: Option<u64>,
) -> (
    (
        Receiver<BudgetedFastqRecordBatch>,
        Receiver<BudgetedFastqRecordBatch>,
    ),
    (JoinHandle<()>, JoinHandle<()>),
) {
    let batch_capacity = read_pair_batch_capacity(budget);
    let queue_capacity = record_queue_capacity(budget).div_ceil(batch_capacity);
    let (r1_tx, r1_rx) = crossbeam::channel::bounded(queue_capacity);
    let (r2_tx, r2_rx) = crossbeam::channel::bounded(queue_capacity);
    let arc_vec_input = Arc::new(vec_input);
    let countof_threads_read = (*budget.threads::<TRead>()).get();
    let stream_each_n_threads = BoundedU64::new_saturating(countof_threads_read / 2);
    let sizeof_stream_each_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);
    let r1_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
    let r2_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));

    let input_r1 = Arc::clone(&arc_vec_input);
    let r1_read_memory_limiter = Arc::clone(&read_memory_limiter);
    let r1_stage_timings = Arc::clone(&stage_timings);
    let handle_r1 = budget.spawn::<TRead, _, _>(0, move || {
        let read_started = Instant::now();
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R1 reader");

        let mut records_read = 0u64;
        for (input_r1, _) in &*input_r1 {
            if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                break;
            }
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
            let mut batch = Vec::with_capacity(batch_capacity);
            let mut stop_reading = false;

            while let Ok(Some(record)) = q1.next() {
                if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                    stop_reading = true;
                    break;
                }
                let permit = r1_read_memory_limiter.acquire(estimate_fastq_record_bytes(&record));
                batch.push(Budgeted::new(record, permit));
                records_read += 1;
                if batch.len() >= batch_capacity {
                    let send_batch =
                        std::mem::replace(&mut batch, Vec::with_capacity(batch_capacity));
                    if r1_tx.send(send_batch).is_err() {
                        stop_reading = true;
                        break;
                    }
                }
            }
            if !batch.is_empty() {
                let _ = r1_tx.send(batch);
            }
            if stop_reading {
                unsafe {
                    s1.shutdown();
                }
                break;
            }
            debug!("R1 finished reading");
        }
        r1_stage_timings.add_read(read_started.elapsed());
    });

    let input_r2 = Arc::clone(&arc_vec_input);
    let r2_read_memory_limiter = Arc::clone(&read_memory_limiter);
    let r2_stage_timings = Arc::clone(&stage_timings);
    let handle_r2 = budget.spawn::<TRead, _, _>(1, move || {
        let read_started = Instant::now();
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R2 reader");

        let mut records_read = 0u64;
        for (_, input_r2) in &*input_r2 {
            if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                break;
            }
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
            let mut batch = Vec::with_capacity(batch_capacity);
            let mut stop_reading = false;

            while let Ok(Some(record)) = q2.next() {
                if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                    stop_reading = true;
                    break;
                }
                let permit = r2_read_memory_limiter.acquire(estimate_fastq_record_bytes(&record));
                batch.push(Budgeted::new(record, permit));
                records_read += 1;
                if batch.len() >= batch_capacity {
                    let send_batch =
                        std::mem::replace(&mut batch, Vec::with_capacity(batch_capacity));
                    if r2_tx.send(send_batch).is_err() {
                        stop_reading = true;
                        break;
                    }
                }
            }
            if !batch.is_empty() {
                let _ = r2_tx.send(batch);
            }
            if stop_reading {
                unsafe {
                    s2.shutdown();
                }
                break;
            }
            debug!("R2 finished reading");
        }
        r2_stage_timings.add_read(read_started.elapsed());
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
    read_memory_limiter: Arc<ReadMemoryLimiter>,
    stage_timings: Arc<GetRawStageTimings>,
    max_read_pairs: Option<u64>,
) -> (
    (
        Receiver<BudgetedFastqRecordBatch>,
        Receiver<BudgetedFastqRecordBatch>,
    ),
    (JoinHandle<()>, JoinHandle<()>),
) {
    let batch_capacity = read_pair_batch_capacity(budget);
    let queue_capacity = record_queue_capacity(budget).div_ceil(batch_capacity);
    let (r1_tx, r1_rx) = crossbeam::channel::bounded(queue_capacity);
    let (r2_tx, r2_rx) = crossbeam::channel::bounded(queue_capacity);
    let arc_vec_input = Arc::new(vec_input);
    let countof_threads_read = (*budget.threads::<TRead>()).get();
    let stream_each_n_threads = BoundedU64::new_saturating(countof_threads_read / 2);
    let sizeof_stream_each_buffer = ByteSize(budget.mem::<MStreamBuffer>().as_u64() / 2);
    let r1_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
    //let r2_shared_alloc = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));

    let input_r1 = Arc::clone(&arc_vec_input);
    let r1_read_memory_limiter = Arc::clone(&read_memory_limiter);
    let r1_stage_timings = Arc::clone(&stage_timings);
    let handle_r1 = budget.spawn::<TRead, _, _>(0, move || {
        let read_started = Instant::now();
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting R1 reader");

        let mut records_read = 0u64;
        for input_r1 in &*input_r1 {
            if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                break;
            }
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
            let mut r1_batch = Vec::with_capacity(batch_capacity);
            let mut r2_batch = Vec::with_capacity(batch_capacity);
            let mut stop_reading = false;

            while let Ok(Some(record)) = q1.next() {
                if max_read_pairs.is_some_and(|limit| records_read >= limit) {
                    stop_reading = true;
                    break;
                }
                let permit = r1_read_memory_limiter.acquire(estimate_fastq_record_bytes(&record));
                r1_batch.push(Budgeted::new(record, permit));
                records_read += 1;
                let dummy_record_r2 = bascet_io::parse::fastq::Record::empty();
                let dummy_permit = r1_read_memory_limiter.acquire(0);
                r2_batch.push(Budgeted::new(dummy_record_r2, dummy_permit));

                if r1_batch.len() >= batch_capacity {
                    let send_r1 =
                        std::mem::replace(&mut r1_batch, Vec::with_capacity(batch_capacity));
                    let send_r2 =
                        std::mem::replace(&mut r2_batch, Vec::with_capacity(batch_capacity));
                    if r1_tx.send(send_r1).is_err() || r2_tx.send(send_r2).is_err() {
                        stop_reading = true;
                        break;
                    }
                }
            }
            if !r1_batch.is_empty() {
                let _ = r1_tx.send(r1_batch);
                let _ = r2_tx.send(r2_batch);
            }
            if stop_reading {
                unsafe {
                    s1.shutdown();
                }
                break;
            }
            debug!("R1 finished reading");
        }
        r1_stage_timings.add_read(read_started.elapsed());
    });

    let handle_r2 = budget.spawn::<TRead, _, _>(0, move || {});

    return ((r1_rx, r2_rx), (handle_r1, handle_r2));
}

///
/// Route inputs from two readers into a stream of paired end
///
fn spawn_debarcode_router(
    r1_rx: Receiver<BudgetedFastqRecordBatch>,
    r2_rx: Receiver<BudgetedFastqRecordBatch>,
    budget: &GetrawBudget,
    batch_stats: Arc<GetRawBatchStats>,
) -> (Receiver<BudgetedReadPairBatch>, JoinHandle<()>) {
    let queue_capacity = chunk_queue_capacity(budget);
    let (rp_tx, rp_rx) = crossbeam::channel::bounded(queue_capacity);
    let rt_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting debarcode router");

        loop {
            match (r1_rx.recv(), r2_rx.recv()) {
                (Ok(r1_batch), Ok(r2_batch)) => {
                    if r1_batch.len() != r2_batch.len() {
                        warn!(
                            r1_batch_len = r1_batch.len(),
                            r2_batch_len = r2_batch.len(),
                            "R1/R2 batch lengths differ"
                        );
                    }

                    let batch = BudgetedReadPairBatch {
                        r1: r1_batch,
                        r2: r2_batch,
                    };

                    if !batch.is_empty() {
                        batch_stats.record_read_pair_batch(batch.len());
                        if rp_tx.send(batch).is_err() {
                            break;
                        }
                    }
                }
                (Err(_), Err(_)) => {
                    debug!("R1 and R2 channels closed, router finishing");
                    break;
                }
                (Ok(r1_batch), Err(_)) => {
                    drop(r1_batch);
                    warn!("R2 channel closed but R1 still has data");
                    break;
                }
                (Err(_), Ok(r2_batch)) => {
                    drop(r2_batch);
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
    readname: &str,
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
    rp_rx: Receiver<BudgetedReadPairBatch>,
    chemistry: GetRawChemistry,
    budget: &GetrawBudget,
    rayon_pool: Arc<rayon::ThreadPool>,
    inflight_limiter: Arc<InFlightLimiter>,
    batch_stats: Arc<GetRawBatchStats>,
    stage_timings: Arc<GetRawStageTimings>,
) -> (
    Receiver<BudgetedDebarcodedRecordBatch>,
    Vec<JoinHandle<()>>,
    GetRawChemistry,
) {
    let (ct_tx, ct_rx) = crossbeam::channel::bounded(chunk_queue_capacity(budget));
    let (result_tx, result_rx) = crossbeam::channel::unbounded();

    let atomic_total_counter = Arc::new(AtomicUsize::new(0));
    let atomic_success_counter = Arc::new(AtomicUsize::new(0));

    let dispatcher_chemistry = chemistry.clone();
    let dispatcher_result_tx = result_tx.clone();
    let dispatcher_handle = budget.spawn::<TDebarcode, _, _>(0, move || {
        rayon_pool.scope(|scope| {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, "Starting debarcode dispatcher");

            while let Ok(batch) = rp_rx.recv() {
                let permit = inflight_limiter.acquire();
                let mut task_chemistry = dispatcher_chemistry.clone();
                let task_result_tx = dispatcher_result_tx.clone();
                let task_atomic_total_counter = Arc::clone(&atomic_total_counter);
                let task_atomic_success_counter = Arc::clone(&atomic_success_counter);
                let task_batch_stats = Arc::clone(&batch_stats);
                let task_stage_timings = Arc::clone(&stage_timings);

                scope.spawn(move |_| {
                    let debarcode_started = Instant::now();
                    let _permit = permit;
                    let mut debarcoded_batch = Vec::with_capacity(batch.len());
                    for (r1, r2) in batch.r1.into_iter().zip(batch.r2.into_iter()) {
                        // TODO: optimisation: barcodes are fixed-size if represented in a non-string way (e.g as u64)
                        let (bc_index, rp) = task_chemistry.detect_barcode_and_trim(
                            r1.get_ref::<R0>(),
                            r1.get_ref::<Q0>(),
                            r2.get_ref::<R0>(),
                            r2.get_ref::<Q0>(),
                        );

                        let thread_total_counter =
                            task_atomic_total_counter.fetch_add(1, Ordering::Relaxed) + 1;

                        //Keep read if ok
                        if bc_index != u32::MAX {
                            let thread_success_counter =
                                task_atomic_success_counter.fetch_add(1, Ordering::Relaxed) + 1;

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
                                r1.value.take_backing(),
                            );
                            bascet_core::PushBacking::<fastq::Record, _>::push_backing(
                                &mut db_record,
                                r2.value.take_backing(),
                            );
                            let permit = ReadMemoryPermit::merge(r1.permit, r2.permit);
                            debarcoded_batch.push((bc_index, Budgeted::new(db_record, permit)));
                        }
                    }

                    if !debarcoded_batch.is_empty() {
                        task_batch_stats.record_debarcoded_batch(debarcoded_batch.len());
                        let _ = task_result_tx.send(debarcoded_batch);
                    }
                    task_stage_timings.add_debarcode(debarcode_started.elapsed());
                });
            }
        });
    });

    drop(result_tx);
    let forwarder_handle = budget.spawn::<TDebarcode, _, _>(1, move || {
        while let Ok(batch) = result_rx.recv() {
            if ct_tx.send(batch).is_err() {
                break;
            }
        }
    });

    return (ct_rx, vec![dispatcher_handle, forwarder_handle], chemistry);
}

///
/// Spawn collector, taking debarcoded/trimmed readers and collecting them for the next step
///
fn spawn_collector(
    db_rx: Receiver<BudgetedDebarcodedRecordBatch>,
    budget: &GetrawBudget,
    batch_stats: Arc<GetRawBatchStats>,
    stage_timings: Arc<GetRawStageTimings>,
) -> (
    Receiver<Vec<(u32, BudgetedDebarcodedRecord)>>,
    JoinHandle<()>,
) {
    let (ct_tx, ct_rx) = crossbeam::channel::bounded(chunk_queue_capacity(budget));
    let sizeof_each_sort_alloc = first_round_sort_chunk_size(budget);
    let mut countof_each_sort_alloc = 0;

    info!(
        first_round_sort_chunk_size = %sizeof_each_sort_alloc,
        sort_buffer = %budget.mem::<MSortBuffer>(),
        sort_threads = (*budget.threads::<TSort>()).get(),
        "Configured first-round sort chunking"
    );
    let ct_handle = budget.spawn::<Total, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting collector");

        let mut collection_buffer: Vec<(u32, BudgetedDebarcodedRecord)> =
            Vec::with_capacity(countof_each_sort_alloc);
        let mut sizeof_sort_alloc = ByteSize(0);
        let timeout = std::time::Duration::from_secs(4);
        let flush_collection = |collection_buffer: &mut Vec<(u32, BudgetedDebarcodedRecord)>,
                                sizeof_sort_alloc: &mut ByteSize,
                                countof_each_sort_alloc: &mut usize|
         -> bool {
            if collection_buffer.is_empty() {
                return true;
            }

            let sizeof_mean_sort_alloc =
                sizeof_sort_alloc.as_u64() / collection_buffer.len() as u64;
            let len = collection_buffer.len();
            debug!(
                records = len,
                bytes = %*sizeof_sort_alloc,
                "Flushing first-round sort chunk"
            );
            let send_buffer = std::mem::replace(collection_buffer, Vec::with_capacity(len.max(1)));
            batch_stats.record_collector_flush(len);
            if ct_tx.send(send_buffer).is_err() {
                return false;
            }

            *countof_each_sort_alloc =
                (sizeof_sort_alloc.as_u64() / sizeof_mean_sort_alloc) as usize;
            *collection_buffer = Vec::with_capacity(*countof_each_sort_alloc);
            *sizeof_sort_alloc = ByteSize(0);
            true
        };

        loop {
            match db_rx.recv_timeout(timeout) {
                Ok(mut debarcoded_batch) => {
                    let collect_started = Instant::now();
                    let batch_mem_size = ByteSize(
                        debarcoded_batch
                            .iter()
                            .map(|(_, db_record)| {
                                db_record.get_ref::<Id>().len()
                                    + db_record.get_ref::<R1>().len()
                                    + db_record.get_ref::<R2>().len()
                                    + db_record.get_ref::<Q1>().len()
                                    + db_record.get_ref::<Q2>().len()
                                    + db_record.get_ref::<Umi>().len()
                            })
                            .sum::<usize>() as u64,
                    );

                    if !collection_buffer.is_empty()
                        && batch_mem_size + sizeof_sort_alloc > sizeof_each_sort_alloc
                        && !flush_collection(
                            &mut collection_buffer,
                            &mut sizeof_sort_alloc,
                            &mut countof_each_sort_alloc,
                        )
                    {
                        break;
                    }

                    if batch_mem_size <= sizeof_each_sort_alloc {
                        sizeof_sort_alloc += batch_mem_size;
                        collection_buffer.append(&mut debarcoded_batch);
                    } else {
                        for (bc_index, db_record) in debarcoded_batch {
                            let cell_mem_size = ByteSize(
                                (db_record.get_ref::<Id>().len()
                                    + db_record.get_ref::<R1>().len()
                                    + db_record.get_ref::<R2>().len()
                                    + db_record.get_ref::<Q1>().len()
                                    + db_record.get_ref::<Q2>().len()
                                    + db_record.get_ref::<Umi>().len())
                                    as u64,
                            );

                            if cell_mem_size + sizeof_sort_alloc > sizeof_each_sort_alloc
                                && !flush_collection(
                                    &mut collection_buffer,
                                    &mut sizeof_sort_alloc,
                                    &mut countof_each_sort_alloc,
                                )
                            {
                                break;
                            }
                            collection_buffer.push((bc_index, db_record));
                            sizeof_sort_alloc += cell_mem_size;
                        }
                    }

                    if sizeof_sort_alloc >= sizeof_each_sort_alloc
                        && !flush_collection(
                            &mut collection_buffer,
                            &mut sizeof_sort_alloc,
                            &mut countof_each_sort_alloc,
                        )
                    {
                        break;
                    }
                    stage_timings.add_collect(collect_started.elapsed());
                }
                Err(RecvTimeoutError::Timeout) => {
                    let collect_started = Instant::now();
                    if !flush_collection(
                        &mut collection_buffer,
                        &mut sizeof_sort_alloc,
                        &mut countof_each_sort_alloc,
                    ) {
                        break;
                    }
                    stage_timings.add_collect(collect_started.elapsed());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }

        let collect_started = Instant::now();
        let _ = flush_collection(
            &mut collection_buffer,
            &mut sizeof_sort_alloc,
            &mut countof_each_sort_alloc,
        );
        stage_timings.add_collect(collect_started.elapsed());
    });

    return (ct_rx, ct_handle);
}

///
/// Spawn sorters. By sorting chunks of reads while they are already in memory, the first major sort pass will already be done in the first write to disk
///
fn spawn_sort_workers(
    ct_rx: Receiver<Vec<(u32, BudgetedDebarcodedRecord)>>,
    chemistry: GetRawChemistry,
    budget: &GetrawBudget,
    stage_timings: Arc<GetRawStageTimings>,
) -> (
    Receiver<Vec<(Vec<u8>, BudgetedDebarcodedRecord)>>,
    Vec<JoinHandle<()>>,
) {
    let countof_threads_sort = (*budget.threads::<TSort>()).get();
    let mut thread_handles = Vec::with_capacity(countof_threads_sort as usize);
    let (st_tx, st_rx) = crossbeam::channel::bounded(chunk_queue_capacity(budget));

    for thread_idx in 0..countof_threads_sort {
        let ct_rx = ct_rx.clone();
        let st_tx = st_tx.clone();
        let thread_chemistry = chemistry.clone();
        let thread_stage_timings = Arc::clone(&stage_timings);

        let thread_handle = budget.spawn::<TSort, _, _>(thread_idx, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, "Starting sort worker");

            while let Ok(vec_bc_indices_db_records) = ct_rx.recv() {
                let records = vec_bc_indices_db_records.len();
                let started = Instant::now();
                debug!(
                    sort_worker = thread_idx,
                    records, "Sorting first-round chunk"
                );
                // HACK: Convert barcode before sorting for correct ordering
                // NOTE: sort in descending order to be able to pop off the end (O(1) rather than O(n))
                // NOTE: to save memory conversion to owned cells is NOT done via map but rather by popping
                let mut records_with_bc: Vec<(Vec<u8>, BudgetedDebarcodedRecord)> =
                    IntoIterator::into_iter(vec_bc_indices_db_records)
                        .map(|(bc_index, db_record)| {
                            let id_as_bc = thread_chemistry.bcindexu32_to_bcu8(&bc_index).to_vec();
                            (id_as_bc, db_record)
                        })
                        .collect();

                glidesort::sort_by(&mut records_with_bc, |(bc_a, _), (bc_b, _)| {
                    Ord::cmp(bc_a, bc_b)
                });

                debug!(
                    sort_worker = thread_idx,
                    records,
                    elapsed = ?started.elapsed(),
                    "Sorted first-round chunk"
                );
                thread_stage_timings.add_sort(started.elapsed());
                let _ = st_tx.send(records_with_bc);
            }
        });
        thread_handles.push(thread_handle);
    }

    drop(st_tx);
    return (st_rx, thread_handles);
}

fn spawn_chunk_writers(
    st_rx: Receiver<Vec<(Vec<u8>, BudgetedDebarcodedRecord)>>,
    timestamp_temp_files: String,
    path_temp_dir: PathBuf,
    budget: &GetrawBudget,
    compression_level: Compression,
    library: &str,
    rayon_pool: Arc<rayon::ThreadPool>,
    stage_timings: Arc<GetRawStageTimings>,
) -> Vec<JoinHandle<ChunkWriterOutput>> {
    let atomic_counter = Arc::new(AtomicUsize::new(0));
    let shared_output = Arc::new(Mutex::new(ChunkWriterOutput {
        paths: Vec::new(),
        histogram_counts: HistogramCounts::new(),
        finish_handles: Vec::new(),
    }));
    let shared_raw_arena = Arc::new(ArenaPool::new(
        *budget.mem::<MCompressRawBuffer>(),
        codec::bbgz::MAX_SIZEOF_BLOCK,
    ));
    let shared_compression_arena = Arc::new(ArenaPool::new(
        *budget.mem::<MCompressBuffer>(),
        codec::bbgz::MAX_SIZEOF_BLOCK,
    ));
    let timestamp_temp_files = Arc::new(timestamp_temp_files);
    let library: Arc<str> = Arc::from(library);

    let thread_handle = budget.spawn::<TWrite, _, _>(0, move || {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown thread");
        debug!(thread = thread_name, "Starting Rayon chunk writer dispatcher");

        rayon_pool.scope(|scope| {
            while let Ok(sorted_record_list) = st_rx.recv() {
                let chunk_index = atomic_counter.fetch_add(1, Ordering::Relaxed);
                let chunk_records = sorted_record_list.len();
                let task_timestamp_temp_files = Arc::clone(&timestamp_temp_files);
                let task_path_temp_dir = path_temp_dir.clone();
                let task_library = Arc::clone(&library);
                let task_raw_arena = Arc::clone(&shared_raw_arena);
                let task_compression_arena = Arc::clone(&shared_compression_arena);
                let task_output = Arc::clone(&shared_output);
                let task_stage_timings = Arc::clone(&stage_timings);
                let task_rayon_pool = Arc::clone(&rayon_pool);

                scope.spawn(move |_| {
                    let chunk_started = Instant::now();
                    let temp_fname =
                        format!("{}_merge_0_{chunk_index}", *task_timestamp_temp_files);
                    let temp_pathbuf = task_path_temp_dir
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
                        Ok(file) => file,
                        Err(e) => {
                            error!(path = ?temp_pathbuf, error = %e, "Failed to create output file");
                            panic!("Failed to create output file");
                        }
                    };

                    debug!(
                        chunk = chunk_index,
                        records = chunk_records,
                        path = ?temp_pathbuf,
                        "Writing first-round chunk"
                    );
                    let mut bbgzwriter = BBGZWriter::builder()
                        .compression_level(compression_level)
                        .with_opt_raw_arena_pool(Arc::clone(&task_raw_arena))
                        .with_opt_compression_arena_pool(Arc::clone(&task_compression_arena))
                        .with_opt_rayon_pool(task_rayon_pool)
                        .with_writer(temp_output_file)
                        .build();

                    let mut records_writen = 0;
                    let mut last_id: SmallVec<[u8; 16]> = SmallVec::new();
                    let mut current_hist_id: Vec<u8> = Vec::new();
                    let mut current_hist_count = 0u64;
                    let mut chunk_histogram_counts = HistogramCounts::new();
                    let mut blockwriter_opt: Option<BBGZWriteBlock<'_>> = None;

                    let library_bytes = task_library.as_bytes();
                    let library_sep = if task_library.is_empty() { "" } else { "_" };
                    let library_sep_bytes = library_sep.as_bytes();

                    for (id, mut record) in sorted_record_list {
                        if *id != *last_id {
                            if let Some(ref mut blockwriter) = blockwriter_opt {
                                blockwriter.flush().unwrap();
                            }
                            if current_hist_count > 0 {
                                *chunk_histogram_counts
                                    .entry(std::mem::take(&mut current_hist_id))
                                    .or_insert(0) += current_hist_count;
                                current_hist_count = 0;
                            }
                            last_id = id.to_smallvec();

                            let mut prefixed_id = Vec::with_capacity(
                                library_bytes.len() + library_sep_bytes.len() + id.len(),
                            );
                            prefixed_id.extend_from_slice(library_bytes);
                            prefixed_id.extend_from_slice(library_sep_bytes);
                            prefixed_id.extend_from_slice(&id);
                            current_hist_id = prefixed_id.clone();

                            let mut bbgzheader = BBGZHeader::new();
                            unsafe {
                                bbgzheader.add_extra_unchecked(b"ID", prefixed_id);
                            }
                            blockwriter_opt = Some(bbgzwriter.begin(bbgzheader));
                        }

                        // SAFETY: safe because blockwriter is COW
                        *record.get_mut::<Id>() =
                            unsafe { std::mem::transmute(last_id.as_slice()) };
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
                            current_hist_count += 1;
                        }
                    }

                    if let Some(ref mut blockwriter) = blockwriter_opt {
                        blockwriter.flush().unwrap();
                    }
                    if current_hist_count > 0 {
                        *chunk_histogram_counts
                            .entry(std::mem::take(&mut current_hist_id))
                            .or_insert(0) += current_hist_count;
                    }
                    let finish_handle = bbgzwriter.finish_async();

                    let temp_input_path = match InputPath::try_from(&temp_pathbuf) {
                        Ok(path) => path,
                        Err(e) => panic!("{}", e),
                    };
                    debug!(path = ?temp_pathbuf, records_written = records_writen, "Wrote debarcoded cell chunk");
                    debug!(
                        chunk = chunk_index,
                        records = records_writen,
                        elapsed = ?chunk_started.elapsed(),
                        "Wrote first-round chunk"
                    );
                    task_stage_timings.add_write(chunk_started.elapsed());

                    let mut output = task_output.lock().unwrap();
                    output.paths.push(temp_input_path);
                    output.finish_handles.push(finish_handle);
                    merge_histogram_counts(&mut output.histogram_counts, chunk_histogram_counts);
                });
            }
        });

        let mut output = shared_output.lock().unwrap();
        ChunkWriterOutput {
            paths: std::mem::take(&mut output.paths),
            histogram_counts: std::mem::take(&mut output.histogram_counts),
            finish_handles: std::mem::take(&mut output.finish_handles),
        }
    });

    vec![thread_handle]
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
    let countof_threads_per_worker_base = countof_threads_total / countof_worker_threads;
    let countof_threads_remainder = countof_threads_total % countof_worker_threads;

    let sizeof_stream_each_buffer =
        ByteSize(budget.mem::<MStreamBuffer>().as_u64() / countof_worker_threads);
    let mut thread_handles = Vec::with_capacity(countof_worker_threads as usize);

    for (thread_idx, (output_path, hist_path)) in output_hist_pairs.into_iter().enumerate() {
        let thread_shared_arena = Arc::new(ArenaPool::new(sizeof_stream_each_buffer, stream_arena));
        let extra = if (thread_idx as u64) < countof_threads_remainder {
            1
        } else {
            0
        };
        let thread_countof_threads =
            BoundedU64::new_saturating(countof_threads_per_worker_base + extra);

        let worker_handle = budget.spawn::<Total, _, _>(thread_idx as u64, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread");
            debug!(thread = thread_name, processing_histogram_for = %output_path, "Starting histogram worker");

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

            let hist_final_path = hist_path.path().path().to_path_buf();
            let hist_tmp_path = atomic_temp_path(&hist_final_path);
            let hist_file = match File::create(&hist_tmp_path) {
                Ok(file) => file,
                Err(e) => {
                    error!(path = ?hist_tmp_path, error = %e, "Failed to create output file");
                    panic!("Failed to create output file");
                }
            };
            let mut bufwriter = BufWriter::new(hist_file);

            let mut current_id: SmallVec<[u8; 16]> = SmallVec::new();
            let mut current_count: u64 = 0;

            while let Ok(Some(record)) = query.next() {
                let id = record.get_ref::<Id>();
                if *id == current_id.as_slice() {
                    current_count += 1;
                } else {
                    if !current_id.is_empty() {
                        bufwriter.write_all(&current_id).unwrap();
                        bufwriter.write_all(b"\t").unwrap();
                        bufwriter.write_all(current_count.to_string().as_bytes()).unwrap();
                        bufwriter.write_all(b"\n").unwrap();
                    }
                    current_id = id.to_smallvec();
                    current_count = 1;
                }
            }
            if !current_id.is_empty() {
                bufwriter.write_all(&current_id).unwrap();
                bufwriter.write_all(b"\t").unwrap();
                bufwriter.write_all(current_count.to_string().as_bytes()).unwrap();
                bufwriter.write_all(b"\n").unwrap();
            }

            bufwriter.flush().unwrap();
            drop(bufwriter);
            publish_atomic_output(&hist_tmp_path, &hist_final_path).unwrap();
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
