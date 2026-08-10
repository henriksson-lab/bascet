use crate::bounded_parser;

use bascet_core::{
    attr::{meta::*, sequence::*},
    threading::spinpark_loop::{self, SPINPARK_COUNTOF_PARKS_BEFORE_WARN, SpinPark},
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, tirp};

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use crossbeam::channel::TryRecvError;
use gxhash::GxBuildHasher;
use hyperloglockless::HyperLogLog;
use nthash_rs::{NtHash, canonical};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        self, Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tracing::{debug, info, warn};

use crate::utils::{atomic_temp_path, publish_atomic_output};

const COUNTDISTINCT_MIN_STREAM_BUFFER: ByteSize = ByteSize::mib(64);
const COUNTDISTINCT_MIN_MEMORY_HEADROOM: ByteSize = ByteSize::mib(512);
const COUNTDISTINCT_STREAM_BUFFER_FRACTION: f64 = 0.50;

/// hyperloglockless asserts `(4..=18).contains(&precision)` when constructing a sketch, and
/// `precision_for_error` happily returns values outside that range (`--error 0.05` gives 23,
/// `--error 50` gives 3). Clamping here turns what would be a panic into a warning.
const HLL_MIN_PRECISION: u8 = 4;
const HLL_MAX_PRECISION: u8 = 18;

#[derive(Args)]
pub struct CountdistinctCMD {
    #[arg(
        short = 'i',
        long = "in",
        value_delimiter = ',',
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub paths_in: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output TSV file with one row per cell: cell_id, n_distinct, n_total"
    )]
    pub path_out: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to std::threads::available parallelism)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-read",
        help = "Number of reader threads",
        value_name = "1.. (50%)",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-work",
        help = "Number of worker threads",
        value_name = "1.. (50%)",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_work: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(1),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
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
        short = 'k',
        long = "kmer-size",
        help = "K-mer size for counting",
        default_value_t = 31,
        value_parser = clap::value_parser!(u16),
    )]
    pub kmer_size: u16,

    #[arg(
        short = 'e',
        long = "error",
        help = "Target relative error of the distinct count, in percent. Determines HyperLogLog precision.",
        default_value_t = 1.0,
        value_parser = clap::value_parser!(f64),
    )]
    pub error_percent: f64,
}

#[derive(Budget, Debug)]
struct CountdistinctBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.6) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[threads(TWork, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads.saturating_sub(1).max(1) as f64 * 0.4) as u64).unwrap())]
    numof_threads_work: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |_, _| bounded_integer::BoundedU64::new(1).unwrap())]
    numof_threads_write: BoundedU64<1, 1>,

    #[mem(MBuffer, |_, total_mem| default_countdistinct_stream_buffer(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

fn default_countdistinct_stream_buffer(total_mem: u64) -> ByteSize {
    let fractional_cap = (total_mem as f64 * COUNTDISTINCT_STREAM_BUFFER_FRACTION) as u64;
    let fraction_headroom = total_mem.saturating_sub(fractional_cap);
    let headroom = COUNTDISTINCT_MIN_MEMORY_HEADROOM
        .as_u64()
        .max(fraction_headroom);
    let available = total_mem.saturating_sub(headroom);
    let floor = COUNTDISTINCT_MIN_STREAM_BUFFER.as_u64().min(total_mem);

    ByteSize(available.max(floor).min(fractional_cap).min(total_mem))
}

/// `hyperloglockless::precision_for_error` takes the error as a fraction and panics outside
/// `0.0 < error < 1.0`, so the percent taken on the command line is converted and validated here.
fn precision_from_error_percent(error_percent: f64) -> Result<u8> {
    if !(error_percent > 0.0 && error_percent < 100.0) {
        anyhow::bail!(
            "--error must be a percentage in (0, 100), got {}",
            error_percent
        );
    }

    let precision = hyperloglockless::precision_for_error(error_percent / 100.0);
    let clamped = precision.clamp(HLL_MIN_PRECISION, HLL_MAX_PRECISION);
    if clamped != precision {
        warn!(
            requested_error_percent = error_percent,
            requested_precision = precision,
            precision = clamped,
            actual_error_percent = hyperloglockless::error_for_precision(clamped) * 100.0,
            "Requested error is outside the range HyperLogLog supports; clamping precision"
        );
    }

    Ok(clamped)
}

/// Add every canonical k-mer of `sequence` to `hll`, returning how many were added.
///
/// `insert` (rather than `insert_hash`) is deliberate: `insert_hash` feeds the raw word straight
/// into the register index and rank, and ntHash rolls each k-mer from the previous one, so its
/// output is not independent enough for that. `insert` runs it through the crate's hasher first.
///
/// Returns `Err(())` if the sequence is shorter than k, matching `CountSketch::add_sequence`.
fn add_sequence(hll: &mut HyperLogLog<GxBuildHasher>, sequence: &[u8], k: u16) -> Result<u64, ()> {
    let mut hasher = match NtHash::new(sequence, k, 1, 0) {
        Ok(hasher) => hasher,
        Err(_) => return Err(()),
    };

    let mut countof_kmers = 0;
    while hasher.roll() {
        hll.insert(&canonical(hasher.forward_hash(), hasher.reverse_hash()));
        countof_kmers += 1;
    }

    Ok(countof_kmers)
}

impl CountdistinctCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let precision = precision_from_error_percent(self.error_percent)?;

        let budget = CountdistinctBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to determine available parallelism, using 2 threads");
                        2
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to convert parallelism to valid thread count, using 2 threads");
                        2.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_numof_threads_read(self.numof_threads_read)
            .maybe_numof_threads_work(self.numof_threads_work)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.log();

        info!(
            input_files = self.paths_in.len(),
            output_path = ?self.path_out,
            error_percent = self.error_percent,
            hll_precision = precision,
            kmer_size = self.kmer_size,
            "Starting Countdistinct"
        );

        ////////////////////////////////////////////////////////////////////
        // Create threads for writing output. Note that
        // cells can be written in any order for this file format
        let path_out = self.path_out.clone();
        let path_tmp = atomic_temp_path(&path_out);
        let output_file = match File::create(&path_tmp) {
            Ok(output) => output,
            Err(e) => {
                warn!(path = ?path_tmp, error = %e, "Failed to create output countdistinct file");
                anyhow::bail!("Failed to create output countdistinct file");
            }
        };

        let (write_tx, write_rx) = crossbeam::channel::unbounded::<CountdistinctRow>();
        let thread_writer = budget.spawn::<TWrite, _, _>(0, move || {
            write_countdistinct_tsv(output_file, write_rx)
                .expect("Failed to write countdistinct TSV file");
        });

        let k = self.kmer_size;
        let numof_threads_work = (*budget.threads::<TWork>()).get();

        //For each input file
        for (input_idx, input) in self.paths_in.iter().enumerate() {
            // Create threads for streaming from the input file
            let decoder = codec::BBGZDecoder::builder()
                .with_path(input.path().path())
                .countof_threads(budget.numof_threads_read)
                .build();
            let parser = parse::Tirp::builder().build();

            let mut stream = Stream::builder()
                .with_decoder(decoder)
                .with_parser(parser)
                .sizeof_decode_arena(self.sizeof_stream_arena)
                .sizeof_decode_buffer(budget.sizeof_stream_buffer)
                .build();

            let mut query = stream.query::<tirp::Record>();

            // One sketch per worker, merged at each cell boundary. A shared AtomicHyperLogLog
            // would put an unconditional fetch_max per k-mer on cache lines every worker wants.
            // The hasher is cloned rather than rebuilt: sketches must agree on a seed to be
            // unioned, and GxBuildHasher::default() picks a new one each time.
            let hasher = GxBuildHasher::default();
            let mut worker_sketches: Vec<HyperLogLog<GxBuildHasher>> = (0..numof_threads_work)
                .map(|_| HyperLogLog::with_hasher(precision, hasher.clone()))
                .collect();

            let arc_flag_synchronize = Arc::new(AtomicBool::new(false));
            let arc_barrier = Arc::new(sync::Barrier::new((numof_threads_work + 1) as usize));
            let arc_countof_reads_skipped = Arc::new(AtomicU64::new(0));
            // Workers keep their k-mer tally thread-local and fold it in here at the barrier,
            // so the hot loop stays free of atomic arithmetic.
            let arc_countof_kmers = Arc::new(AtomicU64::new(0));

            let mut vec_worker_handles = Vec::with_capacity(numof_threads_work as usize);
            let (work_tx, work_rx) = crossbeam::channel::unbounded::<CountdistinctRecord>();

            // Create threads for processing the reads
            for thread_idx in 0..numof_threads_work {
                let thread_work_rx = work_rx.clone();
                let mut sketch_ptr = unsafe {
                    SendPtr::new_unchecked(
                        &mut worker_sketches[thread_idx as usize]
                            as *mut HyperLogLog<GxBuildHasher>,
                    )
                };
                let thread_flag_synchronize = Arc::clone(&arc_flag_synchronize);
                let thread_barrier = Arc::clone(&arc_barrier);
                let thread_countof_reads_skipped = Arc::clone(&arc_countof_reads_skipped);
                let thread_countof_kmers = Arc::clone(&arc_countof_kmers);

                vec_worker_handles.push(budget.spawn::<TWork, _, _>(
                    thread_idx as u64,
                    move || {
                        let thread = std::thread::current();
                        let thread_name = thread.name().unwrap_or("unknown thread");
                        debug!(thread = thread_name, "Starting worker");

                        let mut thread_spinpark_counter = 0;
                        let mut local_countof_kmers = 0u64;
                        loop {
                            let record = match thread_work_rx.try_recv() {
                                Ok(record) => record,
                                Err(TryRecvError::Empty) => {
                                    if thread_flag_synchronize.load(Ordering::Relaxed) == true {
                                        // publish this cell's k-mer tally before the coordinator reads it
                                        thread_countof_kmers
                                            .fetch_add(local_countof_kmers, Ordering::Relaxed);
                                        local_countof_kmers = 0;
                                        // wait for snapshot to be created
                                        thread_barrier.wait();
                                        // wait for reset to be finished
                                        thread_barrier.wait();
                                    }
                                    match spinpark_loop::spinpark_loop::<
                                        100,
                                        SPINPARK_COUNTOF_PARKS_BEFORE_WARN,
                                    >(
                                        &mut thread_spinpark_counter
                                    ) {
                                        SpinPark::Warn => warn!(
                                            source = "Countdistinct::worker",
                                            "channel empty, producer slow"
                                        ),
                                        _ => {}
                                    }
                                    continue;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    break;
                                }
                            };
                            thread_spinpark_counter = 0;

                            // SAFETY: Each worker has exclusive access to its own sketch via raw
                            // pointer. Barriers ensure no concurrent access during sync.
                            let hll = unsafe { sketch_ptr.as_mut() };
                            match add_sequence(hll, record.get_ref::<R1>(), k) {
                                Ok(n) => local_countof_kmers += n,
                                Err(()) => {
                                    thread_countof_reads_skipped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            match add_sequence(hll, record.get_ref::<R2>(), k) {
                                Ok(n) => local_countof_kmers += n,
                                Err(()) => {
                                    thread_countof_reads_skipped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    },
                ));
            }

            let mut record_id_last: Vec<u8> = Vec::new();
            let mut cells_processed = 0u64;
            loop {
                let record = match query.next_into::<CountdistinctRecord>() {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        arc_flag_synchronize.store(true, Ordering::Relaxed);
                        arc_barrier.wait();

                        // SAFETY: Workers are blocked at barrier, coordinator has exclusive access
                        let row = snapshot_cell(
                            &mut worker_sketches,
                            precision,
                            &hasher,
                            &arc_countof_kmers,
                            std::mem::take(&mut record_id_last),
                        );
                        let _ = write_tx.send(row);

                        arc_flag_synchronize.store(false, Ordering::Relaxed);
                        arc_barrier.wait();
                        break;
                    }
                    Err(e) => {
                        panic!("{:?}", e);
                    }
                };

                let record_id = *record.get_ref::<Id>();
                if record_id != &record_id_last {
                    arc_flag_synchronize.store(true, Ordering::Relaxed);
                    arc_barrier.wait();

                    // SAFETY: Workers are blocked at barrier, coordinator has exclusive access
                    let row = snapshot_cell(
                        &mut worker_sketches,
                        precision,
                        &hasher,
                        &arc_countof_kmers,
                        std::mem::take(&mut record_id_last),
                    );
                    let _ = write_tx.send(row);

                    record_id_last = record_id.to_vec();
                    cells_processed += 1;

                    if cells_processed % 100 == 0 {
                        info!(cells_processed = cells_processed, current_cell = ?String::from_utf8_lossy(&record_id_last), "Progress");
                    }

                    arc_flag_synchronize.store(false, Ordering::Relaxed);
                    arc_barrier.wait();
                }

                let _ = work_tx.send(record);
            }

            //Wait for all data to be have been sent to the workers
            drop(work_tx);

            //Wait for the workers to have sent all data
            for handle in vec_worker_handles {
                handle.join().unwrap();
            }

            let reads_skipped = arc_countof_reads_skipped.load(Ordering::Relaxed);
            info!(
                input_file = input_idx,
                cells_processed = cells_processed,
                reads_skipped = reads_skipped,
                "File complete"
            );
        }

        //Send signal to stop countdistinct writers
        drop(write_tx);
        //Wait for writers to finish
        thread_writer.join().unwrap();
        publish_atomic_output(path_tmp, path_out)?;

        Ok(())
    }
}

/// Read the finished cell out of the shared sketch and arm both for the next one.
///
/// Only safe to call while every worker is parked on the barrier.
fn snapshot_cell(
    sketches: &mut [HyperLogLog<GxBuildHasher>],
    precision: u8,
    hasher: &GxBuildHasher,
    countof_kmers: &AtomicU64,
    id: Vec<u8>,
) -> CountdistinctRow {
    // Registers hold a max, so unioning the workers gives exactly the sketch a single sketch
    // would have held. Merging costs no accuracy.
    let (merged, rest) = sketches
        .split_first_mut()
        .expect("at least one worker sketch");
    for sketch in rest.iter() {
        merged
            .union(sketch)
            .expect("worker sketches are built with the same precision");
    }

    let row = CountdistinctRow {
        id: String::from_utf8_lossy(&id).into_owned(),
        n_distinct: merged.count() as u64,
        n_total: countof_kmers.swap(0, Ordering::Relaxed),
    };

    // hyperloglockless has no clear(), so the sketches are replaced wholesale.
    for sketch in sketches.iter_mut() {
        *sketch = HyperLogLog::with_hasher(precision, hasher.clone());
    }

    row
}

#[derive(Composite, Default)]
#[bascet(attrs = (Id, R1, R2), backing = ArenaBacking, marker = AsRecord)]
pub struct CountdistinctRecord {
    id: &'static [u8],
    r1: &'static [u8],
    r2: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

struct CountdistinctRow {
    id: String,
    n_distinct: u64,
    n_total: u64,
}

fn write_countdistinct_tsv(
    output_file: File,
    write_rx: crossbeam::channel::Receiver<CountdistinctRow>,
) -> Result<()> {
    let mut writer = BufWriter::new(output_file);
    writeln!(writer, "cell_id\tn_distinct\tn_total")?;

    while let Ok(row) = write_rx.recv() {
        // The first cell boundary flushes an empty sketch under an empty id; skip it.
        if row.id.is_empty() {
            continue;
        }
        writeln!(writer, "{}\t{}\t{}", row.id, row.n_distinct, row.n_total)?;
    }

    writer.flush()?;
    Ok(())
}
