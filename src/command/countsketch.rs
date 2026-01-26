use crate::{
    bounded_parser, common::U8_CHAR_NEWLINE, countsketch::CountSketch, log_critical, log_info,
    log_warning,
};

use bascet_core::{
    attr::{meta::*, sequence::*},
    spinpark_loop::SPINPARK_PARKS_BEFORE_WARN,
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, tirp, BBGZHeader, BBGZWriter};

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use crossbeam::channel::TryRecvError;
use serde::Serialize;
use serde_with::{formats::CommaSeparator, serde_as, StringWithSeparator};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        self,
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Args)]
pub struct CountsketchCMD {
    #[arg(
        short = 'i',
        long = "in",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub paths_in: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output directory for countsketch files"
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
        value_name = "100%",
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
        short = 's',
        long = "sketch-size",
        help = "Size of the count sketch. Must be a power of two.",
        default_value_t = 4096,
        value_parser = clap::value_parser!(usize),
    )]
    pub countsketch_size: usize,
}

#[derive(Budget, Debug)]
struct CountsketchBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.5) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[threads(TWork, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads.saturating_sub(1).max(1) as f64 * 0.5) as u64).unwrap())]
    numof_threads_work: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |_, _| bounded_integer::BoundedU64::new(1).unwrap())]
    numof_threads_write: BoundedU64<1, 1>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl CountsketchCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let budget = CountsketchBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        log_warning!("Failed to determine available parallelism, using 2 threads"; "error" => %e);
                        2
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        log_warning!("Failed to convert parallelism to valid thread count, using 2 threads"; "error" => %e);
                        2.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_numof_threads_read(self.numof_threads_read)
            .maybe_numof_threads_work(self.numof_threads_work)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();

        log_info!(
            "Starting Countsketch";
            "using" => %budget,
            "input files" => self.paths_in.len(),
            "output path" => ?self.path_out,
            "countsketch size" => self.countsketch_size,
            "kmer size" => self.kmer_size,
        );

        let k = self.kmer_size;
        let numof_threads_work = (*budget.threads::<TWork>()).get();

        for (input_idx, input) in self.paths_in.iter().enumerate() {
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

            let mut worker_sketches: Vec<CountSketch> = (0..numof_threads_work)
                .map(|_| CountSketch::new(self.countsketch_size))
                .collect();

            let arc_flag_synchronize = Arc::new(AtomicBool::new(false));
            let arc_barrier = Arc::new(sync::Barrier::new((numof_threads_work + 1) as usize));

            let mut vec_worker_handles = Vec::with_capacity(numof_threads_work as usize);
            let (work_tx, work_rx) = crossbeam::channel::unbounded::<CountsketchRecord>();

            for thread_idx in 0..numof_threads_work {
                let thread_work_rx = work_rx.clone();
                let mut sketch_ptr = unsafe {
                    SendPtr::new_unchecked(
                        &mut worker_sketches[thread_idx as usize] as *mut CountSketch,
                    )
                };
                let thread_flag_synchronize = Arc::clone(&arc_flag_synchronize);
                let thread_barrier = Arc::clone(&arc_barrier);

                vec_worker_handles.push(budget.spawn::<TWork, _, _>(thread_idx as u64, move || {
                    let thread = std::thread::current();
                    let thread_name = thread.name().unwrap_or("unknown thread"); 
                    log_info!("Starting worker"; "thread" => thread_name);

                    let mut thread_spinpark_counter = 0;
                    loop {
                        let record = match thread_work_rx.try_recv() {
                            Ok(record) => {
                                thread_spinpark_counter = 0;
                                record
                            },
                            Err(TryRecvError::Empty) => {
                                if thread_flag_synchronize.load(Ordering::Relaxed) == true {
                                    // wait for snapshot to be created
                                    thread_barrier.wait();
                                    // wait for reset to be finished
                                    thread_barrier.wait();
                                }
                                spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                                    &mut thread_spinpark_counter,
                                    "Countsketch (worker): channel try_recv (channel empty, producer slow)"
                                );
                                continue;
                            },
                            Err(TryRecvError::Disconnected) => {
                                break;
                            }
                        };
                        // SAFETY: Each worker has exclusive access to its own sketch via raw pointer.
                        // Barriers ensure no concurrent access during sync.
                        unsafe {
                            let sketch = sketch_ptr.as_mut();
                            let _ = sketch.add_sequence(record.get_ref::<R1>(), k);
                            let _ = sketch.add_sequence(record.get_ref::<R2>(), k);
                        }
                    }
                }));
            }

            let output_path = self
                .path_out
                .join(format!("countsketch.{}.csv", input_idx + 1));

            let output_file = match File::create(&output_path) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to create output file, skipping"; "path" => ?output_path, "error" => %e);
                    continue;
                }
            };

            let (write_tx, write_rx) = crossbeam::channel::unbounded::<CountsketchRow>();
            budget.spawn::<TWrite, _, _>(0, move || {
                let bufwriter = BufWriter::new(output_file);
                let mut csvwriter = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(bufwriter);

                while let Ok(countsketch_row) = write_rx.recv() {
                    let id = countsketch_row.get_ref::<Id>();
                    if id.is_empty() {
                        continue;
                    }
                    csvwriter.serialize(&countsketch_row).unwrap();
                }

                csvwriter.flush();
            });

            let mut record_id_last: Vec<u8> = Vec::new();
            let mut cells_processed = 0u64;
            loop {
                let record = match query.next_into::<CountsketchRecord>() {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        arc_flag_synchronize.store(true, Ordering::Relaxed);
                        arc_barrier.wait();

                        // SAFETY: Workers are blocked at barrier, coordinator has exclusive access
                        let (snapshot, n) = {
                            let mut merged_sketch = vec![0i64; self.countsketch_size];
                            let mut total = 0i64;

                            for sketch in &mut worker_sketches {
                                for (i, &val) in sketch.sketch.iter().enumerate() {
                                    merged_sketch[i] += val;
                                }

                                total += sketch.total();
                                sketch.reset();
                            }

                            (merged_sketch, total as u64)
                        };

                        let countsketch_row = CountsketchRow {
                            id: String::from_utf8(record_id_last).unwrap(),
                            depth: n,
                            countsketch: snapshot,

                            owned_backing: (),
                        };

                        let _ = write_tx.send(countsketch_row);

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
                    let (snapshot, n) = {
                        let mut merged_sketch = vec![0i64; self.countsketch_size];
                        let mut total = 0i64;

                        for sketch in &mut worker_sketches {
                            for (i, &val) in sketch.sketch.iter().enumerate() {
                                merged_sketch[i] += val;
                            }

                            total += sketch.total();
                            sketch.reset();
                        }

                        (merged_sketch, total as u64)
                    };

                    let countsketch_row = CountsketchRow {
                        id: String::from_utf8(record_id_last).unwrap(),
                        depth: n,
                        countsketch: snapshot,

                        owned_backing: (),
                    };

                    let _ = write_tx.send(countsketch_row);

                    record_id_last = record_id.to_vec();
                    cells_processed += 1;

                    if cells_processed % 100 == 0 {
                        log_info!("Progress"; "cells_processed" => cells_processed, "current_cell" => ?String::from_utf8_lossy(&record_id_last));
                    }

                    arc_flag_synchronize.store(false, Ordering::Relaxed);
                    arc_barrier.wait();
                }

                let _ = work_tx.send(record);
            }
            drop(work_tx);
            for handle in vec_worker_handles {
                handle.join().unwrap();
            }
            drop(write_tx);

            log_info!("File complete"; "input_file" => input_idx, "total_cells_processed" => cells_processed);
        }

        Ok(())
    }
}

#[derive(Composite, Default)]
#[bascet(attrs = (Id, R1, R2), backing = ArenaBacking, marker = AsRecord)]
pub struct CountsketchRecord {
    id: &'static [u8],
    r1: &'static [u8],
    r2: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default, Serialize)]
#[bascet(attrs = (Id, Depth, Countsketch), backing = OwnedBacking, marker = AsRecord)]
pub struct CountsketchRow {
    id: String,
    depth: u64,
    countsketch: Vec<i64>,

    #[serde(skip)]
    owned_backing: (),
}
