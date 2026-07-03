use crate::{bounded_parser, countsketch::CountSketch};

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
use polars_arrow::array::{Array, Int64Array, UInt64Array, Utf8Array};
use polars_arrow::datatypes::{ArrowDataType, ArrowSchema, ArrowSchemaRef, Field};
use polars_arrow::io::ipc::write::{FileWriter as ArrowFileWriter, WriteOptions};
use polars_arrow::record_batch::RecordBatch;
use std::{
    fs::File,
    io::BufWriter,
    path::PathBuf,
    sync::{
        self, Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tracing::{debug, info, warn};

use crate::utils::{atomic_temp_path, publish_atomic_output};

#[derive(Args)]
pub struct CountsketchCMD {
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
        help = "Output Feather file for the wide countsketch matrix"
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
            countsketch_size = self.countsketch_size,
            kmer_size = self.kmer_size,
            "Starting Countsketch"
        );

        ////////////////////////////////////////////////////////////////////
        // Create threads for writing output. Note that
        // cells can be written in any order for this file format
        let path_out = self.path_out.clone();
        let path_tmp = atomic_temp_path(&path_out);
        let output_file = match File::create(&path_tmp) {
            Ok(output) => output,
            Err(e) => {
                warn!(path = ?path_tmp, error = %e, "Failed to create output countsketch file");
                anyhow::bail!("Failed to create output countsketch file");
            }
        };

        let (write_tx, write_rx) = crossbeam::channel::unbounded::<CountsketchRow>();
        let countsketch_size = self.countsketch_size;
        let thread_writer = budget.spawn::<TWrite, _, _>(0, move || {
            write_countsketch_feather(output_file, write_rx, countsketch_size)
                .expect("Failed to write countsketch Feather file");
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

            let mut worker_sketches: Vec<CountSketch> = (0..numof_threads_work)
                .map(|_| CountSketch::new(self.countsketch_size))
                .collect();

            let arc_flag_synchronize = Arc::new(AtomicBool::new(false));
            let arc_barrier = Arc::new(sync::Barrier::new((numof_threads_work + 1) as usize));
            let arc_countof_reads_skipped = Arc::new(AtomicU64::new(0));

            let mut vec_worker_handles = Vec::with_capacity(numof_threads_work as usize);
            let (work_tx, work_rx) = crossbeam::channel::unbounded::<CountsketchRecord>();

            // Create threads for processing the reads
            for thread_idx in 0..numof_threads_work {
                let thread_work_rx = work_rx.clone();
                let mut sketch_ptr = unsafe {
                    SendPtr::new_unchecked(
                        &mut worker_sketches[thread_idx as usize] as *mut CountSketch,
                    )
                };
                let thread_flag_synchronize = Arc::clone(&arc_flag_synchronize);
                let thread_barrier = Arc::clone(&arc_barrier);
                let thread_countof_reads_skipped = Arc::clone(&arc_countof_reads_skipped);

                vec_worker_handles.push(budget.spawn::<TWork, _, _>(
                    thread_idx as u64,
                    move || {
                        let thread = std::thread::current();
                        let thread_name = thread.name().unwrap_or("unknown thread");
                        debug!(thread = thread_name, "Starting worker");

                        let mut thread_spinpark_counter = 0;
                        loop {
                            let record = match thread_work_rx.try_recv() {
                                Ok(record) => record,
                                Err(TryRecvError::Empty) => {
                                    if thread_flag_synchronize.load(Ordering::Relaxed) == true {
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
                                            source = "Countsketch::worker",
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

                            // SAFETY: Each worker has exclusive access to its own sketch via raw pointer.
                            // Barriers ensure no concurrent access during sync.
                            unsafe {
                                let sketch = sketch_ptr.as_mut();
                                if sketch.add_sequence(record.get_ref::<R1>(), k).is_err() {
                                    thread_countof_reads_skipped.fetch_add(1, Ordering::Relaxed);
                                }
                                if sketch.add_sequence(record.get_ref::<R2>(), k).is_err() {
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

        //Send signal to stop countsketch writers
        drop(write_tx);
        //Wait for writers to finish
        thread_writer.join().unwrap();
        publish_atomic_output(path_tmp, path_out)?;

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

const FEATHER_ROWS_PER_BATCH: usize = 256;

fn write_countsketch_feather(
    output_file: File,
    write_rx: crossbeam::channel::Receiver<CountsketchRow>,
    countsketch_size: usize,
) -> Result<()> {
    let schema = countsketch_schema(countsketch_size);
    let bufwriter = BufWriter::new(output_file);
    let mut writer = ArrowFileWriter::new(
        bufwriter,
        Arc::clone(&schema),
        None,
        WriteOptions { compression: None },
    );
    writer.start()?;
    let mut buffer = CountsketchFeatherBatch::new(Arc::clone(&schema), countsketch_size);

    while let Ok(countsketch_row) = write_rx.recv() {
        let id = countsketch_row.get_ref::<Id>();
        if id.is_empty() {
            continue;
        }
        buffer.push(countsketch_row)?;
        if buffer.len() >= FEATHER_ROWS_PER_BATCH {
            buffer.flush(&mut writer)?;
        }
    }

    buffer.flush(&mut writer)?;
    writer.finish()?;
    Ok(())
}

fn countsketch_schema(countsketch_size: usize) -> ArrowSchemaRef {
    let mut fields = Vec::with_capacity(countsketch_size + 2);
    fields.push(Field::new("cell_id".into(), ArrowDataType::Utf8, false));
    fields.push(Field::new("depth".into(), ArrowDataType::UInt64, false));
    for i in 0..countsketch_size {
        fields.push(Field::new(
            format!("cs_{i}").into(),
            ArrowDataType::Int64,
            false,
        ));
    }
    Arc::new(ArrowSchema::from_iter(fields))
}

struct CountsketchFeatherBatch {
    schema: ArrowSchemaRef,
    ids: Vec<String>,
    depths: Vec<u64>,
    sketch_columns: Vec<Vec<i64>>,
}

impl CountsketchFeatherBatch {
    fn new(schema: ArrowSchemaRef, countsketch_size: usize) -> Self {
        Self {
            schema,
            ids: Vec::with_capacity(FEATHER_ROWS_PER_BATCH),
            depths: Vec::with_capacity(FEATHER_ROWS_PER_BATCH),
            sketch_columns: (0..countsketch_size)
                .map(|_| Vec::with_capacity(FEATHER_ROWS_PER_BATCH))
                .collect(),
        }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn push(&mut self, row: CountsketchRow) -> Result<()> {
        if row.countsketch.len() != self.sketch_columns.len() {
            anyhow::bail!(
                "countsketch row has {} dimensions, expected {}",
                row.countsketch.len(),
                self.sketch_columns.len()
            );
        }

        self.ids.push(row.id);
        self.depths.push(row.depth);
        for (column, value) in self.sketch_columns.iter_mut().zip(row.countsketch) {
            column.push(value);
        }
        Ok(())
    }

    fn flush<W: std::io::Write>(&mut self, writer: &mut ArrowFileWriter<W>) -> Result<()> {
        if self.ids.is_empty() {
            return Ok(());
        }

        let height = self.ids.len();
        let mut arrays: Vec<Box<dyn Array>> = Vec::with_capacity(self.sketch_columns.len() + 2);
        arrays.push(Box::new(Utf8Array::<i32>::from_slice(std::mem::take(
            &mut self.ids,
        ))));
        arrays.push(Box::new(UInt64Array::from_vec(std::mem::take(
            &mut self.depths,
        ))));

        for column in &mut self.sketch_columns {
            let values = std::mem::take(column);
            arrays.push(Box::new(Int64Array::from_vec(values)));
        }

        let batch = RecordBatch::try_new(height, Arc::clone(&self.schema), arrays)?;
        writer.write(&batch, None)?;

        self.ids = Vec::with_capacity(FEATHER_ROWS_PER_BATCH);
        self.depths = Vec::with_capacity(FEATHER_ROWS_PER_BATCH);
        for column in &mut self.sketch_columns {
            *column = Vec::with_capacity(FEATHER_ROWS_PER_BATCH);
        }

        Ok(())
    }
}

#[derive(Composite, Default)]
#[bascet(attrs = (Id, Depth, Countsketch), backing = OwnedBacking, marker = AsRecord)]
pub struct CountsketchRow {
    id: String,
    depth: u64,
    countsketch: Vec<i64>,

    owned_backing: (),
}
