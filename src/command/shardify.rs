use anyhow::Result;
use bascet_core::{spinpark_loop::{SPINPARK_PARKS_BEFORE_WARN, self}, *};
use bascet_derive::Budget;
use bascet_io::{decode, parse, tirp};
use bgzip::{write::BGZFMultiThreadWriter, Compression};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::Args;
use clio::{InputPath, OutputPath};
use crossbeam::channel::{self, Sender};
use itertools::izip;
use std::{
    fs::File, io::{BufRead, BufReader, BufWriter, Write}, path::{Path, PathBuf}, process::id, sync::{Arc, RwLock}, thread::JoinHandle
};

use crate::{
    bounded_parser, common::{self, spin_or_park}, io::traits::*, log_critical, log_info, log_warning, support_which_stream, support_which_writer, threading::{self, PeekableReceiver}
};

use std::thread;

pub const DEFAULT_THREADS_READ: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 2;
pub const DEFAULT_THREADS_TOTAL: usize = 12;

support_which_stream! {
    ShardifyInput => ShardifyStream<T: BascetCell>
    for formats [tirp_bgzf]
}
support_which_writer! {
    ShardifyOutput => ShardifyWriter<W: std::io::Write>
    for formats [tirp_bgzf]
}
/// Commandline option: Take parsed reads and organize them as shards
#[derive(Args)]
pub struct ShardifyCMD {
    #[arg(
        short = 'i',
        long = "in",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of input files (comma-separated)"
    )]
    pub paths_in: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of output files (comma-separated)")
    ]
    pub paths_out: Vec<OutputPath>,

    #[arg(
        long = "include",
        help = "File with list of cells to include (one per line)")
    ]
    pub path_include: InputPath,

    #[arg(                                                                                              
        short = '@',                                                                                    
        long = "threads",                                                                               
        help = "Total threads to use",                   
        value_name = "3..",        
        value_parser = bounded_parser!(BoundedU64<3, { u64::MAX }>),                                        
    )]                                                                                                  
    total_threads: Option<BoundedU64<3, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-read",
        help = "Number of reader threads (default: number of input files)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<2, { u64::MAX }>>,

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
        help = "Total stream buffer size. Will be divided evenly across streams.",
        value_name = "100%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arema buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,
}

#[derive(Budget, Debug)]
struct ShardifyBudget {
    #[threads(Total)]
    threads: BoundedU64<3, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead)]
    numof_threads_read: BoundedU64<2, { u64::MAX }>,

    #[threads(TWrite)]
    numof_threads_write: BoundedU64<1, { u64::MAX }>,

    #[mem(MBuffer, 100.0)]
    sizeof_stream_buffer: ByteSize,
}


impl ShardifyCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let budget = ShardifyBudget::builder()
            .threads(self.total_threads.unwrap_or((self.paths_in.len() + 1).try_into().unwrap()))
            .memory(self.total_mem)
            .numof_threads_read(self.numof_threads_read.unwrap_or(self.paths_in.len().try_into().unwrap()))
            .numof_threads_write(self.numof_threads_write.unwrap_or(1.try_into().unwrap()))
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();
        budget.validate();

        let arc_filter = read_filter(&self.path_include.path().path());
        let numof_streams = self.paths_in.len() as u64;
        let numof_writers = self.paths_out.len() as u64;

        let each_stream_numof_threads = budget.threads::<TRead>().get() / numof_streams;
        let each_stream_numof_threads: BoundedU64<1, { u64::MAX }> = BoundedU64::new(each_stream_numof_threads)
            .unwrap_or_else(|| {
                let saturated: BoundedU64<1, _> = BoundedU64::new_saturating(each_stream_numof_threads);
                log_warning!(
                    "Thread allocation per stream below minimum";
                    "determined" => each_stream_numof_threads,
                    "saturating to" => %saturated
                );
                saturated
            });
        
        let each_stream_sizeof_buffer = ByteSize(budget.mem::<MBuffer>().as_u64() / numof_streams);
        
        log_info!(
            "Starting Shardify";
            "using" => %budget,
            "streams" => numof_streams,
            "writers" => numof_writers,
            "threads per stream" => %each_stream_numof_threads,
            "memory per stream" => %each_stream_sizeof_buffer,
            "cells in filter" => arc_filter.len()
        );

        let (vec_coordinator_tx, mut vec_coordinator_rx): (
            Vec<Sender<ShardifyPartialCell>>,
            Vec<PeekableReceiver<ShardifyPartialCell>>,
        ) = (0..numof_streams)
            .map(|_| {
                let (tx, rx) = channel::unbounded::<ShardifyPartialCell>();
                (tx, PeekableReceiver::new(rx))
            })
            .unzip();

        // let vec_consumers_states = Arc::new(RwLock::new(Vec::with_capacity(numof_streams)));
        let mut vec_reader_handles = Vec::with_capacity(numof_streams as usize);
        // // let mut vec_worker_handles = Vec::with_capacity(self.threads_work);
        let mut vec_writer_handles = Vec::with_capacity(numof_writers as usize);

        let global_cells_processed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let global_cells_kept = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // bounds given by rtrb, this is only a notifier
        let (notify_tx, notify_rx) = channel::unbounded::<()>();

        for (thread_idx, (thread_input, thread_cell_tx)) in
            izip!(self.paths_in.clone(), vec_coordinator_tx).enumerate()
        {
            let thread_filter = Arc::clone(&arc_filter);
            let thread_notify_tx = notify_tx.clone();
            
            let thread_stream_buffer_size = each_stream_sizeof_buffer;
            let thread_stream_arena_size = self.sizeof_stream_arena;

            let global_processed_counter = Arc::clone(&global_cells_processed);
            let global_kept_counter = Arc::clone(&global_cells_kept);

            vec_reader_handles.push(budget.spawn::<TRead, _, _>(thread_idx as u64, move || {
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unknown thread"); 
                log_info!("Starting stream"; "thread" => thread_name, "path" => %thread_input);

                let thread_decoder = decode::Bgzf::builder()
                    .path(thread_input.path().path())
                    .num_threads(each_stream_numof_threads)
                    .build()
                    .unwrap();
                let thread_parser = parse::Tirp::builder()
                    .build()
                    .unwrap();

                let mut thread_stream = Stream::builder()
                    .with_decoder(thread_decoder)
                    .with_parser(thread_parser)
                    .sizeof_arena(thread_stream_arena_size)
                    .sizeof_buffer(thread_stream_buffer_size)
                    .build()
                    .unwrap();

                let mut query = thread_stream
                    .query::<tirp::Cell>()
                    .group_relaxed_with_context::<Id, Id, _>(
                        |id: &&'static [u8], id_ctx: &&'static [u8]| match id.cmp(id_ctx) {
                            std::cmp::Ordering::Less => panic!(
                                "Unordered record list: {:?}, id: {:?}, ctx: {:?}",
                                thread_input.path(),
                                String::from_utf8_lossy(id),
                                String::from_utf8_lossy(id_ctx)
                            ),
                            std::cmp::Ordering::Equal => QueryResult::Keep,
                            std::cmp::Ordering::Greater => QueryResult::Emit,
                        },
                    );

                loop {
                    let cell = match query.next_into::<ShardifyPartialCell>() {
                        Ok(Some(cell)) => {
                            cell
                        },
                        Ok(None) => {
                            let _ = thread_notify_tx.send(());
                            break;
                        }
                        Err(e) => {
                            // log_critical!("Reader: error"; "thread" => thread_name, "error" => ?e);
                            panic!("{:?}", e);
                        },
                    };
                    log_info!("Prudced data at stream"; "thread" => thread_name);
                    
                    let global_processed = global_processed_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    let global_kept = global_kept_counter
                        .load(std::sync::atomic::Ordering::Relaxed);
            
                    if global_processed % 10_000 == 0 {
                        let keep_ratio = (global_kept as f64) / (global_processed as f64); 
                        log_info!(
                            "Processing";
                            "(partial) cells processed" => global_processed,
                            "(partial) cells kept" => format!("{} ({:.2}%)", global_kept, 100.0 * keep_ratio)
                        );
                    }

                    // SAFETY: deref safe as long as cell is alive
                    if !thread_filter.contains(*cell.get_ref::<Id>()) {
                        continue;
                    }

                    global_kept_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let _ = thread_cell_tx.send(cell);
                    // log_info!("Stream sent notification"; "thread_idx" => thread_idx);
                    if thread_cell_tx.len() == 1 {
                        // NOTE: this means we just sent to an empty cell channel. Notify the coordinator!
                        let _ = thread_notify_tx.send(());
                    }
                }
            }));
        }

        let (write_tx, write_rx) = crossbeam::channel::unbounded::<Vec<ShardifyPartialCell>>();
        let global_cells_written = Arc::new(std::sync::atomic::AtomicU64::new(0));
        
        for (thread_idx, thread_output) in IntoIterator::into_iter(self.paths_out.clone()).enumerate()
        {
            log_info!("Starting writer thread"; "thread" => thread_idx, "output path" => %thread_output);

            let thread_file = match std::fs::File::create(thread_output.path().path()) {
                Ok(file) => file,
                Err(e) => {
                    log_critical!("Failed to create output file"; "path" => ?thread_output.path(), "error" => %e);
                }
            };

            let thread_buf_writer = BufWriter::new(thread_file);
            let mut thread_bgzf_writer =
                BGZFMultiThreadWriter::new(thread_buf_writer, Compression::fast());
            let thread_write_rx = write_rx.clone();

            let global_counter = Arc::clone(&global_cells_written);
            vec_writer_handles.push(budget.spawn::<TWrite, _, _>(thread_idx as u64, move || {
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unknown thread"); 
                log_info!("Starting writer"; "thread" => thread_name, "path" => %thread_output);

                while let Ok(vec_records) = thread_write_rx.recv() {
                    for cell in vec_records {
                        let _ = cell.write_to(&mut thread_bgzf_writer);
                    }

                    let global_count =
                        global_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if global_count % 1_000 == 0 {
                        log_info!("Writing"; "(complete) cells written" => global_count);
                    }
                }
                let _ = thread_bgzf_writer.flush();
            }));
        }

        let mut coordinator_spinpark_counter = 0;
        let mut coordinator_min_cell: Option<&[u8]> = None;
        let mut coordinator_vec_take: Vec<usize> = Vec::with_capacity(numof_streams as usize);
        let mut coordinator_vec_send: Vec<ShardifyPartialCell> = Vec::with_capacity(numof_streams as usize); // Local vec

        loop {
            match notify_rx.try_recv() {
                Ok(idx) => idx,
                Err(channel::TryRecvError::Empty) => {
                    spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                        &mut coordinator_spinpark_counter,
                        "Shardify (coordinator): channel peek (channel empty, producer slow)",
                    );
                    continue;
                }
                Err(channel::TryRecvError::Disconnected) => {
                    // NOTE: if the notify reciever is disconnected there will be no data sent anymore
                    break;
                }
            };
            coordinator_spinpark_counter = 0;

            'sweep: loop {
                for (sweep_idx, sweep_rx) in vec_coordinator_rx.iter_mut().enumerate() {
                    let sweep_cell = match sweep_rx.peek() {
                        Ok(token) => token,
                        Err(channel::TryRecvError::Disconnected) => {
                            continue;
                        }
                        Err(channel::TryRecvError::Empty) => {
                            coordinator_vec_take.clear();
                            break 'sweep;
                        }
                    };
                    let sweep_cell_id = sweep_cell.get_ref::<Id>();
                    match coordinator_min_cell {
                        None => {
                            coordinator_min_cell = Some(sweep_cell_id);
                            coordinator_vec_take.push(sweep_idx);
                            continue;
                        }
                        Some(cmc) if *sweep_cell_id < cmc => {
                            coordinator_min_cell = Some(sweep_cell_id);
                            coordinator_vec_take.clear();
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(cmc) if *sweep_cell_id == cmc => {
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(cmc) if *sweep_cell_id > cmc => {
                            continue;
                        }
                        _ => unreachable!()
                    }
                }

                for take_idx in &coordinator_vec_take {
                    let take_rx = &mut vec_coordinator_rx[*take_idx];
                    match take_rx.try_recv() {
                        Ok(take_cell) => {
                            coordinator_vec_send.push(take_cell);
                        }
                        Err(e) => {
                            log_critical!("try_recv failed!"; "stream" => take_idx, "error" => ?e, "vec_take" => ?coordinator_vec_take);
                        }
                    }
                }
                
                // log_info!("Coordinator: sending batch to writers"; "cells" => coordinator_vec_send.len());
                let _ = write_tx.send(coordinator_vec_send.clone());
                
                coordinator_min_cell = None;
                coordinator_vec_take.clear();
                coordinator_vec_send.clear();
            }
        }

        for handle in vec_reader_handles {
            handle.join().expect("Stream thread panicked");
        }
        log_info!("Stream handles closed");
        for handle in vec_writer_handles {
            handle.join().expect("Writer thread panicked");
        }
        log_info!("Write handles closed");

        log_info!("Shardify complete";
            "input files processed" => self.paths_in.len(),
            "output files created" => self.paths_out.len()
        );

        Ok(())
    }
}

fn read_filter<P: AsRef<Path>>(input: P) -> Arc<gxhash::HashSet<Vec<u8>>> {
    // GOOD FIRST ISSUE:
    // implement cell list reader around the support macros!
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let filter = reader
        .lines()
        .map(|l| l.unwrap().into_bytes())
        .collect::<gxhash::HashSet<Vec<u8>>>();

    if filter.is_empty() {
        log_warning!(
            "Empty cell list detected! This configuration may consume massive amounts of computer memory (potentially hundreds of GiB of RAM) and will DUPLICATE the input datasets."
        );
    }
    return Arc::new(filter);
}
#[derive(Composite, Clone, Default)]
#[bascet(
    attrs = (Id, SequencePair = vec_sequence_pairs, QualityPair = vec_quality_pairs, Umi = vec_umis),
    backing = ArenaBacking,
    marker = AsCell<Accumulate>,
    intermediate = tirp::Record
)]
pub struct ShardifyPartialCell {
    id: &'static [u8],
    #[collection]
    vec_sequence_pairs: Vec<(&'static [u8], &'static [u8])>,
    #[collection]
    vec_quality_pairs: Vec<(&'static [u8], &'static [u8])>,
    #[collection]
    vec_umis: Vec<&'static [u8]>,

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}
impl ShardifyPartialCell {
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let id = self.get_bytes::<Id>();
        let reads = &self.vec_sequence_pairs;
        let quals = &self.vec_quality_pairs;
        let umis = &self.vec_umis;

        for ((r1, r2), (q1, q2), umi) in izip!(reads, quals, umis) {
            writer.write_all(id)?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(&[crate::common::U8_CHAR_1])?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(&[crate::common::U8_CHAR_1])?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(r1)?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(r2)?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(q1)?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(q2)?;
            writer.write_all(&[crate::common::U8_CHAR_TAB])?;
            writer.write_all(umi)?;
            writer.write_all(&[crate::common::U8_CHAR_NEWLINE])?;
        }

        Ok(())
    }
}