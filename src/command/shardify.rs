use anyhow::Result;
use bascet_core::{
    channel::PeekableReceiver,
    spinpark_loop::{self, SPINPARK_PARKS_BEFORE_WARN},
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, BBGZHeader, BBGZTrailer, MAX_SIZEOF_BLOCKusize};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::Args;
use clio::{InputPath, OutputPath};
use crossbeam::channel::{self, Sender};
use itertools::{izip, Itertools};
use smallvec::SmallVec;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
};

use crate::{bounded_parser, log_critical, log_info, log_warning};

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
        help = "File with list of cells to include (one per line)"
    )]
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
        default_value_t = ByteSize::gib(32),
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

    #[threads(TRead, |_, _| BoundedU64::const_new::<2>())]
    countof_threads_read: BoundedU64<2, { u64::MAX }>,

    #[threads(TWrite, |_, _| BoundedU64::const_new::<1>())]
    countof_threads_write: BoundedU64<1, { u64::MAX }>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl ShardifyCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let budget = ShardifyBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                (self.paths_in.len() + 1)
                    .try_into()
                    .expect("At least two input files and one output file required")
            }))
            .memory(self.total_mem)
            .countof_threads_read(
                (self.paths_in.len())
                    .try_into()
                    .expect("At least two input files required"),
            )
            .countof_threads_write(self.numof_threads_write.unwrap_or_else(|| {
                (self.paths_out.len())
                    .try_into()
                    .expect("At least one output file required")
            }))
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();
        budget.validate();

        let arc_filter = read_filter(&self.path_include.path().path());
        let numof_streams = self.paths_in.len() as u64;
        let numof_writers = self.paths_out.len() as u64;

        let sizeof_stream_each_buffer = ByteSize(budget.mem::<MBuffer>().as_u64() / numof_streams);

        log_info!(
            "Starting Shardify";
            "using" => %budget,
            "memory per stream" => %sizeof_stream_each_buffer,
            "cells in filter" => arc_filter.len()
        );

        let pairs: Vec<(
            Sender<parse::bbgz::Block>,
            PeekableReceiver<parse::bbgz::Block>,
        )> = (0..numof_streams)
            .map(|_| bascet_core::channel::peekable::<parse::bbgz::Block>())
            .collect();
        let (vec_coordinator_tx, mut vec_coordinator_rx): (
            Vec<Sender<parse::bbgz::Block>>,
            Vec<PeekableReceiver<parse::bbgz::Block>>,
        ) = pairs.into_iter().unzip();

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

            let thread_stream_buffer_size = sizeof_stream_each_buffer;
            let thread_stream_arena_size = self.sizeof_stream_arena;

            let global_processed_counter = Arc::clone(&global_cells_processed);
            let global_kept_counter = Arc::clone(&global_cells_kept);

            vec_reader_handles.push(budget.spawn::<TRead, _, _>(thread_idx as u64, move || {
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unknown thread"); 
                log_info!("Starting stream"; "thread" => thread_name, "path" => %thread_input);

                let thread_decoder = codec::plain::PlaintextDecoder::builder()
                    .with_path(&*thread_input.path())
                    .build()
                    .unwrap();
                let thread_parser = parse::bbgz::parser();

                let mut thread_stream = Stream::builder()
                    .with_decoder(thread_decoder)
                    .with_parser(thread_parser)
                    .sizeof_decode_arena(thread_stream_arena_size)
                    .sizeof_decode_buffer(thread_stream_buffer_size)
                    .build();

                let mut query = thread_stream
                    .query::<parse::bbgz::Block>()
                    .assert_with_context::<Id, Id, _>(
                        |id_current: &&'static [u8], id_context: &&'static [u8]| {
                            id_current >= id_context
                        },
                        "id_current < id_context",
                    );

                loop {
                    let block = match query.next() {
                        Ok(Some(cell)) => {
                            cell
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        },
                    };

                    let global_processed = global_processed_counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    let global_kept = global_kept_counter
                        .load(std::sync::atomic::Ordering::Relaxed);

                    if global_processed % 1_000 == 0 {
                        let keep_ratio = (global_kept as f64) / (global_processed as f64);
                        log_info!(
                            "Processing";
                            "bbgz blocks processed" => global_processed,
                            "bbgz blocks kept" => format!("{} ({:.2}%)", global_kept, 100.0 * keep_ratio)
                        );
                    }

                    // SAFETY: deref safe as long as cell is alive
                    if !thread_filter.contains(block.as_bytes::<Id>()) {
                        continue;
                    }

                    global_kept_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let _ = thread_cell_tx.send(block);
                    // log_info!("Stream sent notification"; "thread_idx" => thread_idx);
                    if thread_cell_tx.len() == 1 {
                        // NOTE: this means we just sent to an empty cell channel. Notify the coordinator!
                        let _ = thread_notify_tx.send(());
                    }
                }
            }));
        }
        drop(notify_tx);

        let (write_tx, write_rx) = crossbeam::channel::unbounded::<Vec<parse::bbgz::Block>>();
        let global_cells_written = Arc::new(std::sync::atomic::AtomicU64::new(0));

        for (thread_idx, thread_output) in
            IntoIterator::into_iter(self.paths_out.clone()).enumerate()
        {
            log_info!("Starting writer thread"; "thread" => thread_idx, "output path" => %thread_output);

            let thread_file = match thread_output.clone().create() {
                Ok(file) => file,
                Err(e) => {
                    log_critical!("Failed to create output file"; "path" => ?thread_output.path(), "error" => %e);
                }
            };

            let mut thread_buf_writer = BufWriter::new(thread_file);
            let thread_write_rx = write_rx.clone();

            let global_counter = Arc::clone(&global_cells_written);
            vec_writer_handles.push(budget.spawn::<TWrite, _, _>(thread_idx as u64, move || {
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unknown thread");
                log_info!("Starting writer"; "thread" => thread_name, "path" => %thread_output);

                while let Ok(vec_blocks) = thread_write_rx.recv() {
                    let mut merge_blocks: SmallVec<[parse::bbgz::Block; 32]> = SmallVec::new();
                    let mut merge_bsize = 0;

                    for block in vec_blocks {
                        let header_bytes = block.as_bytes::<Header>();
                        let raw_bytes = block.as_bytes::<Compressed>();
                        let trailer_bytes = block.as_bytes::<Trailer>();

                        let bsize = header_bytes.len() + raw_bytes.len() + trailer_bytes.len() - 1;
                        if bsize + merge_bsize > MAX_SIZEOF_BLOCKusize {
                            // SAFETY at this point we will always have at least 1 merge block
                            let new_header_bytes = merge_blocks[0].as_bytes::<Header>();
                            let new_trailer_bytes = merge_blocks[0].as_bytes::<Header>();

                            let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                            let mut new_trailer =
                                BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                            for merge_block in merge_blocks.iter().skip(1) {
                                let merge_header_bytes = merge_block.as_bytes::<Header>();
                                let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                                let merge_header =
                                    BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                                let merge_trailer =
                                    BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                                // SAFETY bsize + merge_bsize > usize_MAX_SIZEOF_BLOCK guarantees blocks can be merged
                                unsafe { new_header.merge_unchecked(merge_header) };
                                new_trailer.merge(merge_trailer);
                            }

                            // new_header
                            //     .write_with_bsize(&mut thread_buf_writer, merge_bsize)
                            //     .unwrap();
                            for merge_block in &merge_blocks {
                                let merge_raw_bytes = merge_block.as_bytes::<Compressed>();
                                thread_buf_writer.write_all(merge_raw_bytes).unwrap();
                            }
                            new_trailer.write_with(&mut thread_buf_writer).unwrap();

                            merge_blocks.clear();
                            merge_bsize = 0;
                        }
                        let header = BBGZHeader::from_bytes(header_bytes).unwrap();
                        merge_blocks.push(block);
                        // merge_bsize += header.BC.BSIZE as usize;
                    }
                    if merge_blocks.len() > 0 {
                        // SAFETY at this point we will always have at least 1 merge block
                        let new_header_bytes = merge_blocks[0].as_bytes::<Header>();
                        let new_trailer_bytes = merge_blocks[0].as_bytes::<Header>();

                        let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                        let mut new_trailer = BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                        for merge_block in merge_blocks.iter().skip(1) {
                            let merge_header_bytes = merge_block.as_bytes::<Header>();
                            let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                            let merge_header = BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                            let merge_trailer =
                                BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                            // SAFETY if block was larger than usize_MAX_BLOCK_SIZE then it wouldve been flushed earlier
                            unsafe { new_header.merge_unchecked(merge_header) };
                            new_trailer.merge(merge_trailer);
                        }

                        // new_header
                        //     .write_with_bsize(&mut thread_buf_writer, merge_bsize)
                        //     .unwrap();
                        for merge_block in &merge_blocks {
                            let merge_raw_bytes = merge_block.as_bytes::<Compressed>();
                            thread_buf_writer.write_all(merge_raw_bytes).unwrap();
                        }
                        new_trailer.write_with(&mut thread_buf_writer).unwrap();
                    }

                    let global_count =
                        global_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if global_count % 1_000 == 0 {
                        log_info!("Writing"; "bbgz blocks written" => global_count);
                    }
                }
                thread_buf_writer
                    .write_all(&codec::bbgz::MARKER_EOF)
                    .unwrap();
                thread_buf_writer.flush().unwrap();
            }));
        }

        let mut coordinator_spinpark_counter = 0;
        let mut coordinator_min_cell: Option<&[u8]> = None;
        let mut coordinator_vec_take: Vec<usize> = Vec::with_capacity(numof_streams as usize);
        let mut coordinator_vec_send: Vec<parse::bbgz::Block> =
            Vec::with_capacity(numof_streams as usize);

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
                        _ => unreachable!(),
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

        drop(write_tx);
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
