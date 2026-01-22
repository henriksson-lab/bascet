use anyhow::Result;
use bascet_core::{
    channel::PeekableReceiver,
    spinpark_loop::{self, SPINPARK_PARKS_BEFORE_WARN, SpinPark},
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
use smallvec::{smallvec, SmallVec, ToSmallVec};
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
                (self.paths_in.len() + self.paths_out.len())
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

                let thread_file = match thread_input.clone().open() {
                    Ok(file) => file,
                    Err(e) => panic!("{e}")
                };

                let thread_decoder = codec::plain::PlaintextDecoder::builder()
                    .with_reader(thread_file)
                    .build();
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
                        Ok(Some(block)) => block,
                        Err(e) => panic!("{e:?}"),
                        Ok(None) => {
                            log_info!("Stream finished"; "thread" => thread_name);
                            break;
                        }
                    };

                    let global_processed = global_processed_counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    let global_kept = global_kept_counter
                        .load(std::sync::atomic::Ordering::Relaxed) + 1;

                    if global_processed % 100_000 == 0 {
                        let keep_ratio = (global_kept as f64) / (global_processed as f64);
                        log_info!(
                            "Processing";
                            "bbgz blocks processed" => global_processed,
                            "bbgz blocks kept" => format!("{} ({:.2}%)", global_kept, 100.0 * keep_ratio)
                        );
                    }

                    if !thread_filter.contains(block.as_bytes::<Id>()) {
                        continue;
                    }

                    global_kept_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let _ = thread_cell_tx.send(block);
                    if thread_cell_tx.len() == 1 {
                        // NOTE: this means we just sent to an empty cell channel. Notify the coordinator!
                        let _ = thread_notify_tx.send(());
                    }
                }
                let _ = thread_notify_tx.send(());
                drop(thread_notify_tx);
                drop(thread_cell_tx);
                log_info!("Reader thread exiting"; "thread" => thread_name);
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

            let mut thread_buf_writer = BufWriter::with_capacity(
                ByteSize::mib(8).as_u64() as usize,
                thread_file
            );
            let thread_write_rx = write_rx.clone();

            let global_counter = Arc::clone(&global_cells_written);
            vec_writer_handles.push(budget.spawn::<TWrite, _, _>(thread_idx as u64, move || {
                let thread = std::thread::current();
                let thread_name = thread.name().unwrap_or("unknown thread");
                log_info!("Starting writer"; "thread" => thread_name, "path" => %thread_output);

                let mut merge_blocks: SmallVec<[parse::bbgz::Block; 32]> = SmallVec::new();
                let mut merge_csize;
                let mut merge_hsize;

                while let Ok(vec_blocks) = thread_write_rx.recv() {
                    let n = vec_blocks.len() as u64;
                    merge_blocks.clear();
                    merge_csize = 0;
                    merge_hsize = 0;

                    for block in vec_blocks {
                        let header_bytes = block.as_bytes::<Header>();
                        let compressed_bytes = block.as_bytes::<Compressed>();

                        let csize = compressed_bytes.len();
                        let hsize = header_bytes.len() + csize;

                        if merge_hsize + hsize + BBGZTrailer::SSIZE > MAX_SIZEOF_BLOCKusize {
                            if merge_blocks.len() > 0 {
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

                                new_header.write_with_csize(&mut thread_buf_writer, merge_csize).unwrap();
                                let last_idx = merge_blocks.len() - 1;
                                for i in 0..last_idx {
                                    let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                                    let merge_raw_bytes_len = merge_raw_bytes.len();
                                    thread_buf_writer.write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)]).unwrap();
                                }
                                let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }.as_bytes::<Compressed>();
                                let last_raw_bytes_len = last_raw_bytes.len();
                                thread_buf_writer.write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)]).unwrap();
                                thread_buf_writer.write_all(&[0x03, 0x00]).unwrap();
                                new_trailer.write_with(&mut thread_buf_writer).unwrap();

                                merge_blocks.clear();
                                merge_csize = 0;
                                merge_hsize = 0;
                            }
                        }

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
                    }

                    if merge_blocks.len() > 0 {
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

                        new_header.write_with_csize(&mut thread_buf_writer, merge_csize).unwrap();
                        let last_idx = merge_blocks.len() - 1;
                        for i in 0..last_idx {
                            let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                            let merge_raw_bytes_len = merge_raw_bytes.len();
                            thread_buf_writer.write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)]).unwrap();
                        }
                        let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }.as_bytes::<Compressed>();
                        let last_raw_bytes_len = last_raw_bytes.len();
                        thread_buf_writer.write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)]).unwrap();
                        thread_buf_writer.write_all(&[0x03, 0x00]).unwrap();
                        new_trailer.write_with(&mut thread_buf_writer).unwrap();
                    }
                    
                    let last_counter = global_counter.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                    let new_counter = last_counter + n;
                    if last_counter / 100_000 != new_counter / 100_000 {
                        log_info!("Writing"; "bbgz blocks written" => new_counter);
                    }
                }
                
                thread_buf_writer
                    .write_all(&codec::bbgz::MARKER_EOF)
                    .unwrap();
                thread_buf_writer.flush().unwrap();
                log_info!("Exiting writer {thread_idx}");
            }));
        }

        let mut coordinator_spinpark_counter = 0;
        let mut coordinator_min_cell: Option<&[u8]> = None;
        let mut coordinator_vec_take: Vec<usize> = Vec::with_capacity(numof_streams as usize);
        let mut coordinator_vec_send: Vec<parse::bbgz::Block> = Vec::with_capacity(numof_streams as usize);
        // min cell is always alive during sweeps so this can be a slice. last sends are not always alive during sweeps so they must be cloned
        let mut coordinator_vec_last: SmallVec<[SmallVec<[u8; 16]>; 32]> = smallvec![smallvec![0; 16]; numof_streams as usize];

        'notify: loop {
            // Wait for notification (Ok/Disconnected both must proceed to sweep)
            if let Err(channel::TryRecvError::Empty) = notify_rx.try_recv() {
                spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                    &mut coordinator_spinpark_counter,
                    "Shardify (coordinator): waiting for notification",
                );
                continue;
            }
            coordinator_spinpark_counter = 0;

            'sweep: loop {
                coordinator_min_cell = None;
                coordinator_vec_take.clear();
                let mut sweeps_connected = vec_coordinator_rx.len();

                for (sweep_idx, sweep_rx) in vec_coordinator_rx.iter_mut().enumerate() {
                    let sweep_block = match sweep_rx.peek() {
                        Ok(block) => {
                            let block_id = block.as_bytes::<Id>();
                            let last_id = &mut coordinator_vec_last[sweep_idx];
                            if block_id > &**last_id {
                                last_id.clear();
                                last_id.extend_from_slice(block_id);
                            }
                            block
                        }
                        Err(channel::TryRecvError::Disconnected) => {
                            sweeps_connected -= 1;
                            continue;
                        }
                        Err(channel::TryRecvError::Empty) => {
                            // Channel empty - check if we need to wait for it
                            let last_id = &coordinator_vec_last[sweep_idx];
                            match coordinator_min_cell {
                                // This channel might produce a lower/equal ID than the min would be, must wait
                                None => break 'sweep,
                                // This channel might produce a lower/equal ID, must wait
                                Some(cmc) if &**last_id <= cmc => break 'sweep,
                                // This channel's last ID is higher, safe to skip
                                Some(_) => continue,
                            }
                        }
                    };

                    let sweep_id = sweep_block.as_bytes::<Id>();
                    match coordinator_min_cell {
                        None => {
                            coordinator_min_cell = Some(sweep_id);
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(cmc) if sweep_id < cmc => {
                            coordinator_min_cell = Some(sweep_id);
                            coordinator_vec_take.clear();
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(cmc) if sweep_id == cmc => {
                            coordinator_vec_take.push(sweep_idx);
                        }
                        _ => { }
                    }
                }

                // All channels scanned - take the minimum blocks
                for take_idx in &coordinator_vec_take {
                    let take_rx = &mut vec_coordinator_rx[*take_idx];
                    match take_rx.try_recv() {
                        Ok(take_cell) => coordinator_vec_send.push(take_cell),
                        Err(e) => log_critical!("try_recv failed!"; "stream" => take_idx, "error" => ?e),
                    }
                }

                if !coordinator_vec_send.is_empty() {
                    let _ = write_tx.send(coordinator_vec_send.clone());
                    coordinator_vec_send.clear();
                }

                if likely_unlikely::unlikely(sweeps_connected == 0) {
                    log_info!("All channels disconnected, exiting coordinator");
                    break 'notify;
                }
            }

            // Broke out of sweep without sending - spinpark before retrying
            spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                &mut coordinator_spinpark_counter,
                "Shardify (coordinator): sweep waiting for data",
            );
        }

        drop(write_tx);
        for handle in vec_writer_handles {
            handle.join().expect("Writer thread panicked");
        }
        log_info!("Write handles closed");

        for handle in vec_reader_handles {
            handle.join().expect("Reader thread panicked");
        }
        log_info!("Reader handles closed");

        log_info!("Shardify complete";
            "input files processed" => self.paths_in.len(),
            "output files created" => self.paths_out.len()
        );

        Ok(())
    }
}

fn read_filter<P: AsRef<Path>>(input: P) -> Arc<gxhash::HashSet<Vec<u8>>> {
    // TODO [GOOD FIRST ISSUE] Improve this
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let filter = reader
        .split(b'\n')
        .map(|l| l.unwrap())
        .collect::<gxhash::HashSet<Vec<u8>>>();

    if filter.is_empty() {
        log_warning!(
            "Empty cell list detected! This configuration will DUPLICATE the input datasets."
        );
    }
    return Arc::new(filter);
}
