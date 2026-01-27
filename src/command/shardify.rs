use anyhow::Result;
use bascet_core::{
    attr::{block::*, meta::*},
    channel::PeekableReceiver,
    spinpark_loop::{self, SpinPark, SPINPARK_PARKS_BEFORE_WARN},
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, BBGZHeader, BBGZTrailer, MAX_SIZEOF_BLOCKusize};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use clap::Args;
use clio::{InputPath, OutputPath};
use crossbeam::channel::{self, Receiver, Sender};
use itertools::{izip, Itertools};
use smallvec::{smallvec, SmallVec, ToSmallVec};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
        help = "File with list of cells to include (one per line). If omitted, all cells are kept."
    )]
    pub path_include: Option<InputPath>,

    #[arg(
        long = "temp",
        help = "Temporary storage directory. Defaults to <path_out>"
    )]
    pub path_temp: Option<PathBuf>,

    #[arg(
        short = '@',
        long = "threads",                                                                               
        help = "Total threads to use",                   
        value_name = "2..",        
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    pub total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-write",
        help = "Number of writer threads",
        value_name = "1..",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    pub numof_threads_write: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(32),
        value_parser = clap::value_parser!(ByteSize),
    )]
    pub total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size. Will be divided evenly across streams.",
        value_name = "100%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    pub sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arema buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    pub sizeof_stream_arena: ByteSize,

    #[arg(
        long = "show-filter-warning",
        default_value_t = true,
        hide = true
    )]
    pub show_filter_warning: bool,

    #[arg(
        long = "show-startup-message",
        default_value_t = true,
        hide = true
    )]
    pub show_startup_message: bool,
}

#[derive(Budget, Debug)]
struct ShardifyBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |_, _| BoundedU64::const_new::<1>())]
    countof_threads_read: BoundedU64<1, { u64::MAX }>,

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
                    .expect("At least one input file and one output file required")
            }))
            .memory(self.total_mem)
            .countof_threads_read(
                (self.paths_in.len())
                    .try_into()
                    .expect("At least one input file required"),
            )
            .countof_threads_write(self.numof_threads_write.unwrap_or_else(|| {
                (self.paths_out.len())
                    .try_into()
                    .expect("At least one output file required")
            }))
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();
        budget.validate();

        let arc_filter = match &self.path_include {
            Some(path) => read_filter(&**path.path(), self.show_filter_warning),
            None => Arc::new(None),
        };
        let countof_streams_input = self.paths_in.len() as u64;
        let countof_writers_output = self.paths_out.len() as u64;

        let sizeof_stream_each_buffer = ByteSize(budget.mem::<MBuffer>().as_u64() / countof_streams_input);

        if !self.show_startup_message {
            log_info!(
                "Starting Shardify";
                "using" => %budget,
                "memory per stream" => %sizeof_stream_each_buffer,
                "cells in filter" => (&*arc_filter).as_ref().map_or(0, |f| f.len())
            );
        }

        let pairs: Vec<(
            Sender<parse::bbgz::Block>,
            PeekableReceiver<parse::bbgz::Block>,
        )> = (0..countof_streams_input)
            .map(|_| bascet_core::channel::peekable::<parse::bbgz::Block>())
            .collect();
        let (vec_coordinator_tx, mut vec_coordinator_rx): (
            Vec<Sender<parse::bbgz::Block>>,
            Vec<PeekableReceiver<parse::bbgz::Block>>,
        ) = pairs.into_iter().unzip();

        // let vec_consumers_states = Arc::new(RwLock::new(Vec::with_capacity(numof_streams)));
        let mut vec_reader_handles = Vec::with_capacity(countof_streams_input as usize);
        // // let mut vec_worker_handles = Vec::with_capacity(self.threads_work);
        let mut vec_writer_handles = Vec::with_capacity(countof_writers_output as usize);

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

                    if let Some(ref filter) = *thread_filter {
                        if !filter.contains(block.as_bytes::<Id>()) {
                            continue;
                        }
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

        let shard_channels: Vec<(
            Sender<Vec<parse::bbgz::Block>>,
            Receiver<Vec<parse::bbgz::Block>>,
        )> = (0..countof_writers_output)
            .map(|_| crossbeam::channel::unbounded::<Vec<parse::bbgz::Block>>())
            .collect();
        let (vec_write_tx, vec_write_rx): (Vec<_>, Vec<_>) = shard_channels.into_iter().unzip();

        let global_cells_written = Arc::new(std::sync::atomic::AtomicU64::new(0));

        for (thread_idx, (thread_output, thread_write_rx)) in
            izip!(self.paths_out.clone(), vec_write_rx).enumerate()
        {
            log_info!("Starting writer thread"; "thread" => thread_idx, "output path" => %thread_output);

            let thread_file = match thread_output.clone().create() {
                Ok(file) => file,
                Err(e) => {
                    log_critical!("Failed to create output file"; "path" => ?thread_output.path(), "error" => %e);
                }
            };

            let mut thread_buf_writer =
                BufWriter::with_capacity(ByteSize::mib(8).as_u64() as usize, thread_file);

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
                                        merge_first.as_bytes::<Trailer>(),
                                    )
                                };

                                let mut new_header =
                                    BBGZHeader::from_bytes(new_header_bytes).unwrap();
                                let mut new_trailer =
                                    BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                                for merge_block in merge_blocks.iter().skip(1) {
                                    let merge_header_bytes = merge_block.as_bytes::<Header>();
                                    let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                                    let merge_header =
                                        BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                                    let merge_trailer =
                                        BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                                    new_header.merge(merge_header).unwrap();
                                    new_trailer.merge(merge_trailer).unwrap();
                                }

                                new_header
                                    .write_with_csize(&mut thread_buf_writer, merge_csize)
                                    .unwrap();
                                let last_idx = merge_blocks.len() - 1;
                                for i in 0..last_idx {
                                    let merge_raw_bytes = unsafe { merge_blocks.get_unchecked(i) }
                                        .as_bytes::<Compressed>();
                                    let merge_raw_bytes_len = merge_raw_bytes.len();
                                    thread_buf_writer
                                        .write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)])
                                        .unwrap();
                                }
                                let last_raw_bytes =
                                    unsafe { merge_blocks.get_unchecked(last_idx) }
                                        .as_bytes::<Compressed>();
                                let last_raw_bytes_len = last_raw_bytes.len();
                                thread_buf_writer
                                    .write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)])
                                    .unwrap();
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
                                merge_first.as_bytes::<Trailer>(),
                            )
                        };

                        let mut new_header = BBGZHeader::from_bytes(new_header_bytes).unwrap();
                        let mut new_trailer = BBGZTrailer::from_bytes(new_trailer_bytes).unwrap();

                        for merge_block in merge_blocks.iter().skip(1) {
                            let merge_header_bytes = merge_block.as_bytes::<Header>();
                            let merge_trailer_bytes = merge_block.as_bytes::<Trailer>();

                            let merge_header = BBGZHeader::from_bytes(merge_header_bytes).unwrap();
                            let merge_trailer =
                                BBGZTrailer::from_bytes(merge_trailer_bytes).unwrap();

                            new_header.merge(merge_header).unwrap();
                            new_trailer.merge(merge_trailer).unwrap();
                        }

                        new_header
                            .write_with_csize(&mut thread_buf_writer, merge_csize)
                            .unwrap();
                        let last_idx = merge_blocks.len() - 1;
                        for i in 0..last_idx {
                            let merge_raw_bytes =
                                unsafe { merge_blocks.get_unchecked(i) }.as_bytes::<Compressed>();
                            let merge_raw_bytes_len = merge_raw_bytes.len();
                            thread_buf_writer
                                .write_all(&merge_raw_bytes[..(merge_raw_bytes_len - 2)])
                                .unwrap();
                        }
                        let last_raw_bytes = unsafe { merge_blocks.get_unchecked(last_idx) }
                            .as_bytes::<Compressed>();
                        let last_raw_bytes_len = last_raw_bytes.len();
                        thread_buf_writer
                            .write_all(&last_raw_bytes[..(last_raw_bytes_len - 2)])
                            .unwrap();
                        thread_buf_writer.write_all(&[0x03, 0x00]).unwrap();
                        new_trailer.write_with(&mut thread_buf_writer).unwrap();
                    }

                    let last_counter =
                        global_counter.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
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

        let mut coordinator_vec_last_id: SmallVec<[SmallVec<[u8; 16]>; 32]> =
            smallvec![smallvec![0; 16]; countof_streams_input as usize];
        let mut coordinator_vec_take: Vec<usize> = Vec::with_capacity(countof_streams_input as usize);
        let mut coordinator_vec_send: Vec<parse::bbgz::Block> =
            Vec::with_capacity(countof_streams_input as usize);
        let mut coordinator_spinpark_counter = 0;
        let mut sweep_spinpark_counter = 0;

        'notify: loop {
            if let Err(channel::TryRecvError::Empty) = notify_rx.try_recv() {
                spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                    &mut coordinator_spinpark_counter,
                    "Shardify (coordinator): waiting for notification",
                );
                continue;
            }
            coordinator_spinpark_counter = 0;

            'sweep: loop {
                let mut sweep_min_cell: Option<&[u8]> = None;
                let mut sweep_connected = vec_coordinator_rx.len();
                coordinator_vec_take.clear();

                for (sweep_idx, sweep_rx) in vec_coordinator_rx.iter_mut().enumerate() {
                    let sweep_block = match sweep_rx.peek() {
                        Ok(block) => {
                            let block_id = block.as_bytes::<Id>();
                            let last_id = &mut coordinator_vec_last_id[sweep_idx];
                            if block_id > &**last_id {
                                last_id.clear();
                                last_id.extend_from_slice(block_id);
                            }
                            block
                        }
                        Err(channel::TryRecvError::Disconnected) => {
                            sweep_connected -= 1;
                            continue;
                        }
                        Err(channel::TryRecvError::Empty) => {
                            let last_id = &coordinator_vec_last_id[sweep_idx];
                            match sweep_min_cell {
                                None => {
                                    spinpark_loop::spinpark_loop_warn::<
                                        100,
                                        SPINPARK_PARKS_BEFORE_WARN,
                                    >(
                                        &mut sweep_spinpark_counter,
                                        "Shardify (coordinator): sweep waiting for data",
                                    );
                                    break 'sweep;
                                }
                                Some(mc) if &**last_id <= mc => {
                                    spinpark_loop::spinpark_loop_warn::<
                                        100,
                                        SPINPARK_PARKS_BEFORE_WARN,
                                    >(
                                        &mut sweep_spinpark_counter,
                                        "Shardify (coordinator): sweep waiting for data",
                                    );
                                    break 'sweep;
                                }
                                Some(_) => continue,
                            }
                        }
                    };
                    sweep_spinpark_counter = 0;

                    let sweep_id = sweep_block.as_bytes::<Id>();
                    match sweep_min_cell {
                        None => {
                            sweep_min_cell = Some(sweep_id);
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(mc) if sweep_id < mc => {
                            sweep_min_cell = Some(sweep_id);
                            coordinator_vec_take.clear();
                            coordinator_vec_take.push(sweep_idx);
                        }
                        Some(mc) if sweep_id == mc => {
                            coordinator_vec_take.push(sweep_idx);
                        }
                        _ => {}
                    }
                }

                for &sweep_idx in &coordinator_vec_take {
                    match vec_coordinator_rx[sweep_idx].try_recv() {
                        Ok(block) => coordinator_vec_send.push(block),
                        Err(e) => {
                            log_critical!("try_recv failed!"; "stream" => sweep_idx, "error" => ?e)
                        }
                    }
                }

                if !coordinator_vec_send.is_empty() {
                    let cell_id = unsafe { coordinator_vec_send.get_unchecked(0) }.as_bytes::<Id>();
                    let shard_idx = (gxhash::gxhash64(cell_id, 0x00) % countof_writers_output) as usize;
                    // std::mem::take(&mut coordinator_vec_send);
                    let _ = vec_write_tx[shard_idx].send(std::mem::take(&mut coordinator_vec_send));
                }

                if likely_unlikely::unlikely(sweep_connected == 0) {
                    log_info!("All channels disconnected, exiting coordinator");
                    break 'notify;
                }
            }
        }

        drop(vec_write_tx);
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

fn read_filter<P: AsRef<Path>>(input: P, show_warning: bool) -> Arc<Option<gxhash::HashSet<Vec<u8>>>> {
    // TODO [GOOD FIRST ISSUE] Improve this
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let filter = reader
        .split(b'\n')
        .map(|l| l.unwrap())
        .collect::<gxhash::HashSet<Vec<u8>>>();

    if filter.is_empty() && show_warning {
        log_warning!(
            "Empty cell list detected! This configuration will DUPLICATE the input datasets."
        );
    }
    Arc::new(Some(filter))
}
