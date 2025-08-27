use crate::{
    command::determine_thread_counts_2,
    io::{
        format::tirp::{alloc::PageBuffer, SENTINEL_BYTE},
        traits::*,
    },
    kmer::kmc_counter::CountSketch,
    log_critical, log_info, log_warning, support_which_stream, support_which_temp,
    support_which_writer,
};

use clap::Args;
use itertools::enumerate;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

// SAFETY: These pointers are valid for the buffer page lifetime
#[derive(Clone, Copy)]
struct SafePtr<T>(*const T);

#[derive(Clone, Copy)]
struct SafeMutPtr<T>(*mut T);

unsafe impl<T> Send for SafePtr<T> {}
unsafe impl<T> Sync for SafePtr<T> {}
unsafe impl<T> Send for SafeMutPtr<T> {}
unsafe impl<T> Sync for SafeMutPtr<T> {}

impl<T> SafePtr<T> {
    #[inline(always)]
    fn new(ptr: *const T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    fn as_ptr(&self) -> *const T {
        self.0
    }
}

impl<T> SafeMutPtr<T> {
    #[inline(always)]
    fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    fn as_ptr(&self) -> *mut T {
        self.0
    }
}

pub const DEFAULT_THREADS_READ: usize = 8;
pub const DEFAULT_THREADS_WORK: usize = 4;
pub const DEFAULT_THREADS_TOTAL: usize = 12;
pub const DEFAULT_COUNTSKETCH_SIZE: usize = 128;
pub const DEFAULT_KMER_SIZE: usize = 31;
pub const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 128;

support_which_stream! {
    CountsketchInput => CountsketchStream<T: BascetCell>
    for formats [tirp, zip]
}
support_which_writer! {
    CountsketchOutput => CountsketchWriter<W: std::io::Write>
    for formats [csv]
}
#[derive(Args)]
pub struct CountsketchCMD {
    // Input bascets
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,
    // Output path
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_TOTAL)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
    // Countsketch parameters
    #[arg(long = "sketch-size", value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_COUNTSKETCH_SIZE)]
    pub countsketch_size: usize,
    // K-mer size
    #[arg(short = 'k', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_KMER_SIZE)]
    pub kmer_size: usize,
    // Channel buffer size
    #[arg(long = "buffer-size", value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_CHANNEL_BUFFER_SIZE)]
    pub channel_buffer_size: usize,
}

impl CountsketchCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        log_info!("Starting Countsketch";
            "input files" => self.path_in.len(),
            "output path" => ?self.path_out,
            "total threads" => self.threads_total,
            "read threads" => self.threads_read,
            "work threads" => self.threads_work,
            "countsketch size" => self.countsketch_size,
            "kmer size" => self.kmer_size
        );

        let (n_readers, n_workers) = match determine_thread_counts_2(
            Some(self.threads_total),
            Some(self.threads_read),
            Some(self.threads_work),
        ) {
            Ok(counts) => {
                log_info!("Thread allocation successful"; "readers" => counts.0, "workers" => counts.1);
                counts
            }
            Err(e) => {
                log_critical!("Failed to determine thread counts"; "error" => %e);
            }
        };

        let mut processed_files = 0;
        let mut total_cells_processed = 0;
        let mut total_errors = 0;

        for (i, input) in enumerate(&self.path_in) {
            log_info!("Processing input file"; "path" => ?input);

            let file = match CountsketchInput::try_from_path(input) {
                Ok(file) => file,
                Err(e) => {
                    log_warning!("Failed to open input file, skipping"; "path" => ?input, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let mut stream: CountsketchStream<CountsketchCell> =
                match CountsketchStream::try_from_input(file) {
                    Ok(stream) => stream,
                    Err(e) => {
                        log_warning!("Failed to create stream from file, skipping"; "path" => ?input, "error" => %e);
                        total_errors += 1;
                        continue;
                    }
                };
            stream = stream.set_reader_threads(n_readers);

            let output_path = self.path_out.join(format!("countsketch.{i}.csv"));
            let output_auto = match CountsketchOutput::try_from_path(&output_path) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to identify output file, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };
            let output_file = match File::create(output_auto.path()) {
                Ok(output) => output,
                Err(e) => {
                    log_warning!("Failed to create output file, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            };

            let mut output_countsketch_writer =  match CountsketchWriter::try_from_output(output_auto) {
                Ok(writer) => writer,
                Err(e) => {
                    log_warning!("Failed to create output writer, skipping"; "path" => ?output_path, "error" => %e);
                    total_errors += 1;
                    continue;
                }
            }.set_writer(BufWriter::new(output_file));

            let worker_threadpool = threadpool::ThreadPool::new(n_workers);
            let (work_tx, work_rx) =
                crossbeam::channel::bounded::<Option<CountsketchCell>>(self.channel_buffer_size);

            let (write_tx, write_rx) = crossbeam::channel::bounded::<
                Option<(CountsketchCell, CountSketch)>,
            >(self.channel_buffer_size);

            let _ = std::thread::spawn(move || {
                while let Ok(Some((cell, countsketch))) = write_rx.recv() {
                    // log_info!("Writing"; "cell" => %String::from_utf8_lossy(cell.cell), "open" => write_rx.len());
                    let _ = output_countsketch_writer.write_countsketch(&cell, &countsketch);
                }
                log_info!("Write finished!");
                let _ = output_countsketch_writer.get_writer().unwrap().flush();
            });

            for worker_id in 0..n_workers {
                let work_rx = work_rx.clone();
                let write_tx = write_tx.clone();

                let kmer_size = self.kmer_size;
                let mut countsketch = CountSketch::new(self.countsketch_size);

                worker_threadpool.execute(move || {
                    let mut cells_processed = 0;

                    while let Ok(Some(cell)) = work_rx.recv() {
                        // println!("Worker {} receiving cell {} at {:?}", 
                        //         worker_id,
                        //         String::from_utf8_lossy(cell.get_cell().unwrap()),
                        //         std::time::SystemTime::now());
                        let Some(reads) = cell.get_reads() else { continue };

                        for (r1, r2) in reads {
                            for kmer in r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                            for kmer in r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                            let mut rev_r1 = Vec::with_capacity(r1.len());
                            for &base in r1.iter().rev() {
                                rev_r1.push(match base {
                                    b'A' => b'T', b'T' => b'A',
                                    b'G' => b'C', b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r1.windows(kmer_size) {
                                countsketch.add(kmer);
                            }

                            let mut rev_r2 = Vec::with_capacity(r2.len());
                            for &base in r2.iter().rev() {
                                rev_r2.push(match base {
                                    b'A' => b'T', b'T' => b'A',
                                    b'G' => b'C', b'C' => b'G',
                                    _ => base,
                                });
                            }
                            for kmer in rev_r2.windows(kmer_size) {
                                countsketch.add(kmer);
                            }
                        }

                        let _ = write_tx.send(Some((cell, countsketch.clone())));
                        countsketch.reset();
                        cells_processed += 1;
                    }
                    log_info!("Worker thread completed"; "worker id" => worker_id, "cells processed" => cells_processed);
                });
            }

            let (cells_parsed, parse_errors) = {
                let mut cells_parsed = 0;
                let mut parse_errors = 0;

                for cell_res in &mut stream {
                    let cell = match cell_res {
                        Ok(cell) => cell,
                        Err(e) => match e {
                            crate::runtime::Error::ParseError { .. } => {
                                log_warning!("Parse error"; "error" => %e);
                                parse_errors += 1;
                                continue;
                            }
                            _ => {
                                log_critical!("Stream error"; "error" => %e);
                            }
                        },
                    };

                    match work_tx.send(Some(cell)) {
                        Ok(_) => cells_parsed += 1,
                        Err(e) => {
                            log_critical!("Channel send failed"; "error" => %e);
                        }
                    }

                    if cells_parsed % 100 == 0 {
                        log_info!("Processing"; "cells parsed" => cells_parsed, "parse errors" => parse_errors);
                    }
                }

                (cells_parsed, parse_errors)
            };

            for worker_id in 0..n_workers {
                if let Err(e) = work_tx.send(None) {
                    log_warning!("Failed to send stop signal to worker"; "worker id" => worker_id, "error" => %e);
                }
            }
            let _ = write_tx.send(None);

            worker_threadpool.join();

            processed_files += 1;
            total_cells_processed += cells_parsed;
            total_errors += parse_errors;

            log_info!("Completed file"; "output path" => ?output_path, "cells processed" => cells_parsed, "parse errors" => parse_errors);
        }

        log_info!("Countsketch complete"; "files processed" => processed_files, "files given as input" => self.path_in.len(), "total cells processed" => total_cells_processed, "total errors" => total_errors);

        if total_errors > 0 {
            log_warning!("Execution completed with errors");
        }

        Ok(())
    }
}

struct CountsketchCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,

    _page_end_ptrs: Vec<(SafePtr<u8>, SafeMutPtr<PageBuffer>)>,
    _owned: Vec<Vec<u8>>,
}

impl Drop for CountsketchCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for (end_ptr, page_ptr) in &self._page_end_ptrs {
                let sentinel_pos = end_ptr.as_ptr().add(1);
                let page = page_ptr.as_ptr();
                let page_start = (*page).inner.as_ptr();
                let page_end = page_start.add((*page).inner.capacity());

                if sentinel_pos >= page_end || *sentinel_pos == SENTINEL_BYTE {
                    (*page).expired.store(true, Ordering::Relaxed);
                }
            }
        }
    }
}

impl BascetCell for CountsketchCell {
    type Builder<'page> = CountsketchCellBuilder;

    fn builder<'page>() -> Self::Builder<'page> {
        CountsketchCellBuilder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }
}

struct CountsketchCellBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,

    page_end_ptrs: Vec<(SafePtr<u8>, SafeMutPtr<PageBuffer>)>,
    owned: Vec<Vec<u8>>,
}

impl CountsketchCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),

            page_end_ptrs: Vec::new(),
            owned: Vec::new(),
        }
    }
}

impl<'page> BascetCellBuilder<'page> for CountsketchCellBuilder {
    type Token = CountsketchCell;

    #[inline(always)]
    fn add_sentinel_tracking(
        mut self,
        buffer_page_ptr: *mut PageBuffer,
        _buffer_bounds: (*const u8, *const u8),
    ) -> Self {
        if let Some(cell) = self.cell {
            let end_ptr = unsafe { cell.as_ptr().add(cell.len().saturating_sub(1)) };
            self.page_end_ptrs
                .push((SafePtr::new(end_ptr), SafeMutPtr::new(buffer_page_ptr)));
        }
        self
    }

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
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        self.reads.push((slice, &[]));
        self
    }

    #[inline(always)]
    fn add_rp_slice(mut self, r1: &'static [u8], r2: &'static [u8]) -> Self {
        self.reads.push((r1, r2));
        self
    }

    #[inline(always)]
    fn build(self) -> CountsketchCell {
        CountsketchCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,

            _page_end_ptrs: self.page_end_ptrs,
            _owned: self.owned,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for CountsketchStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
