use std::{cmp::min, fs::File, sync::Arc, usize};

use clio::Output;
use memmap2::MmapOptions;

use crate::utils::{BoundedMaxHeap, BoundedMinHeap};

use super::threading::DefaultThreadState;

pub struct ParamsIO<'a> {
    pub file_in: &'a mut std::fs::File,
    pub path_out: &'a mut Output,
}

pub struct ParamsRuntime {
    pub kmer_size: usize,
    pub ovlp_size: usize,
    pub features_nmin: usize,
    pub features_nmax: usize,
    pub seed: u64,
}

pub struct ParamsThreading<'a> {
    pub threads_io: usize,
    pub threads_work: usize,
    pub thread_buffer_size: usize,
    pub thread_pool: &'a threadpool::ThreadPool,
    pub thread_states: &'a Vec<Arc<DefaultThreadState>>,
}

pub fn extract_features(
    params_io: ParamsIO,
    params_runtime: ParamsRuntime,
    params_threading: ParamsThreading,
) -> anyhow::Result<()> {
    // no idea why i need to use &* here
    let mmap = Arc::new(unsafe { MmapOptions::new().map(&*params_io.file_in) }.unwrap());

    let (tx, rx) = crossbeam::channel::bounded(64);
    let rx = Arc::new(rx);

    // Launch I/O work in thread pool
    let thread_pool = params_threading.thread_pool;

    let io_tx = tx.clone();
    let io_mmap = Arc::clone(&mmap);
    let chunk_size = params_threading.thread_buffer_size;

    thread_pool.execute(move || {
        let n_chunks = (io_mmap.len() + chunk_size - 1) / chunk_size;
        for i in 0..n_chunks {
            let raw_start = i * chunk_size;
            let raw_end = min(
                raw_start + chunk_size + params_runtime.ovlp_size,
                io_mmap.len(),
            );
            let valid_start = Self::find_chunk_start(&io_mmap[raw_start..], raw_start);
            let valid_end = Self::find_chunk_end(&io_mmap[..raw_end], raw_end);
            io_tx.send(Some((valid_start, valid_end))).unwrap();
        }
        for _ in 0..pool_size {
            io_tx.send(None).unwrap();
        }
    });
    // Launch worker threads
    assert!(thread_states.len() == pool_size - 1);

    let n_worker_threads = pool_size - 1;
    for i in 0..n_worker_threads {
        let rx = Arc::clone(&rx);
        let mmap = Arc::clone(&mmap);
        let state = Arc::clone(&thread_states[i]);
        let codec = self.codec.clone();

        thread_pool.execute(move || {
            while let Ok(Some((start, end))) = rx.recv() {
                let chunk = &mmap[start..end];
                unsafe {
                    let rng = &mut *state.rng.get();
                    let min_heap = &mut *state.min_heap.get();
                    let max_heap = &mut *state.max_heap.get();
                    let buffer = &mut *state.buffer.get();
                    buffer.clear();
                    buffer.extend_from_slice(chunk);
                    Self::featurise_process_chunk(buffer, rng, min_heap, max_heap, &codec);
                }
            }
        });
    }
    thread_pool.join();

    // Merge results
    let mut final_min_heap = BoundedMinHeap::with_capacity(self.config.nlo_results);
    let mut final_max_heap = BoundedMaxHeap::with_capacity(self.config.nhi_results);
    for state in thread_states.iter() {
        unsafe {
            final_min_heap.extend((&*state.min_heap.get()).iter().copied());
            final_max_heap.extend((&*state.max_heap.get()).iter().copied());
        }
    }

    Ok((final_min_heap, final_max_heap))
}


// Find the start of a valid chunk at a newline boundary
#[inline(always)]
fn find_chunk_start(chunk: &[u8], raw_start: usize) -> usize {
    for i in 0..min(Self::OVERLAP_WINDOW_SIZE, chunk.len()) {
        if chunk[i] == b'\n' {
            return raw_start + i + 1;
        }
    }
    raw_start
}

// Find the end of a valid chunk at a newline boundary
#[inline(always)]
fn find_chunk_end(chunk: &[u8], raw_end: usize) -> usize {
    let window_size = min(Self::OVERLAP_WINDOW_SIZE, chunk.len());
    for i in (chunk.len() - window_size..chunk.len()).rev() {
        if chunk[i] == b'\n' {
            return min(i + 1, raw_end);
        }
    }
    raw_end
}

// Fast parsing of ASCII numerical values using a lookup table
#[inline(always)]
unsafe fn parse_count_u32(bytes: &[u8]) -> u32 {
    // Fast path for single digit (most common case)
    if bytes.len() == 1 {
        return (bytes[0] - b'0') as u32;
    }

    // Pre-computed lookup table for two-digit numbers
    const LOOKUP: [u32; 100] = {
        let mut table = [0u32; 100];
        let mut i = 0;
        while i < 100 {
            table[i] = (i / 10 * 10 + i % 10) as u32;
            i += 1;
        }
        table
    };

    // Handle first two digits
    if bytes.len() == 2 {
        let idx = ((bytes[0] - b'0') * 10 + (bytes[1] - b'0')) as usize;
        return LOOKUP[idx];
    }

    // Initialize with first two digits for longer numbers
    let mut result = {
        let idx = ((bytes[0] - b'0') * 10 + (bytes[1] - b'0')) as usize;
        LOOKUP[idx]
    };

    // Process remaining digits in pairs
    let remaining = &bytes[2..];
    let chunks = remaining.chunks_exact(2);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let idx = ((chunk[0] - b'0') * 10 + (chunk[1] - b'0')) as usize;
        result = result.wrapping_mul(100) + LOOKUP[idx];
    }

    // Handle last digit if present
    if let Some(&d) = remainder.first() {
        result = result.wrapping_mul(10) + (d - b'0') as u32;
    }

    return result;
}