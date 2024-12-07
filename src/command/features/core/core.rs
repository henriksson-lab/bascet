use std::{cmp::min, fs::File, sync::Arc, usize};

use clio::Output;
use memmap2::MmapOptions;

use crate::utils::{BoundedHeap, BoundedMaxHeap, BoundedMinHeap, KMERCodec};

use super::threading::DefaultThreadState;

pub struct ParamsIO<'a> {
    pub file_in: &'a File,
    pub path_out: &'a mut Output,
}

pub struct ParamsRuntime {
    pub kmer_size: usize,
    pub ovlp_size: usize,
    pub features_nmin: usize,
    pub features_nmax: usize,
    pub codec: KMERCodec,
    pub seed: u64,
}

pub struct ParamsThreading<'a> {
    pub threads_io: usize,
    pub threads_work: usize,
    pub thread_buffer_size: usize,
    pub thread_pool: &'a threadpool::ThreadPool,
    pub thread_states: &'a Vec<Arc<DefaultThreadState>>,
}

pub struct KMCProcessor<'a> {
    pub params_io: ParamsIO<'a>,
    pub params_runtime: ParamsRuntime,
    pub params_threading: ParamsThreading<'a>,
}

#[inline(always)]
fn find_chunk_start(chunk: &[u8], raw_start: usize, ovlp_size: usize) -> usize {
    for i in 0..min(ovlp_size, chunk.len()) {
        if chunk[i] == b'\n' {
            return raw_start + i + 1;
        }
    }
    raw_start
}

#[inline(always)]
fn find_chunk_end(chunk: &[u8], raw_end: usize, ovlp_size: usize) -> usize {
    let window_size = min(ovlp_size, chunk.len());
    for i in (chunk.len() - window_size..chunk.len()).rev() {
        if chunk[i] == b'\n' {
            return min(i + 1, raw_end);
        }
    }
    raw_end
}

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

#[inline(always)]
fn featurise_process_chunk(
    chunk: &[u8],
    rng: &mut impl rand::Rng,
    min_heap: &mut BoundedMinHeap<u128>,
    max_heap: &mut BoundedMaxHeap<u128>,
    codec: &KMERCodec,
    kmer_size: usize,
    ovlp_size: usize,
) {
    let chunk_length = chunk.len();
    let min_read_size = kmer_size + 2; // K + 2 is minimum size for a kmer + count (\t\d)
    let n_max_panes = chunk_length / min_read_size;
    let mut cursor = 0;

    for _ in 0..n_max_panes {
        if cursor >= chunk_length {
            break;
        }

        let pane_start = cursor;
        let remaining = chunk_length - pane_start;

        if remaining < min_read_size {
            break;
        }

        // Find the length of the current pane (up to next newline)
        let mut pane_length = min_read_size;
        for i in pane_length..min(ovlp_size, remaining) {
            if chunk[pane_start + i] == b'\n' {
                pane_length = i;
                break;
            }
        }

        // Extract and encode kmer with its count
        let kmer_end = pane_start + kmer_size;
        let count = unsafe { parse_count_u32(&chunk[kmer_end + 1..pane_start + pane_length]) };

        let encoded = unsafe {
            codec
                .encode(&chunk[pane_start..kmer_end], count, rng)
                .into_bits()
        };

        let _ = min_heap.push(encoded);
        let _ = max_heap.push(encoded);

        cursor += pane_length + 1; // +1 for newline
    }
}

impl<'a> KMCProcessor<'a> {
    pub fn new(
        params_io: ParamsIO<'a>,
        params_runtime: ParamsRuntime,
        params_threading: ParamsThreading<'a>,
    ) -> Self {
        Self {
            params_io,
            params_runtime,
            params_threading,
        }
    }

    pub fn extract(&self) -> anyhow::Result<(BoundedMinHeap<u128>, BoundedMaxHeap<u128>)> {
        let mmap = Arc::new(unsafe { MmapOptions::new().map(self.params_io.file_in) }.unwrap());
        let (tx, rx) = crossbeam::channel::bounded(64);
        let rx = Arc::new(rx);

        let thread_pool = self.params_threading.thread_pool;
        let io_tx = tx.clone();
        let io_mmap = Arc::clone(&mmap);
        let chunk_size = self.params_threading.thread_buffer_size;
        let ovlp_size = self.params_runtime.ovlp_size;
        let threads_work = self.params_threading.threads_work;

        thread_pool.execute(move || {
            let n_chunks = (io_mmap.len() + chunk_size - 1) / chunk_size;
            for i in 0..n_chunks {
                let raw_start = i * chunk_size;
                let raw_end = min(raw_start + chunk_size + ovlp_size, io_mmap.len());
                let valid_start = find_chunk_start(&io_mmap[raw_start..], raw_start, ovlp_size);
                let valid_end = find_chunk_end(&io_mmap[..raw_end], raw_end, ovlp_size);
                io_tx.send(Some((valid_start, valid_end))).unwrap();
            }
            for _ in 0..threads_work {
                io_tx.send(None).unwrap();
            }
        });

        let arc_codec = Arc::new(self.params_runtime.codec);
        let kmer_size = self.params_runtime.kmer_size;
        let ovlp_size = self.params_runtime.ovlp_size;

        for i in 0..self.params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let mmap = Arc::clone(&mmap);
            let state = Arc::clone(&self.params_threading.thread_states[i]);
            let codec = arc_codec.clone();
            let kmer_size = kmer_size;
            let ovlp_size = ovlp_size;

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
                        featurise_process_chunk(
                            buffer, rng, min_heap, max_heap, &codec, kmer_size, ovlp_size,
                        );
                    }
                }
            });
        }
        thread_pool.join();

        let mut final_min_heap = BoundedMinHeap::with_capacity(self.params_runtime.features_nmin);
        let mut final_max_heap = BoundedMaxHeap::with_capacity(self.params_runtime.features_nmax);
        for state in self.params_threading.thread_states.iter() {
            unsafe {
                final_min_heap.extend((&*state.min_heap.get()).iter().copied());
                final_max_heap.extend((&*state.max_heap.get()).iter().copied());
            }
        }

        Ok((final_min_heap, final_max_heap))
    }
}
