use fs2::FileExt;
use memmap2::MmapOptions;
use std::{cmp::min, fs::File, sync::Arc, usize};

use crate::utils::{BoundedHeap, BoundedMaxHeap, BoundedMinHeap, KMERCodec};

use super::{
    constants::{HUGE_PAGE_SIZE, KMC_COUNTER_MAX_DIGITS},
    params,
};

pub struct KMCProcessor {}

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
    // Fast path for single digit, most common case
    if bytes.len() == 1 {
        return (bytes[0] - b'0') as u32;
    }

    // Fast path for two digits, second most common case
    if bytes.len() == 2 {
        return ((bytes[0] - b'0') * 10 + (bytes[1] - b'0')) as u32;
    }

    // LUT for two-digit numbers
    const LOOKUP: [u32; 100] = {
        let mut table = [0u32; 100];
        let mut i = 0;
        while i < 100 {
            table[i] = (i / 10 * 10 + i % 10) as u32;
            i += 1;
        }
        table
    };

    let chunks = bytes.chunks_exact(2);
    let remainder = chunks.remainder();

    let mut result = 0u32;
    for chunk in chunks {
        let idx = ((chunk[0] - b'0') * 10 + (chunk[1] - b'0')) as usize;
        result = result.wrapping_mul(100) + LOOKUP[idx];
    }

    // Handle last digit if present
    if let Some(&d) = remainder.first() {
        result = result.wrapping_mul(10) + (d - b'0') as u32;
    }

    result
}

#[inline(always)]
fn featurise_process_chunk(
    chunk: &[u8],
    rng: &mut impl rand::Rng,
    min_heap: &mut BoundedMinHeap<u128>,
    max_heap: &mut BoundedMaxHeap<u128>,
    codec: KMERCodec,
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
            // eprintln!(
            //     "About to encode kmer at offset {}, len={}: {:?}",
            //     pane_start,
            //     kmer_size,
            //     std::str::from_utf8(&chunk[pane_start..kmer_end]).unwrap_or("[invalid utf8]")
            // );

            codec
                .encode(&chunk[pane_start..kmer_end], count, rng)
                .into_bits()
        };

        let _ = min_heap.push(encoded);
        let _ = max_heap.push(encoded);

        cursor += pane_length + 1; // +1 for newline
    }
}

impl KMCProcessor {
    pub fn extract<'a>(
        params_io: params::IO<'a>,
        params_runtime: params::Runtime,
        params_threading: params::Threading<'a>,
    ) -> anyhow::Result<(BoundedMinHeap<u128>, BoundedMaxHeap<u128>)> {
        let params_io = Arc::new(params_io);
        let params_runtime = Arc::new(params_runtime);
        let params_threading = Arc::new(params_threading);

        let file = File::open(params_io.path_in).unwrap();
        let lock = file.lock_exclusive();
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.unwrap());
        let (tx, rx) = crossbeam::channel::bounded(256);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for i in 0..params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let mmap = Arc::clone(&mmap);
            let state = Arc::clone(&params_threading.thread_states[i]);
            let params_runtime = Arc::clone(&params_runtime);

            let ovlp_size = params_runtime.kmer_size + KMC_COUNTER_MAX_DIGITS;
            params_threading.thread_pool.execute(move || {
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
                            buffer,
                            rng,
                            min_heap,
                            max_heap,
                            params_runtime.codec,
                            params_runtime.kmer_size,
                            ovlp_size,
                        );
                    }
                }
            });
        }

        let io_tx = Arc::clone(&tx);
        let io_mmap = Arc::clone(&mmap);
        let io_buffer_size = params_threading.thread_buffer_size;
        let io_threads_work = params_threading.threads_work;
        let io_ovlp_size = params_runtime.kmer_size + KMC_COUNTER_MAX_DIGITS;

        params_threading.thread_pool.execute(move || {
            let n_chunks = (io_mmap.len() + io_buffer_size - 1) / io_buffer_size;
            for i in 0..n_chunks {
                let raw_start = i * io_buffer_size;
                let raw_end = min(raw_start + io_buffer_size + io_ovlp_size, io_mmap.len());
                let valid_start = find_chunk_start(&io_mmap[raw_start..], raw_start, io_ovlp_size);
                let valid_end = find_chunk_end(&io_mmap[..raw_end], raw_end, io_ovlp_size);
                io_tx.send(Some((valid_start, valid_end))).unwrap();
            }
            for _ in 0..io_threads_work {
                io_tx.send(None).unwrap();
            }
        });

        params_threading.thread_pool.join();

        let mut final_min_heap = BoundedMinHeap::with_capacity(params_runtime.features_nmin);
        let mut final_max_heap = BoundedMaxHeap::with_capacity(params_runtime.features_nmax);

        for state in params_threading.thread_states.iter() {
            unsafe {
                final_min_heap.extend((&*state.min_heap.get()).iter().copied());
                final_max_heap.extend((&*state.max_heap.get()).iter().copied());
            }
        }
        drop(lock);
        Ok((final_min_heap, final_max_heap))
    }
}
