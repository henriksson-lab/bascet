use crossbeam::channel::{self, bounded};
use memmap2::MmapOptions;
use rand::{distributions::Uniform, Rng};
use std::{
    cmp::min,
    fs::File,
    sync::{Arc, LazyLock},
    thread,
};
use threadpool::ThreadPool;

use crate::bounded_heap::{BoundedHeap, BoundedMaxHeap, BoundedMinHeap};

use super::ThreadState;

// Global uniform distribution for kmer encoding - thread-safe and initialized on first use
static RANGE: LazyLock<Uniform<u16>> = LazyLock::new(|| Uniform::from(u16::MIN..=u16::MAX));

#[derive(Clone, Copy)]
pub struct Config {
    pub threads: usize,
    pub work_threads: usize,
    pub chunk_size: usize,
    pub nlo_results: usize,
    pub nhi_results: usize,
}
impl Config {
    pub fn new(threads: usize, chunk_size: usize, nlo_results: usize, nhi_results: usize) -> Self {
        Self {
            threads: threads,
            work_threads: threads - 1,
            chunk_size: chunk_size,
            nlo_results: nlo_results,
            nhi_results: nhi_results,
        }
    }
}

pub struct Dump<const K: usize> {
    config: Config,
    codec: crate::kmer::Codec<K>,
}

impl<const K: usize> Dump<K> {
    // Window size calculation: K (kmer size) + 1 (newline) + 10 (max digits in u32) + 1 (safety margin)
    const OVERLAP_WINDOW_SIZE: usize = K + 1 + 10 + 1;

    pub fn new(config: Config) -> Self {
        Self {
            config,
            codec: crate::kmer::Codec::new(),
        }
    }

    // Main feature extraction function that processes file data in parallel
    pub fn featurise<R>(
        &self,
        file: File,
        thread_pool: &ThreadPool,
        thread_states: &[Arc<ThreadState<R>>],
    ) -> Result<(BoundedMinHeap<u128>, BoundedMaxHeap<u128>), ()>
    where
        R: Rng + Send + 'static,
    {
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.unwrap());
        let (tx, rx) = bounded(64);
        let rx = Arc::new(rx);
        let pool_size = thread_pool.max_count();
        assert!(pool_size >= 2);
        assert!(pool_size == self.config.threads);

        // Launch I/O work in thread pool
        let io_tx = tx.clone();
        let io_mmap = Arc::clone(&mmap);
        let chunk_size = self.config.chunk_size;

        thread_pool.execute(move || {
            let n_chunks = (io_mmap.len() + chunk_size - 1) / chunk_size;
            for i in 0..n_chunks {
                let raw_start = i * chunk_size;
                let raw_end = min(
                    raw_start + chunk_size + Self::OVERLAP_WINDOW_SIZE,
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

    // Process a single chunk of data
    #[inline(always)]
    fn featurise_process_chunk(
        chunk: &[u8],
        rng: &mut impl rand::Rng,
        min_heap: &mut BoundedMinHeap<u128>,
        max_heap: &mut BoundedMaxHeap<u128>,
        codec: &crate::kmer::Codec<K>,
    ) {
        let chunk_length = chunk.len();
        let n_max_panes = chunk_length / (K + 2); // K + 2 is minimum size for a kmer + count
        let mut cursor = 0;

        for _ in 0..n_max_panes {
            if cursor >= chunk_length {
                break;
            }

            let pane_start = cursor;
            let remaining = chunk_length - pane_start;

            if remaining < K + 2 {
                break;
            }

            // Find the length of the current pane (up to next newline)
            let mut pane_length = K + 2;
            for i in pane_length..min(Self::OVERLAP_WINDOW_SIZE, remaining) {
                if chunk[pane_start + i] == b'\n' {
                    pane_length = i;
                    break;
                }
            }

            // Extract and encode kmer with its count
            let kmer_end = pane_start + K;
            let count =
                unsafe { Self::parse_count_u32(&chunk[kmer_end + 1..pane_start + pane_length]) };

            let encoded = unsafe {
                codec
                    .encode(&chunk[pane_start..kmer_end], count, rng, *RANGE)
                    .into_bits()
            };
            // let decoded_kmer = unsafe { codec.decode(encoded) };
            // let decoded_count = crate::kmer::EncodedKMER::from_bits(encoded).count();
            // let random_bits = crate::kmer::EncodedKMER::from_bits(encoded).rand();
            // let original_kmer = std::str::from_utf8(&chunk[pane_start..kmer_end]).unwrap();
            // if false || decoded_kmer != original_kmer || decoded_count as u32 != count {
            //     println!(
            //         "MISMATCH! Original: kmer='{}' count={} | Decoded: kmer='{}' count={}, rand: {}",
            //         original_kmer, count, decoded_kmer, decoded_count, random_bits
            //     );
            // }
            let _ = min_heap.push(encoded);
            let _ = max_heap.push(encoded);

            cursor += pane_length + 1; // +1 for newline
        }
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
}
