use std::{cell::UnsafeCell, cmp::min, fs::File, marker::PhantomData, sync::Arc};

use crossbeam::channel;
use memmap2::MmapOptions;
use rand::{distributions::Uniform, SeedableRng};
use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
    ThreadPool,
};

use crate::bounded_heap::{BoundedHeap, BoundedMaxHeap, BoundedMinHeap};
use crate::kmc::ThreadState;

pub struct Config {
    pub seed: usize,
    pub threads: usize,
    pub chunk_size: usize,
    pub nlo_results: usize,
    pub nhi_results: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 0,
            threads: 8,
            chunk_size: 262144, // 256KiB chunks
            nlo_results: 50_000,
            nhi_results: 1_000,
        }
    }
}

pub struct Dump<const K: usize> {
    config: Config,
    codec: crate::kmer::Codec<K>,
}

impl<const K: usize> Dump<K> {
    const OVERLAP_WINDOW_SIZE: usize = K + 1 + 10 + 1;

    pub fn new(config: Config) -> Self {
        Self {
            config,
            codec: crate::kmer::Codec::new(),
        }
    }

    #[inline(always)]
    fn parse_count(bytes: &[u8]) -> u32 {
        let mut count = 0u32;
        for &d in bytes {
            count = count.wrapping_mul(10) + (d - b'0') as u32;
        }
        count
    }

    pub fn featurise<R: rand::Rng>(
        &self,
        file: File,
        worker_states: &[ThreadState<R>],
        thread_pool: &ThreadPool,
    ) -> Result<(BoundedMinHeap<u128>, BoundedMaxHeap<u128>), ()> {
        let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
        let mmap = Arc::new(mmap);

        let (tx, rx) = channel::bounded::<Option<(usize, usize)>>(64);

        let io_handle = {
            let mmap = Arc::clone(&mmap);
            let chunk_size = self.config.chunk_size;
            std::thread::spawn(move || {
                let n_chunks = (mmap.len() + chunk_size - 1) / chunk_size;

                for chunk_idx in 0..n_chunks {
                    let raw_start = chunk_idx * chunk_size;
                    let raw_end = min(
                        raw_start + chunk_size + Self::OVERLAP_WINDOW_SIZE,
                        mmap.len(),
                    );

                    let valid_start = Self::find_chunk_start(&mmap[raw_start..], raw_start);
                    let valid_end = Self::find_chunk_end(&mmap[..raw_end], raw_end);

                    tx.send(Some((valid_start, valid_end))).unwrap();
                }
                tx.send(None).unwrap();
            })
        };

        thread_pool.install(|| {
            worker_states.par_iter().for_each(|state| {
                let rx = rx.clone();
                let codec = &self.codec;
                while let Ok(Some((start, end))) = rx.recv() {
                    let chunk = &mmap[start..end];
                    unsafe {
                        let rng = &mut *state.rng.get();
                        let min_heap = &mut *state.min_heap.get();
                        let max_heap = &mut *state.max_heap.get();
                        let buffer = &mut *state.buffer.get();
                        buffer.clear();
                        buffer.extend_from_slice(chunk);
                        Self::featurise_process_chunk(buffer, rng, min_heap, max_heap, codec);
                    }
                }
            });
        });

        io_handle.join().unwrap();

        let mut final_min_heap = BoundedMinHeap::with_capacity(self.config.nlo_results);
        let mut final_max_heap = BoundedMaxHeap::with_capacity(self.config.nhi_results);

        for state in worker_states {
            unsafe {
                let min_heap = &*state.min_heap.get();
                let max_heap = &*state.max_heap.get();

                final_min_heap.extend(min_heap.iter().copied());
                final_max_heap.extend(max_heap.iter().copied());
            }
        }

        Ok((final_min_heap, final_max_heap))
    }

    #[inline(always)]
    fn find_chunk_start(chunk: &[u8], raw_start: usize) -> usize {
        for i in 0..min(Self::OVERLAP_WINDOW_SIZE, chunk.len()) {
            if chunk[i] == b'\n' {
                return raw_start + i + 1;
            }
        }
        raw_start
    }

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

    #[inline(always)]
    fn featurise_process_chunk(
        chunk: &[u8],
        rng: &mut impl rand::Rng,
        min_heap: &mut BoundedMinHeap<u128>,
        max_heap: &mut BoundedMaxHeap<u128>,
        codec: &crate::kmer::Codec<K>,
    ) {
        let range: Uniform<u16> = Uniform::new_inclusive(u16::MIN, u16::MAX);
        let chunk_length = chunk.len();
        let n_max_panes = chunk_length / (K + 2);
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

            let mut pane_length = K + 2;
            for i in pane_length..min(Self::OVERLAP_WINDOW_SIZE, remaining) {
                if chunk[pane_start + i] == b'\n' {
                    pane_length = i;
                    break;
                }
            }

            let kmer_end = pane_start + K;
            let count = Self::parse_count(&chunk[kmer_end + 1..pane_start + pane_length]);

            let encoded = unsafe {
                codec
                    .encode(&chunk[pane_start..kmer_end], count, rng, range)
                    .into_bits()
            };

            let _ = min_heap.push(encoded);
            let _ = max_heap.push(encoded);

            cursor += pane_length + 1;
        }
    }
}
