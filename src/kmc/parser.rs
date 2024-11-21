use std::{
    cell::UnsafeCell, cmp::min, collections::VecDeque, fs::File, marker::PhantomData, path::Path,
    sync::mpsc::sync_channel, thread, u16,
};

use crossbeam::channel;
use memmap2::MmapOptions;
use rand::{distributions::Uniform, rngs::SmallRng};
use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
    ThreadPool,
};

use crate::bounded_heap::{BoundedHeapBehaviour, BoundedMaxHeap, BoundedMinHeap};

pub struct Config<const K: usize>;

impl<const K: usize> Config<K> {
    pub const SEED: usize = 0;
    pub const THREADS: usize = 8;
    pub const CHUNK_SIZE: usize = 65536;
    pub const OVERLAP_WINDOW_SIZE: usize = K + 1 + 10 + 1;
    pub const CODEC: crate::kmer::Codec<K> = crate::kmer::Codec::new();
    pub const NLO_RESULTS: u64 = 50_000;
    pub const NHI_RESULTS: u64 = 1_000;
}

// Then your Dump becomes:
pub struct Dump<const K: usize> {
    _marker: PhantomData<Config<K>>,
}

pub struct ThreadState {
    pub rng: std::cell::UnsafeCell<rand::rngs::SmallRng>,
    pub min_heap: std::cell::UnsafeCell<crate::bounded_heap::BoundedMinHeap<u128>>,
    pub max_heap: std::cell::UnsafeCell<crate::bounded_heap::BoundedMaxHeap<u128>>,
}
//NOTE: unsafe traits need to be implemented manually
unsafe impl Send for ThreadState {}
unsafe impl Sync for ThreadState {}

impl<const K: usize> Dump<K> {
    pub fn new() -> Self {
        return Self {
            _marker: PhantomData,
        };
    }

    //NOTE: thread_states should implement thread states for n - 1 threads!
    pub fn featurise(
        &self,
        file: File,
        worker_states: &[ThreadState],
        thread_pool: &ThreadPool,
    ) -> Result<Vec<u128>, ()> {
        let (sender, receiver) = channel::bounded(32);

        let io_handle = std::thread::spawn(move || {
            let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
            let n_chunks = (mmap.len() + Config::<K>::CHUNK_SIZE - 1) / Config::<K>::CHUNK_SIZE;

            for chunk_idx in 0..n_chunks {
                let raw_start = chunk_idx * Config::<K>::CHUNK_SIZE;
                let raw_end = min(
                    raw_start + Config::<K>::CHUNK_SIZE + Config::<K>::OVERLAP_WINDOW_SIZE,
                    mmap.len(),
                );
                let raw_data = &mmap[raw_start..raw_end];

                let mut valid_start = raw_start;
                for i in 0..min(Config::<K>::OVERLAP_WINDOW_SIZE, raw_data.len()) {
                    if raw_data[i] == b'\n' {
                        valid_start = raw_start + i + 1;
                        break;
                    }
                }

                let mut valid_end = raw_end;
                let end_search_start =
                    valid_end - min(Config::<K>::OVERLAP_WINDOW_SIZE, valid_end - raw_start);
                for i in (end_search_start..valid_end).rev() {
                    if raw_data[i - raw_start] == b'\n' {
                        valid_end = i + 1;
                        break;
                    }
                }

                let valid_chunk = mmap[valid_start..valid_end].to_vec();
                sender.send(Some(valid_chunk)).unwrap();
            }
            sender.send(None).unwrap();
        });

        worker_states.par_iter().for_each(|state| {
            let receiver = receiver.clone();
            while let Ok(Some(chunk)) = receiver.recv() {
                let rng = unsafe { &mut *state.rng.get() };
                let min_heap = unsafe { &mut *state.min_heap.get() };
                let max_heap = unsafe { &mut *state.max_heap.get() };
                Self::featurise_process_chunk(&chunk, rng, min_heap, max_heap);
            }
        });

        io_handle.join().unwrap();

        // Collect results from all heaps
        let mut results = Vec::new();
        for state in worker_states {
            let min_heap = unsafe { &*state.min_heap.get() };
            let max_heap = unsafe { &*state.max_heap.get() };
            results.extend(min_heap.iter());
            results.extend(max_heap.iter());
        }
        Ok(results)
    }

    fn featurise_process_chunk(
        chunk: &[u8],
        rng: &mut impl rand::Rng,
        min_heap: &mut BoundedMinHeap<u128>,
        max_heap: &mut BoundedMaxHeap<u128>,
    ) {
        let range: Uniform<u16> = Uniform::new_inclusive(u16::MIN, u16::MAX);
        let chunk_length = chunk.len();
        let n_max_lines = chunk_length / (K + 1 + 1); // minimum line length: KMER + tab + digit
        let mut cursor = 0;
    
        for _ in 0..n_max_lines {
            let line_start = cursor;
            if line_start >= chunk_length {
                break;
            }
    
            // Minimum line size check
            if line_start + K + 2 > chunk_length {
                break;
            }
    
            // Verify tab after KMER
            if chunk[line_start + K] != b'\t' {
                break;
            }
    
            // Find end of line (next newline)
            let count_start = line_start + K + 1;
            let mut line_length = K + 1 + 1; // minimum length
            for i in count_start..min(line_start + Config::<K>::OVERLAP_WINDOW_SIZE, chunk_length) {
                if chunk[i] == b'\n' {
                    line_length = i - line_start;
                    break;
                }
            }
    
            // Parse count
            let mut count = 0;
            for d in &chunk[count_start..line_start + line_length] {
                count = count * 10 + (d - b'0') as u32;
            }
    
            let encoded: u128 = unsafe {
                Config::<K>::CODEC
                    .encode(&chunk[line_start..line_start + K], count, rng, range)
                    .into_bits()
            };
    
            let _ = min_heap.push(encoded);
            let _ = max_heap.push(encoded);
    
            cursor = line_start + line_length + 1;
        }
    }
}
