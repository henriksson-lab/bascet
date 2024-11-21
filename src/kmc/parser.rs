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
    pub fn featurise(&self, file: File, worker_states: &[ThreadState], thread_pool: &ThreadPool) -> Result<Vec<u128>, ()> {
        let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
        let mmap = std::sync::Arc::new(mmap);
        let (tx, rx) = channel::bounded::<Option<(usize, usize)>>(32);
    
        let io_handle = {
            let mmap = mmap.clone();
            std::thread::spawn(move || {
                let n_chunks = (mmap.len() + Config::<K>::CHUNK_SIZE - 1) / Config::<K>::CHUNK_SIZE;
    
                for chunk_idx in 0..n_chunks {
                    let raw_start = chunk_idx * Config::<K>::CHUNK_SIZE;
                    let raw_end = min(
                        raw_start + Config::<K>::CHUNK_SIZE + Config::<K>::OVERLAP_WINDOW_SIZE,
                        mmap.len(),
                    );
                    
                    let mut valid_start = raw_start;
                    for i in 0..min(Config::<K>::OVERLAP_WINDOW_SIZE, raw_end - raw_start) {
                        if mmap[raw_start + i] == b'\n' {
                            valid_start = raw_start + i + 1;
                            break;
                        }
                    }
    
                    let mut valid_end = raw_end;
                    let end_search_start = 
                        valid_end - min(Config::<K>::OVERLAP_WINDOW_SIZE, valid_end - raw_start);
                    for i in (end_search_start..valid_end).rev() {
                        if mmap[i] == b'\n' {
                            valid_end = i + 1;
                            break;
                        }
                    }
    
                    tx.send(Some((valid_start, valid_end))).unwrap();
                }
                tx.send(None).unwrap();
            })
        };
    
        thread_pool.install(|| {
            worker_states.par_iter().for_each(|state| {
                let receiver = rx.clone();
                let mmap = mmap.clone();
                while let Ok(Some((start, end))) = receiver.recv() {
                    let chunk = &mmap[start..end];
                    let rng = unsafe { &mut *state.rng.get() };
                    let min_heap = unsafe { &mut *state.min_heap.get() };
                    let max_heap = unsafe { &mut *state.max_heap.get() };
                    Self::featurise_process_chunk(chunk, rng, min_heap, max_heap);
                }
            });
        });
    
        io_handle.join().unwrap();
    
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
        let n_max_panes = chunk_length / (K + 1 + 1);
        let mut cursor = 0;
    
        for _ in 0..n_max_panes {
            let pane_start = cursor;
            if pane_start >= chunk_length {
                break;
            }
    
            let pane_remainder = chunk_length.saturating_sub(pane_start);
            let min_pane_length = K + 1 + 1;
            let max_pane_length = min(Config::<K>::OVERLAP_WINDOW_SIZE, chunk_length - pane_start);
    
            if min_pane_length > pane_remainder || max_pane_length < K {
                break;
            }
    
            let mut pane_length = min_pane_length;
            for o in min_pane_length..max_pane_length {
                if chunk[pane_start + o] == b'\n' {
                    pane_length = o;
                    break;
                }
            }
    
            let pane_end = pane_start + pane_length;
            let kmer_end = pane_start + K;
            let count_start = kmer_end + 1;
    
            let mut count = 0;
            for d in &chunk[count_start..pane_end] {
                count = count * 10 + (d - b'0') as u32;
            }
    
            let encoded: u128 = unsafe {
                Config::<K>::CODEC
                    .encode(&chunk[pane_start..kmer_end], count, rng, range)
                    .into_bits()
            };
            
            

            let _ = min_heap.push(encoded);
            let _ = max_heap.push(encoded);
    
            cursor += pane_length + 1;
        }
    }
}
