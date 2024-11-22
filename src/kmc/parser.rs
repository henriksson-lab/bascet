use std::{
    cell::UnsafeCell,
    cmp::min,
    fs::File,
    marker::PhantomData,
    sync::Arc,
};

use crossbeam::channel;
use memmap2::MmapOptions;
use rand::{distributions::Uniform, SeedableRng};
use rayon::{iter::{IntoParallelRefIterator, ParallelIterator}, ThreadPool};

use crate::bounded_heap::{BoundedHeap, BoundedMaxHeap, BoundedMinHeap};
pub struct Config<const K: usize>;

impl<const K: usize> Config<K> {
    pub const SEED: usize = 0;
    pub const THREADS: usize = 8;
    pub const CHUNK_SIZE: usize = 262_144; // 256KB chunks
    pub const OVERLAP_WINDOW_SIZE: usize = K + 1 + 10 + 1;
    pub const CODEC: crate::kmer::Codec<K> = crate::kmer::Codec::new();
    pub const NLO_RESULTS: usize = 50_000;
    pub const NHI_RESULTS: usize = 1_000;
}

pub struct Dump<const K: usize> {
    _marker: PhantomData<Config<K>>,
}

pub struct ThreadState<R: rand::Rng> {
    pub rng: UnsafeCell<R>,
    pub min_heap: UnsafeCell<BoundedMinHeap<u128>>,
    pub max_heap: UnsafeCell<BoundedMaxHeap<u128>>,
    buffer: UnsafeCell<Vec<u8>>,
}

unsafe impl<R: rand::Rng> Send for ThreadState<R> {}
unsafe impl<R: rand::Rng> Sync for ThreadState<R> {}

impl<R: rand::Rng> ThreadState<R> {
    pub fn new(
        rng: R,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
        buffer_size: usize,
    ) -> Self {
        Self {
            rng: UnsafeCell::new(rng),
            min_heap: UnsafeCell::new(BoundedMinHeap::with_capacity(min_heap_capacity)),
            max_heap: UnsafeCell::new(BoundedMaxHeap::with_capacity(max_heap_capacity)),
            buffer: UnsafeCell::new(Vec::with_capacity(buffer_size)),
        }
    }

    pub fn from_seed<S: rand::SeedableRng + rand::RngCore>(
        seed: u64,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
        buffer_size: usize,
    ) -> ThreadState<S> {
        ThreadState::new(
            S::seed_from_u64(seed),
            min_heap_capacity,
            max_heap_capacity,
            buffer_size,
        )
    }

    pub fn from_entropy<S: rand::SeedableRng + rand::RngCore>(
        min_heap_capacity: usize,
        max_heap_capacity: usize,
        buffer_size: usize,
    ) -> ThreadState<S> {
        ThreadState::new(
            S::from_entropy(),
            min_heap_capacity,
            max_heap_capacity,
            buffer_size,
        )
    }
}

impl<const K: usize> Dump<K> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
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
            std::thread::spawn(move || {
                let chunk_size = Config::<K>::CHUNK_SIZE;
                let n_chunks = (mmap.len() + chunk_size - 1) / chunk_size;
                
                for chunk_idx in 0..n_chunks {
                    let raw_start = chunk_idx * chunk_size;
                    let raw_end = min(
                        raw_start + chunk_size + Config::<K>::OVERLAP_WINDOW_SIZE,
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
                while let Ok(Some((start, end))) = rx.recv() {
                    let chunk = &mmap[start..end];
                    unsafe {
                        let rng = &mut *state.rng.get();
                        let min_heap = &mut *state.min_heap.get();
                        let max_heap = &mut *state.max_heap.get();
                        let buffer = &mut *state.buffer.get();
                        buffer.clear();
                        buffer.extend_from_slice(chunk);
                        Self::featurise_process_chunk(buffer, rng, min_heap, max_heap);
                    }
                }
            });
        });
        
        io_handle.join().unwrap();

        let mut final_min_heap = BoundedMinHeap::with_capacity(Config::<K>::NLO_RESULTS);
        let mut final_max_heap = BoundedMaxHeap::with_capacity(Config::<K>::NHI_RESULTS);

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
        for i in 0..min(Config::<K>::OVERLAP_WINDOW_SIZE, chunk.len()) {
            if chunk[i] == b'\n' {
                return raw_start + i + 1;
            }
        }
        raw_start
    }

    #[inline(always)]
    fn find_chunk_end(chunk: &[u8], raw_end: usize) -> usize {
        let window_size = min(Config::<K>::OVERLAP_WINDOW_SIZE, chunk.len());
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
            for i in pane_length..min(Config::<K>::OVERLAP_WINDOW_SIZE, remaining) {
                if chunk[pane_start + i] == b'\n' {
                    pane_length = i;
                    break;
                }
            }

            let kmer_end = pane_start + K;
            let count = Self::parse_count(&chunk[kmer_end + 1..pane_start + pane_length]);

            let encoded = unsafe {
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