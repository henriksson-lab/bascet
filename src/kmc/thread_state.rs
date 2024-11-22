use std::cell::UnsafeCell;

use crate::bounded_heap::{BoundedMaxHeap, BoundedMinHeap};

pub struct ThreadState<R: rand::Rng> {
    pub rng: std::cell::UnsafeCell<R>,
    pub min_heap: std::cell::UnsafeCell<crate::bounded_heap::BoundedMinHeap<u128>>,
    pub max_heap: std::cell::UnsafeCell<crate::bounded_heap::BoundedMaxHeap<u128>>,
    pub buffer: std::cell::UnsafeCell<Vec<u8>>,
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

    pub fn reset(&self) {
        unsafe {
            // Clear the min heap
            (*self.min_heap.get()).clear();

            // Clear the max heap
            (*self.max_heap.get()).clear();

            // Clear the buffer
            (*self.buffer.get()).clear();
        }
    }
}
