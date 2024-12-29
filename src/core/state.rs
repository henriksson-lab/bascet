use std::sync::Mutex;

use rand::rngs::SmallRng;
use rand::SeedableRng;

use crate::utils::BoundedMaxHeap;
use crate::utils::BoundedMinHeap;

pub struct Threading {
    pub rng: Mutex<SmallRng>,
    pub buffer: Mutex<Vec<u8>>,
    pub min_heap: Mutex<BoundedMinHeap<u128>>,
    pub max_heap: Mutex<BoundedMaxHeap<u128>>,
}

unsafe impl Send for Threading {}
unsafe impl Sync for Threading {}
impl Threading {
    pub fn new(
        rng: SmallRng,
        buffer_size: usize,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
    ) -> Self {
        Self {
            rng: Mutex::new(rng),
            buffer: Mutex::new(Vec::with_capacity(buffer_size)),
            min_heap: Mutex::new(BoundedMinHeap::with_capacity(min_heap_capacity)),
            max_heap: Mutex::new(BoundedMaxHeap::with_capacity(max_heap_capacity)),
        }
    }

    pub fn from_seed(
        seed: u64,
        buffer_size: usize,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
    ) -> Threading {
        Threading::new(
            SmallRng::seed_from_u64(seed),
            buffer_size,
            min_heap_capacity,
            max_heap_capacity,
        )
    }

    pub fn reset(&mut self) {
        self.buffer.lock().unwrap().clear();
        self.min_heap.lock().unwrap().clear();
        self.max_heap.lock().unwrap().clear();
    }
}
