pub struct ThreadState<R: rand::Rng> {
    pub rng: std::cell::UnsafeCell<R>,
    pub buffer: std::cell::UnsafeCell<Vec<u8>>,
    pub min_heap: std::cell::UnsafeCell<crate::utils::BoundedMinHeap<u128>>,
    pub max_heap: std::cell::UnsafeCell<crate::utils::BoundedMaxHeap<u128>>,
}

unsafe impl<R: rand::Rng> Send for ThreadState<R> {}
unsafe impl<R: rand::Rng> Sync for ThreadState<R> {}

pub type DefaultThreadState = ThreadState<rand::rngs::SmallRng>;
impl<R: rand::Rng> ThreadState<R> {
    pub fn new(
        rng: R,
        buffer_size: usize,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
    ) -> Self {
        Self {
            rng: std::cell::UnsafeCell::new(rng),
            buffer: std::cell::UnsafeCell::new(Vec::with_capacity(buffer_size)),
            min_heap: std::cell::UnsafeCell::new(crate::utils::BoundedMinHeap::with_capacity(
                min_heap_capacity,
            )),
            max_heap: std::cell::UnsafeCell::new(crate::utils::BoundedMaxHeap::with_capacity(
                max_heap_capacity,
            )),
        }
    }

    pub fn from_seed<S: rand::SeedableRng + rand::RngCore>(
        seed: u64,
        buffer_size: usize,
        min_heap_capacity: usize,
        max_heap_capacity: usize,
    ) -> ThreadState<S> {
        ThreadState::new(
            S::seed_from_u64(seed),
            buffer_size,
            min_heap_capacity,
            max_heap_capacity,
        )
    }

    pub fn reset(&self) {
        unsafe {
            (*self.buffer.get()).clear();
            (*self.min_heap.get()).clear();
            (*self.max_heap.get()).clear();
        }
    }
}
