use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::Thread;

pub(crate) struct Waiters {
    parked: Box<[AtomicU64]>,
    threads: Box<[OnceLock<Thread>]>,
    next: AtomicUsize,
    join: OnceLock<Thread>,
}

impl Waiters {
    pub(crate) fn new(workers: usize) -> Self {
        Self {
            parked: (0..workers.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
            threads: (0..workers).map(|_| OnceLock::new()).collect(),
            next: AtomicUsize::new(0),
            join: OnceLock::new(),
        }
    }

    pub(crate) fn register(&self) -> usize {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let _ = self.threads[id].set(std::thread::current());
        id
    }

    pub(crate) fn park(&self, id: usize, ready: impl Fn() -> bool) {
        let mask = 1u64 << (id % 64);
        let slot = &self.parked[id / 64];
        slot.fetch_or(mask, Ordering::SeqCst);
        if ready() {
            slot.fetch_and(!mask, Ordering::SeqCst);
            return;
        }
        while slot.load(Ordering::SeqCst) & mask != 0 {
            std::thread::park();
        }
    }

    pub(crate) fn unpark_one(&self) {
        for (word, slot) in self.parked.iter().enumerate() {
            let mut bits = slot.load(Ordering::SeqCst);
            while bits != 0 {
                let lane = bits.trailing_zeros() as usize;
                match slot.compare_exchange_weak(
                    bits,
                    bits ^ (1 << lane),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        if let Some(thread) = self.threads[word * 64 + lane].get() {
                            thread.unpark();
                        }
                        return;
                    }
                    Err(seen) => bits = seen,
                }
            }
        }
    }

    pub(crate) fn unpark_all(&self) {
        for slot in self.parked.iter() {
            slot.store(0, Ordering::SeqCst);
        }
        for thread in self.threads.iter().filter_map(OnceLock::get) {
            thread.unpark();
        }
    }

    pub(crate) fn register_join(&self) {
        let _ = self.join.get_or_init(std::thread::current);
    }

    pub(crate) fn unpark_join(&self) {
        if let Some(thread) = self.join.get() {
            thread.unpark();
        }
    }
}
