//! Shared backpressure primitives for the pipelined commands. Both the BWA stock-driver and
//! the BAM-sort pipelines need to bound (a) total bytes of records held across in-flight
//! chunks and (b) the count of work items queued between stages — to keep memory bounded and
//! avoid deadlock when later stages stall.
//!
//! Pattern lifted from `getraw`'s in-module limiters; deduplicated here so command modules
//! can share the implementation.

use std::sync::{Arc, Condvar, Mutex};

/// Bytes-based semaphore. `acquire(N)` blocks until `N` bytes are free (or `N >= cap`, in
/// which case it caps at `cap` and proceeds when no other permits are outstanding). Permit
/// releases on drop.
pub struct ReadMemoryLimiter {
    cap: usize,
    used: Mutex<usize>,
    available: Condvar,
}

impl ReadMemoryLimiter {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            used: Mutex::new(0),
            available: Condvar::new(),
        }
    }

    pub fn acquire(self: &Arc<Self>, bytes: usize) -> ReadMemoryPermit {
        if bytes == 0 {
            return ReadMemoryPermit {
                bytes,
                limiter: Arc::clone(self),
            };
        }
        let charge = bytes.min(self.cap);
        let mut used = self.used.lock().unwrap();
        while *used + charge > self.cap {
            used = self.available.wait(used).unwrap();
        }
        *used += charge;
        ReadMemoryPermit {
            bytes: charge,
            limiter: Arc::clone(self),
        }
    }
}

pub struct ReadMemoryPermit {
    bytes: usize,
    limiter: Arc<ReadMemoryLimiter>,
}

impl Drop for ReadMemoryPermit {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let mut used = self.limiter.used.lock().unwrap();
        *used = used.saturating_sub(self.bytes);
        self.limiter.available.notify_all();
    }
}

/// Count-based semaphore. Used to cap the number of work items in flight (e.g. compressor
/// queue depth), bounding the writer's reorder buffer and preventing producers from
/// outrunning consumers.
pub struct InFlightLimiter {
    available: Mutex<usize>,
    ready: Condvar,
}

impl InFlightLimiter {
    pub fn new(cap: usize) -> Self {
        Self {
            available: Mutex::new(cap.max(1)),
            ready: Condvar::new(),
        }
    }

    pub fn acquire(self: &Arc<Self>) -> InFlightPermit {
        let mut available = self.available.lock().unwrap();
        while *available == 0 {
            available = self.ready.wait(available).unwrap();
        }
        *available -= 1;
        InFlightPermit {
            limiter: Arc::clone(self),
        }
    }
}

pub struct InFlightPermit {
    limiter: Arc<InFlightLimiter>,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        let mut available = self.limiter.available.lock().unwrap();
        *available += 1;
        self.limiter.ready.notify_one();
    }
}
