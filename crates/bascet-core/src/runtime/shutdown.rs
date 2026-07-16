use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

pub(crate) struct Shutdown {
    closers: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
    fired: AtomicBool,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            closers: Mutex::new(Vec::new()),
            fired: AtomicBool::new(false),
        }
    }

    pub(crate) fn register(&self, closer: Box<dyn Fn() + Send + Sync>) {
        self.closers.lock().push(closer);
    }

    pub(crate) fn trigger(&self) {
        if self.fired.swap(true, Ordering::AcqRel) {
            return;
        }
        for closer in self.closers.lock().iter() {
            closer();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn trigger_runs_each_closer_once() {
        let shutdown = Shutdown::new();
        let a = Arc::new(AtomicU32::new(0));
        let b = Arc::new(AtomicU32::new(0));

        let a2 = Arc::clone(&a);
        shutdown.register(Box::new(move || {
            a2.fetch_add(1, Ordering::Relaxed);
        }));
        let b2 = Arc::clone(&b);
        shutdown.register(Box::new(move || {
            b2.fetch_add(1, Ordering::Relaxed);
        }));

        shutdown.trigger();
        shutdown.trigger();

        assert_eq!(a.load(Ordering::Relaxed), 1);
        assert_eq!(b.load(Ordering::Relaxed), 1);
    }
}
