use parking_lot::Mutex;

pub(crate) struct Shutdown {
    closers: Mutex<Option<Vec<Box<dyn FnOnce() + Send>>>>,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            closers: Mutex::new(Some(Vec::new())),
        }
    }

    pub(crate) fn register(&self, closer: Box<dyn FnOnce() + Send>) {
        if let Some(closers) = self.closers.lock().as_mut() {
            closers.push(closer);
        }
    }

    pub(crate) fn trigger(&self) {
        let closers = self.closers.lock().take();
        for closer in closers.into_iter().flatten() {
            closer();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

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
