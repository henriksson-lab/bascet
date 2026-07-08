use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use event_listener::{Event, Listener};

pub struct Shutdown {
    flag: Arc<AtomicBool>,
    event: Arc<Event>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            event: Arc::new(Event::new()),
        }
    }

    pub fn trigger(&self) {
        self.flag.store(true, Ordering::Release);
        self.event.notify(usize::MAX);
    }

    pub fn is_triggered(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    pub fn wait(&self) {
        if self.is_triggered() {
            return;
        }
        let listener = self.event.listen();
        if !self.is_triggered() {
            listener.wait();
        }
    }

    #[allow(dead_code)]
    pub async fn wait_async(&self) {
        if self.is_triggered() {
            return;
        }
        let listener = self.event.listen();
        if !self.is_triggered() {
            listener.await;
        }
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        Self {
            flag: self.flag.clone(),
            event: self.event.clone(),
        }
    }
}
