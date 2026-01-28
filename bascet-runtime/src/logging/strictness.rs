use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

#[derive(Clone, Copy, Debug)]
pub enum LogStrictness {
    Ignore,
    Lenient(u64),
    Strict,
}

pub struct LogStrictnessLayer;

impl LogStrictnessLayer {
    fn count() -> &'static AtomicU64 {
        static COUNT: AtomicU64 = AtomicU64::new(0);
        &COUNT
    }

    fn limit() -> &'static AtomicU64 {
        static LIMIT: AtomicU64 = AtomicU64::new(u64::MAX);
        &LIMIT
    }

    fn panicking() -> &'static AtomicBool {
        static PANICKING: AtomicBool = AtomicBool::new(false);
        &PANICKING
    }

    pub fn set(strictness: LogStrictness) {
        Self::count().store(0, Ordering::Relaxed);
        match strictness {
            LogStrictness::Ignore => Self::limit().store(u64::MAX, Ordering::Relaxed),
            LogStrictness::Strict => Self::limit().store(0, Ordering::Relaxed),
            LogStrictness::Lenient(n) => Self::limit().store(n, Ordering::Relaxed),
        }
    }

    pub fn panic() {
        Self::panicking().store(true, Ordering::Relaxed);
    }
}

impl<S: Subscriber> Layer<S> for LogStrictnessLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if Self::panicking().load(Ordering::Relaxed) == true {
            return;
        }

        if *event.metadata().level() <= Level::WARN {
            let count = Self::count().fetch_add(1, Ordering::Relaxed);
            let limit = Self::limit().load(Ordering::Relaxed);

            if count >= limit {
                panic!(
                    "Warning limit exceeded ({}/{}): {} in {}",
                    count + 1,
                    limit,
                    event.metadata().level(),
                    event.metadata().target()
                );
            }
        }
    }
}
