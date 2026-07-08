use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use event_listener::Listener;
use tracing::{debug, error};

use super::consts::{STALL_HARD, STALL_WARN};
use super::runtime::Runtime;
use super::shutdown::Shutdown;

pub(crate) struct Watchdog;

impl Watchdog {
    pub(crate) fn spawn(runtime: Runtime) {
        std::thread::spawn(move || {
            loop {
                if !Self::wait(&runtime) {
                    break;
                }

                let metrics = runtime.metrics();
                let snap = metrics.shared_processed.load(Ordering::Relaxed);
                if !Self::sleep(runtime.shutdown(), STALL_WARN) {
                    break;
                }

                let metrics = runtime.metrics();
                if metrics.shared_processed.load(Ordering::Relaxed) == snap
                    && metrics.any_active()
                {
                    debug!("pipeline stall: no throughput in {:?}", STALL_WARN);
                    if !Self::sleep(runtime.shutdown(), STALL_HARD - STALL_WARN) {
                        break;
                    }
                    let metrics = runtime.metrics();
                    if metrics.shared_processed.load(Ordering::Relaxed) == snap
                        && metrics.shared_sourced.load(Ordering::Relaxed)
                            > metrics.shared_processed.load(Ordering::Relaxed)
                        && metrics.any_active()
                    {
                        error!("pipeline deadlock: no throughput in {:?}", STALL_HARD);
                    }
                }
            }
        });
    }

    fn wait(runtime: &Runtime) -> bool {
        while !runtime.shutdown().is_triggered() {
            let listener = runtime.watchdog().listen();
            if listener.wait_timeout(Duration::from_millis(100)).is_some() {
                return true;
            }
        }
        false
    }

    fn sleep(shutdown: &Shutdown, duration: Duration) -> bool {
        let deadline = Instant::now() + duration;
        while !shutdown.is_triggered() {
            let now = Instant::now();
            if now >= deadline {
                return true;
            }
            std::thread::park_timeout((deadline - now).min(Duration::from_millis(100)));
        }
        false
    }
}
