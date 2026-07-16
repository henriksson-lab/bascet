pub mod pool;
pub(crate) mod shutdown;
pub mod tier;

pub use pool::{Job, Pool};
pub use tier::Tier;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bon::bon;
use parking_lot::Mutex;

use crate::apply::Error;
use crate::pipeline::connect::{Assemble, Build};
use crate::runner::Runner;
use crate::runtime::shutdown::Shutdown;
use crate::schedule::preempt::Preempt;
use crate::schedule::{Schedule, Scheduler};
use crate::set::Set;

pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    pub(crate) pool: Pool,
    pub(crate) shutdown: Shutdown,
    pub(crate) error: Mutex<Option<Error>>,
}

impl RuntimeInner {
    pub(crate) fn record_error(&self, error: Error) {
        let mut slot = self.error.lock();
        if slot.is_none() {
            tracing::error!("pipeline layer errored");
            *slot = Some(error);
        }
    }

    pub(crate) fn take_error(&self) -> Option<Error> {
        self.error.lock().take()
    }
}

#[bon]
impl Runtime {
    #[builder]
    pub fn new(burn: Option<usize>, jobs: Option<usize>, tasks: Option<usize>) -> Self {
        let cores = core_affinity::get_core_ids()
            .map(|c| c.len())
            .unwrap_or(1)
            .max(1);
        let reserved = (cores / 8).max(2).min(cores.saturating_sub(1).max(1));
        let burn = burn.unwrap_or(cores - reserved);
        let jobs = jobs.unwrap_or(reserved * 2);
        let tasks = tasks.unwrap_or(reserved * 512);
        Self {
            inner: Arc::new(RuntimeInner {
                pool: Pool::spawn(burn, jobs, tasks),
                shutdown: Shutdown::new(),
                error: Mutex::new(None),
            }),
        }
    }

    pub fn pipeline<W: Set>(self, pipeline: impl Assemble<W>) -> Runner {
        let inner = self.inner;
        let mut build = Build {
            runtime: Arc::clone(&inner),
            layers: Vec::new(),
            upstream: Vec::new(),
        };
        let sink = pipeline.assemble(&mut build);
        let schedule = Arc::new(Schedule {
            scheduler: Mutex::new(Scheduler {
                layers: build.layers.into_boxed_slice(),
                upstream: build
                    .upstream
                    .into_iter()
                    .map(Vec::into_boxed_slice)
                    .collect(),
                idle: Vec::new(),
                waiter: None,
            }),
        });
        let closer = Arc::downgrade(&schedule);
        inner.shutdown.register(Box::new(move || {
            if let Some(schedule) = closer.upgrade() {
                let mut scheduler = schedule.scheduler.lock();
                for layer in scheduler.layers.iter().flatten() {
                    layer.preempt.store(Preempt::Halt as u8, Ordering::Relaxed);
                }
                for waker in scheduler.idle.drain(..) {
                    waker.wake();
                }
            }
        }));
        let weak = Arc::downgrade(&inner);
        let job_schedule = Arc::clone(&schedule);
        inner.pool.broadcast(move |tier| {
            let schedule = Arc::clone(&job_schedule);
            let runtime = weak.clone();
            Box::new(move || schedule.participate(&runtime, tier))
        });
        Runner {
            runtime: inner,
            schedule,
            sink,
        }
    }
}
