pub(crate) mod allocation;
pub(crate) mod exception;
pub(crate) mod machine;
pub(crate) mod shutdown;
pub mod tier;
pub(crate) mod workers;

pub use allocation::Pinning;
pub use tier::Tier;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bon::bon;
use hwlocality::Topology;
use parking_lot::Mutex;

use crate::apply::Error;
use crate::pipeline::connect::{Assemble, Build};
use crate::runner::Runner;
use crate::runtime::allocation::Allocation;
use crate::runtime::exception::Exception;
use crate::runtime::machine::Machine;
use crate::runtime::shutdown::Shutdown;
use crate::runtime::workers::Workers;
use crate::schedule::preempt::Preempt;
use crate::schedule::{Epoch, Schedule, Scheduler};
use crate::set::Set;

pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
    topology: Option<Arc<Topology>>,
    allocation: Allocation,
}

pub(crate) struct RuntimeInner {
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
    pub fn new(
        with_total: Option<usize>,
        with_burn: Option<usize>,
        with_jobs: Option<usize>,
        with_tasks: Option<usize>,
        with_pinning: Option<Pinning>,
    ) -> Self {
        let topology = Topology::new().ok().map(Arc::new);
        let machine = match &topology {
            Some(topology) => Machine::probe(&*topology),
            None => {
                Exception::HWUnavailableTopology.log();
                Machine::fallback()
            }
        };
        let allocation = Allocation::plan(
            &machine,
            with_total,
            with_burn,
            with_jobs,
            with_tasks,
            with_pinning.unwrap_or_default(),
        );
        let topology = if machine.binds {
            topology
        } else {
            Exception::HWUnavailableAffinity.log();
            None
        };

        Self {
            inner: Arc::new(RuntimeInner {
                shutdown: Shutdown::new(),
                error: Mutex::new(None),
            }),
            topology,
            allocation,
        }
    }

    pub fn pipeline<W: Set>(self, pipeline: impl Assemble<W>) -> Runner {
        let Runtime {
            inner,
            topology,
            allocation,
        } = self;
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
            epoch: Epoch::new(),
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
                drop(scheduler);
                schedule.epoch.advance();
            }
        }));
        let weak = Arc::downgrade(&inner);
        let worker_schedule = Arc::clone(&schedule);
        let workers = Workers::spawn(topology, allocation, move |tier| {
            let schedule = Arc::clone(&worker_schedule);
            let runtime = weak.clone();
            move || schedule.participate(&runtime, tier)
        });
        Runner {
            runtime: inner,
            schedule,
            sink,
            workers,
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime::builder().build()
    }
}
