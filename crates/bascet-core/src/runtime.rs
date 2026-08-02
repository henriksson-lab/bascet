pub(crate) mod allocation;
pub mod exception;
pub(crate) mod machine;
pub(crate) mod shutdown;
pub mod tier;
pub(crate) mod workers;

pub use allocation::Pinning;
pub use tier::Tier;

use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bon::bon;
use crossbeam_queue::ArrayQueue;
use hwlocality::Topology;
use parking_lot::Mutex;

use crate::apply::Error;
use crate::exception::Raise;
use crate::pipeline::connect::{Assemble, Build};
use crate::runner::Runner;
use crate::runtime::allocation::Allocation;
use crate::runtime::exception::Exception;
use crate::runtime::machine::Machine;
use crate::runtime::shutdown::Shutdown;
use crate::runtime::workers::Workers;
use crate::schedule::layer::{Layer, LayerState};
use crate::schedule::preempt::Preempt;
use crate::schedule::wait::Waiters;
use crate::schedule::{Schedule, Scheduler};
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
            if let Err(error) = error.annotate().raise() {
                *slot = Some(error);
            }
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
        with_total: Option<NonZero<u64>>,
        with_burn: Option<u64>,
        with_jobs: Option<u64>,
        with_tasks: Option<u64>,
        with_pinning: Option<Pinning>,
    ) -> Result<Self, crate::Exception> {
        let topology = Topology::new().ok().map(Arc::new);
        let machine = match &topology {
            Some(topology) => Machine::probe(&*topology)?,
            None => {
                Exception::HWUnavailableTopology
                    .annotate()
                    .fixed("falling back to available parallelism")
                    .help(
                        "placement ignores cache and numa boundaries; check that hwloc can read the machine",
                    )
                    .raise()?;
                Machine::fallback()
            }
        };
        let allocation = Allocation::plan(
            &machine,
            with_total,
            with_jobs,
            with_tasks,
            with_burn,
            with_pinning.unwrap_or_default(),
        )?;
        let topology = if machine.binds {
            topology
        } else {
            Exception::HWUnavailableAffinity
                .annotate()
                .fixed("dropped the topology; workers run unpinned")
                .help("the platform exposes no thread binding, so every worker migrates freely")
                .raise()?;
            None
        };

        Ok(Self {
            inner: Arc::new(RuntimeInner {
                shutdown: Shutdown::new(),
                error: Mutex::new(None),
            }),
            topology,
            allocation,
        })
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
        let workers = allocation.burn.len() + allocation.jobs as usize + allocation.tasks as usize;
        let layers: Box<[Arc<Layer>]> = build
            .layers
            .into_iter()
            .map(|layer| match layer {
                Some(layer) => Arc::new(layer),
                None => panic!("layer slot reserved but never registered"),
            })
            .collect();
        let upstream: Box<[Box<[usize]>]> = build
            .upstream
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect();
        for (consumer, producers) in upstream.iter().enumerate() {
            for &producer in producers.iter() {
                let _ = layers[producer].downstream.set(Arc::clone(&layers[consumer]));
            }
        }
        let count = layers.len();
        let schedule = Arc::new(Schedule {
            scheduler: Scheduler {
                layers,
                upstream,
                suspended: (0..count).map(|_| ArrayQueue::new(workers)).collect(),
                live: AtomicU64::new(count as u64),
            },
            waiters: Waiters::new(workers),
        });
        let closer = Arc::downgrade(&schedule);
        inner.shutdown.register(Box::new(move || {
            if let Some(schedule) = closer.upgrade() {
                for layer in schedule.scheduler.layers.iter() {
                    layer.preempt.store(Preempt::Halt as u8, Ordering::Relaxed);
                    layer.mark(LayerState::Runnable);
                }
                schedule.wake_all();
                schedule.wake_join();
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
        Runtime::builder().build().unwrap()
    }
}
