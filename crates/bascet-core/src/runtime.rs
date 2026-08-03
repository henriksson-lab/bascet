pub(crate) mod allocation;
pub mod exception;
pub(crate) mod machine;
pub mod tier;
pub(crate) mod workers;

pub use allocation::Pinning;
pub use tier::Tier;

use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bon::bon;
use hwlocality::Topology;
use parking_lot::Mutex;

use crate::apply::Error;
use crate::exception::Raise;
use crate::pipeline::connect::{Assemble, Build};
use crate::runner::Runner;
use crate::runtime::allocation::Allocation;
use crate::runtime::exception::Exception;
use crate::runtime::machine::Machine;
use crate::runtime::workers::Workers;
use crate::schedule::layer::Layer;
use crate::schedule::wait::Waiters;
use crate::schedule::{Schedule, Worker};

pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
    topology: Option<Arc<Topology>>,
}

pub(crate) struct RuntimeInner {
    pub(crate) error: Mutex<Option<Error>>,
    pub(crate) allocation: Allocation,
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
                error: Mutex::new(None),
                allocation,
            }),
            topology,
        })
    }

    pub fn pipeline(self, pipeline: impl Assemble) -> Runner {
        let Runtime { inner, topology } = self;
        let mut build = Build {
            runtime: Arc::clone(&inner),
            layers: Vec::new(),
            upstream: Vec::new(),
        };
        let sink = pipeline.assemble(&mut build);

        let Build {
            layers, upstream, ..
        } = build;
        let layers: Box<[Arc<Layer>]> = layers
            .into_iter()
            .map(|layer| match layer {
                Some(layer) => Arc::new(layer),
                None => panic!("layer slot reserved but never registered"),
            })
            .collect();
        for (consumer, producers) in upstream.iter().enumerate() {
            let up: Box<[Arc<Layer>]> = producers.iter().map(|&p| Arc::clone(&layers[p])).collect();
            let _ = layers[consumer].build_upstream.set(up);
            for &producer in producers.iter() {
                let _ = layers[producer]
                    .build_downstream
                    .set(Arc::clone(&layers[consumer]));
            }
        }
        let count = layers.len();
        let sink = Arc::clone(&layers[sink]);
        let schedule = Arc::new(Schedule {
            layers,
            live: AtomicU64::new(count as u64),
            waiters: Waiters::new(inner.allocation.workers()),
            runtime: Arc::clone(&inner),
        });
        let worker_schedule = Arc::clone(&schedule);
        let workers = Workers::spawn(topology, &inner.allocation, move |tier| {
            let schedule = Arc::clone(&worker_schedule);
            move || {
                let mut worker = Worker::new(tier);
                worker.run(&schedule);
            }
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
