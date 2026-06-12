use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use core_affinity::CoreId;
use event_listener::Event;

use crate::pipeline::scheduler::Scheduler;

use super::builder::PipelineBuilder;
use super::shutdown::Shutdown;

#[derive(Default)]
pub struct Pipeline {
    pub(crate) inner_wires: Vec<Wire>,
}

pub(crate) struct Wire;

#[derive(Clone)]
pub(crate) struct Runtime {
    pub(crate) inner_task_runtime: Arc<tokio::runtime::Runtime>,
    pub(crate) inner_trycheck_stalled: Arc<Event>,
}

#[derive(Clone)]
pub struct Metrics {
    pub countof_processed: Arc<AtomicU64>,
    pub countof_sourced: Arc<AtomicU64>,
    pub countof_active: Arc<AtomicUsize>,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            countof_processed: Arc::new(AtomicU64::new(0)),
            countof_sourced: Arc::new(AtomicU64::new(0)),
            countof_active: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn any_active(&self) -> bool {
        self.countof_active.load(Ordering::Relaxed) > 0
    }
}

pub struct Runner {
    pub(crate) inner_pipeline: Pipeline,
    pub(crate) inner_scheduler: Scheduler,
    pub(crate) inner_runtime: Runtime,
    pub(crate) inner_shutdown: Shutdown,
    pub(crate) inner_metrics: Metrics,
}

impl Runner {
    pub fn builder() -> PipelineBuilder<(), (), (), (), ()> {
        PipelineBuilder::new()
    }

    pub(crate) fn new(
        inner_pipeline: Pipeline,
        inner_scheduler: Scheduler,
        inner_runtime: Runtime,
        inner_shutdown: Shutdown,
        inner_metrics: Metrics,
    ) -> Self {
        Self {
            inner_pipeline,
            inner_scheduler,
            inner_runtime,
            inner_shutdown,
            inner_metrics,
        }
    }

    pub fn shutdown(&self) {
        self.inner_shutdown.trigger();
    }

    pub fn join(&self) {
        self.inner_shutdown.wait();
    }

    pub fn any_active(&self) -> bool {
        self.inner_metrics.any_active()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner_metrics
    }
}

pub(crate) fn make_runtime() -> (Runtime, Vec<CoreId>, usize, usize) {
    let all_cores = core_affinity::get_core_ids().unwrap_or_default();
    let p = all_cores.len().max(1);
    let reserved = (p / 8).max(4).min(p.saturating_sub(1));
    let burn_count = p - reserved;
    let job_slots = reserved * 2;
    let task_slots = reserved * 512;
    let tokio_workers = (reserved / 2).max(2);
    let burn_cores: Vec<CoreId> = all_cores.into_iter().take(burn_count).collect();

    (
        Runtime {
            inner_task_runtime: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .max_blocking_threads(16)
                    .worker_threads(tokio_workers)
                    .enable_time()
                    .build()
                    .unwrap(),
            ),
            inner_trycheck_stalled: Arc::new(Event::new()),
        },
        burn_cores,
        job_slots,
        task_slots,
    )
}
