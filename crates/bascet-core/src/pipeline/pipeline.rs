use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::scheduler::Petitioner;

use super::builder::PipelineBuilder;
use super::runtime::Runtime;
use super::shutdown::Shutdown;

#[derive(Default)]
pub struct Pipeline {
    #[allow(dead_code)]
    pub(crate) inner_wires: Vec<Wire>,
}

pub(crate) struct Wire;

#[derive(Clone)]
pub struct Metrics {
    pub shared_processed: Arc<AtomicU64>,
    pub shared_sourced: Arc<AtomicU64>,
    pub shared_active: Arc<AtomicU64>,
    pub shared_idle: Arc<AtomicU64>,
    pub shared_backpressure: Arc<AtomicU64>,
    pub local_processed: u64,
    pub local_sourced: u64,
    pub local_idle: u64,
    pub local_backpressure: u64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            shared_processed: Arc::new(AtomicU64::new(0)),
            shared_sourced: Arc::new(AtomicU64::new(0)),
            shared_active: Arc::new(AtomicU64::new(0)),
            shared_idle: Arc::new(AtomicU64::new(0)),
            shared_backpressure: Arc::new(AtomicU64::new(0)),
            local_processed: 0,
            local_sourced: 0,
            local_idle: 0,
            local_backpressure: 0,
        }
    }

    #[inline(always)]
    pub(crate) fn add_processed(&mut self, n: u64) {
        self.local_processed += n;
    }

    #[inline(always)]
    pub(crate) fn add_sourced(&mut self, n: u64) {
        self.local_sourced += n;
    }

    #[inline(always)]
    pub(crate) fn add_idle(&mut self, n: u64) {
        self.local_idle += n;
    }

    #[inline(always)]
    pub(crate) fn add_backpressure(&mut self, n: u64) {
        self.local_backpressure += n;
    }

    pub(crate) fn flush_processed(&mut self) {
        Self::flush(&self.shared_processed, &mut self.local_processed);
    }

    pub(crate) fn flush_sourced(&mut self) {
        Self::flush(&self.shared_sourced, &mut self.local_sourced);
    }

    pub(crate) fn flush_idle(&mut self) {
        Self::flush(&self.shared_idle, &mut self.local_idle);
    }

    pub(crate) fn flush_backpressure(&mut self) {
        Self::flush(&self.shared_backpressure, &mut self.local_backpressure);
    }

    pub(crate) fn flush_all(&mut self) {
        self.flush_processed();
        self.flush_sourced();
        self.flush_idle();
        self.flush_backpressure();
    }

    fn flush(shared: &AtomicU64, local: &mut u64) {
        if *local == 0 {
            return;
        }
        shared.fetch_add(*local, Ordering::Relaxed);
        *local = 0;
    }

    pub(crate) fn any_active(&self) -> bool {
        self.shared_active.load(Ordering::Relaxed) > 0
    }
}

impl Pipeline {
    pub fn builder() -> PipelineBuilder<(), (), (), ()> {
        PipelineBuilder::new()
    }
}

pub struct Runner {
    #[allow(dead_code)]
    pub(crate) inner_pipeline: Pipeline,
    #[allow(dead_code)]
    pub(crate) inner_petitioner: Petitioner,
    #[allow(dead_code)]
    pub(crate) inner_runtime: Runtime,
    pub(crate) inner_shutdown: Shutdown,
    pub(crate) inner_metrics: Metrics,
}

impl Runner {
    pub(crate) fn new(
        inner_pipeline: Pipeline,
        inner_petitioner: Petitioner,
        inner_runtime: Runtime,
        inner_shutdown: Shutdown,
        inner_metrics: Metrics,
    ) -> Self {
        Self {
            inner_pipeline,
            inner_petitioner,
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
