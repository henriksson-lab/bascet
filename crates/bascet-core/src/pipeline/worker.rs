use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crossbeam::channel::Receiver;

mod r#async;
mod runtime;
mod sync;
mod topology;

pub(crate) use runtime::Dispatch;
pub(super) use topology::{Connect, Drain};

use super::edge::{Downstream, Upstream};
use super::pipeline::Metrics;
use super::runtime::Runtime;
use super::scheduler::{Decision, Id, Petitioner, Slot};
use crate::apply::Apply;
use crate::layer::{Activity, WorkerState};

pub(super) struct Exit;

pub(crate) struct Worker<S: Apply> {
    pub(super) id: Id,
    pub(super) active: Arc<AtomicU64>,
    pub(super) runtime: Runtime,
    pub(super) metrics: Metrics,
    pub(super) petitioner: Petitioner,
    pub(super) stage: S,
    pub(super) input: Option<Upstream<S::Input>>,
    pub(super) output: Option<Downstream<S::Output>>,
    pub(super) state: WorkerState,
}

impl<S: Apply> Worker<S> {
    pub(crate) fn new(
        slot: Slot,
        runtime: Runtime,
        stage: S,
        input: Option<Upstream<S::Input>>,
        output: Option<Downstream<S::Output>>,
    ) -> Self {
        Self {
            id: slot.id,
            active: slot.active,
            metrics: runtime.metrics(),
            runtime,
            petitioner: slot.petitioner,
            stage,
            input,
            output,
            state: slot.state,
        }
    }

    pub(super) fn finish(self, _exit: Exit) {
        self.state.set_activity(Activity::Exiting);
        let mut metrics = self.metrics;
        let work = metrics
            .local_processed
            .saturating_add(metrics.local_sourced);
        metrics.flush_all();
        self.petitioner.finish(self.id, work);
    }
}

impl Petitioner {
    pub(super) fn fail(&self, id: Id, kind: &str) {
        tracing::error!(?id, kind, "pipeline worker panicked");
        self.finish(id, 0);
    }
}

impl Exit {
    pub(super) fn poll(decision_rx: &Receiver<Decision>) -> Option<Self> {
        decision_rx.try_recv().ok().map(|_| Exit)
    }
}

impl Slot {
    pub(super) fn finish(self, processed: u64, _exit: Exit) {
        self.state.set_activity(Activity::Exiting);
        self.petitioner.finish(self.id, processed);
    }
}

