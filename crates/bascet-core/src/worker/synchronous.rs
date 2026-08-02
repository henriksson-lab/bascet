use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use crate::apply::Apply;
use crate::apply::Error;
use crate::apply::execute::{Assembled, Provides};
use crate::apply::fuse::Fuse;
use crate::pipeline::batch::{Batch, Keys, Len};
use crate::pipeline::edge::Downstream;
use crate::pipeline::gather::{Closed, Gather};
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::Schedule;
use crate::schedule::layer::{Assignment, LayerState};
use crate::schedule::preempt::Preempt;
use crate::set::Set;
use crate::set::ops::partition::Compose;
use crate::utils::AtomicPatience;
use crate::worker::State;

pub(crate) struct Task<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
{
    pub(crate) apply: A,
    pub(crate) gather: U,
    pub(crate) fuse: Fuse<Batch<Assembled<Stores, A, W>>>,
    pub(crate) layer: usize,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) patience: Arc<AtomicPatience>,
    pub(crate) runtime: Weak<RuntimeInner>,
    pub(crate) finalized: bool,
    pub(crate) state: State,
}

impl<A, U, W, Stores> Assignment for Task<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Assembled<Stores, A, W>: Len + Send + 'static,
{
    fn drive(&mut self, schedule: &Schedule, _tier: Tier) {
        if self.finalized {
            return self.finish(schedule);
        }
        loop {
            if self.fuse.residue() && !self.flush() {
                schedule.scheduler.layers[self.layer].mark(LayerState::Blocked);
                self.state = State::Blocked;
                return;
            }
            if self.preempt.load(Ordering::Relaxed) == Preempt::Halt as u8
                && self
                    .preempt
                    .compare_exchange(
                        Preempt::Halt as u8,
                        Preempt::Continue as u8,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                self.fuse.flush();
                self.state = State::Yielded;
                return;
            }
            match self.gather.try_recv() {
                Ok(Some(batch)) => {
                    self.rouse_upstream(schedule);
                    match self.apply.apply_batch(&batch) {
                        Ok(Some(produced)) => {
                            let stores = batch.into_parts();
                            let out =
                                <Stores as Compose<Provides<Stores, A>, W, A::Produces>>::compose(
                                    stores, produced,
                                );
                            self.fuse.push(Batch::new(out));
                            if !self.flush() {
                                schedule.scheduler.layers[self.layer].mark(LayerState::Blocked);
                                self.state = State::Blocked;
                                return;
                            }
                            if self.fuse.orphaned() {
                                return self.finish(schedule);
                            }
                            self.rouse_downstream(schedule);
                        }
                        Ok(None) => return self.finish(schedule),
                        Err(error) => {
                            self.fuse.flush();
                            return self.fail(error);
                        }
                    }
                }
                Ok(None) => {
                    self.fuse.flush();
                    schedule.scheduler.layers[self.layer].mark(LayerState::Starved);
                    self.state = State::Starved;
                    return;
                }
                Err(Closed) => return self.finish(schedule),
            }
        }
    }

    fn state(&self) -> State {
        self.state
    }

    fn layer(&self) -> usize {
        self.layer
    }
}

impl<A, U, W, Stores> Task<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
{
    pub(crate) fn new(
        apply: &A,
        gather: &U,
        downstream: &Option<Downstream<Batch<Assembled<Stores, A, W>>>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        patience: &Arc<AtomicPatience>,
        runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        Assembled<Stores, A, W>: Len + Send + 'static,
    {
        Box::new(Task {
            apply: apply.clone(),
            gather: gather.clone(),
            fuse: Fuse::new(downstream.clone()),
            layer,
            preempt: Arc::clone(preempt),
            patience: Arc::clone(patience),
            runtime: runtime.clone(),
            finalized: false,
            state: State::New,
        })
    }

    fn flush(&mut self) -> bool {
        if self.fuse.flush() {
            return true;
        }
        let patience = self.patience.patience();
        for _ in 0..patience {
            std::hint::spin_loop();
            if self.fuse.flush() {
                self.patience.hit();
                return true;
            }
        }
        self.patience.miss();
        false
    }

    fn rouse_downstream(&self, schedule: &Schedule) {
        if let Some(down) = schedule.scheduler.layers[self.layer].downstream.get() {
            if down.ready() == LayerState::Starved && down.rouse(LayerState::Starved) {
                schedule.wake();
            }
        }
    }

    fn rouse_upstream(&self, schedule: &Schedule) {
        for &up in schedule.scheduler.upstream[self.layer].iter() {
            let producer = &schedule.scheduler.layers[up];
            if producer.ready() == LayerState::Blocked && producer.rouse(LayerState::Blocked) {
                schedule.wake();
            }
        }
    }

    fn finish(&mut self, schedule: &Schedule) {
        if !self.finalized {
            self.finalized = true;
            if let Err(error) = self.apply.finish() {
                if let Some(runtime) = self.runtime.upgrade() {
                    runtime.record_error(error);
                }
            }
        }
        let clean = self.fuse.flush();
        if self.fuse.orphaned() {
            tracing::warn!(
                layer = self.layer,
                "finalize output discarded: consumer gone"
            );
        }
        if clean {
            self.state = State::Finished;
        } else {
            schedule.scheduler.layers[self.layer].mark(LayerState::Blocked);
            self.state = State::Blocked;
        }
    }

    fn fail(&mut self, error: Error) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.record_error(error);
        }
        self.state = State::Failed;
    }
}
