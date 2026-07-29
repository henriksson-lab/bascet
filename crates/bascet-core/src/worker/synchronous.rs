use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use crate::apply::Apply;
use crate::apply::Error;
use crate::apply::execute::{Assembled, Provides};
use crate::apply::fuse::Fuse;
use crate::pipeline::batch::{Batch, Keys, Len};
use crate::pipeline::gather::{Closed, Gather};
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::Schedule;
use crate::schedule::layer::Assignment;
use crate::schedule::preempt::Preempt;
use crate::set::Set;
use crate::set::ops::partition::Compose;
use crate::utils::AtomicPatience;
use crate::worker::State;

pub(crate) struct Run<A, U, W, Stores>
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
}

impl<A, U, W, Stores> Assignment for Run<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Assembled<Stores, A, W>: Len + Send + 'static,
{
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> State {
        if self.finalized {
            return self.conclude();
        }
        loop {
            if self.fuse.residue() && !self.flush() {
                return self.leave();
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
                if !self.visit(schedule, tier, true) {
                    return self.leave();
                }
            }
            match self.gather.try_recv() {
                Ok(Some(batch)) => match self.apply.apply_batch(&batch) {
                    Ok(Some(produced)) => {
                        let stores = batch.into_parts();
                        let out = <Stores as Compose<Provides<Stores, A>, W, A::Produces>>::compose(
                            stores, produced,
                        );
                        self.fuse.push(Batch::new(out));
                        if !self.flush() {
                            return self.leave();
                        }
                        if !self.visit(schedule, tier, false) {
                            return self.leave();
                        }
                    }
                    Ok(None) => {
                        return self.conclude();
                    }
                    Err(error) => {
                        self.fuse.flush();
                        return self.fail(error);
                    }
                },
                Ok(None) => {
                    self.fuse.flush();
                    self.check_in(schedule);
                    return self.starve();
                }
                Err(Closed) => {
                    return self.conclude();
                }
            }
        }
    }

    fn layer(&self) -> usize {
        self.layer
    }
}

impl<A, U, W, Stores> Run<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
{
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

    fn visit(&mut self, schedule: &Schedule, tier: Tier, claim: bool) -> bool {
        let mut scheduler = schedule.scheduler.lock();
        {
            let Some(layer) = scheduler.layers[self.layer].as_mut() else {
                return false;
            };
            layer
                .preempt
                .store(Preempt::Continue as u8, Ordering::Relaxed);
        }
        scheduler.wake();
        schedule.epoch.advance();
        let stay = scheduler.runnable(self.layer)
            && ((tier == Tier::Burn && !claim)
                || scheduler.pick(Some(self.layer)) == Some(self.layer));
        if stay && let Some(layer) = scheduler.layers[self.layer].as_mut() {
            layer.pass += 1;
        }
        stay
    }

    fn check_in(&self, schedule: &Schedule) {
        let mut scheduler = schedule.scheduler.lock();
        if let Some(layer) = scheduler.layers[self.layer].as_mut() {
            layer
                .preempt
                .store(Preempt::Continue as u8, Ordering::Relaxed);
        }
        scheduler.wake();
        schedule.epoch.advance();
    }

    fn leave(&self) -> State {
        if self.fuse.residue() || self.gather.residue() {
            State::Blocked
        } else {
            State::Yielded
        }
    }

    fn starve(&self) -> State {
        if self.fuse.residue() {
            State::Blocked
        } else {
            State::Starved
        }
    }

    fn conclude(&mut self) -> State {
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
            State::Finished
        } else {
            State::Blocked
        }
    }

    fn fail(&mut self, error: Error) -> State {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.record_error(error);
        }
        State::Failed
    }
}
