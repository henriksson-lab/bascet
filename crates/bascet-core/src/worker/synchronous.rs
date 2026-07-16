use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use crate::apply::Apply;
use crate::apply::Error;
use crate::apply::emit::Emit;
use crate::pipeline::gather::{Closed, Gather};
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::Schedule;
use crate::schedule::layer::Assignment;
use crate::schedule::preempt::Preempt;
use crate::set::Set;
use crate::utils::Patience;
use crate::worker::State;

pub(crate) struct Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    pub(crate) apply: A,
    pub(crate) gather: U,
    pub(crate) emit: Emit<A::Output, W>,
    pub(crate) layer: usize,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) runtime: Weak<RuntimeInner>,
    pub(crate) budget: Patience<u32>,
    pub(crate) round: u32,
    pub(crate) finalized: bool,
}

impl<A, U, W> Assignment for Run<A, U, W>
where
    A: Apply,
    A::Output: Send + 'static,
    U: Gather<Item = A::Input>,
    W: Set,
{
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> State {
        if self.finalized {
            return self.conclude();
        }
        loop {
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
                self.budget.miss();
                self.emit.flush();
                if !self.visit(schedule, tier, true) {
                    return self.leave();
                }
            }
            match self.gather.try_recv() {
                Ok(Some(item)) => {
                    if let Err(error) = self.apply.apply(item, &mut self.emit) {
                        self.emit.flush();
                        return self.fail(error);
                    }
                    if self.emit.finished() {
                        return self.conclude();
                    }
                    self.round += 1;
                    if self.round >= self.budget.patience() && !self.gather.residue() {
                        self.round = 0;
                        self.budget.hit();
                        self.emit.flush();
                        if !self.visit(schedule, tier, false) {
                            return self.leave();
                        }
                    }
                }
                Ok(None) => {
                    self.emit.flush();
                    if !self.visit(schedule, tier, false) {
                        return self.starve();
                    }
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

impl<A, U, W> Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
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
        let stay = scheduler.runnable(self.layer)
            && ((tier == Tier::Burn && !claim)
                || scheduler.pick(Some(self.layer)) == Some(self.layer));
        if stay
            && let Some(layer) = scheduler.layers[self.layer].as_mut()
        {
            layer.pass += 1;
        }
        stay
    }

    fn leave(&self) -> State {
        if self.emit.residue() || self.gather.residue() {
            State::Blocked
        } else {
            State::Yielded
        }
    }

    fn starve(&self) -> State {
        if self.emit.residue() {
            State::Blocked
        } else {
            State::Starved
        }
    }

    fn conclude(&mut self) -> State {
        if !self.finalized {
            self.finalized = true;
            if let Err(error) = self.apply.finish(&mut self.emit) {
                if let Some(runtime) = self.runtime.upgrade() {
                    runtime.record_error(error);
                }
            }
        }
        let clean = self.emit.flush();
        if self.emit.orphaned() {
            tracing::warn!(layer = self.layer, "finalize output discarded: consumer gone");
        }
        if clean { State::Finished } else { State::Blocked }
    }

    fn fail(&mut self, error: Error) -> State {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.record_error(error);
        }
        self.finalized = true;
        State::Failed
    }
}
