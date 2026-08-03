use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::apply::Apply;
use crate::apply::execute::{Assembled, Provides};
use crate::apply::fuse::Fuse;
use crate::pipeline::batch::{Batch, Keys};
use crate::pipeline::edge::Downstream;
use crate::pipeline::gather::{Closed, Gather};
use crate::runtime::Tier;
use crate::schedule::Schedule;
use crate::schedule::layer::{Assignment, Exit, Layer, LayerState};
use crate::schedule::preempt::Cooperate;
use crate::set::Set;
use crate::set::partition::Compose;

pub(crate) struct Task<A, U, W, Stores>
where
    A: Apply<Stores>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
{
    pub(crate) apply: A,
    pub(crate) gather: U,
    pub(crate) fuse: Fuse<Batch<Assembled<Stores, A, W>>>,
    pub(crate) layer: Arc<Layer>,
    pub(crate) finalized: bool,
}

impl<A, U, W, Stores> Assignment for Task<A, U, W, Stores>
where
    A: Apply<Stores>,
    U: Gather<Item = Batch<Stores>>,
    W: Set,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Assembled<Stores, A, W>: Send + 'static,
{
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> Exit {
        if self.finalized {
            return self.finish(schedule);
        }
        loop {
            if self.fuse.residue() && !self.flush() {
                self.layer.mark(LayerState::Blocked);
                return Exit::Suspended;
            }
            match Cooperate::from(self.layer.live_preempt.load(Ordering::Relaxed)) {
                Cooperate::Shutdown => return self.finish(schedule),
                Cooperate::Halt => {
                    if self
                        .layer
                        .live_preempt
                        .compare_exchange(
                            Cooperate::Halt as u8,
                            Cooperate::Continue as u8,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        self.fuse.flush();
                        return Exit::Suspended;
                    }
                }
                Cooperate::Continue | Cooperate::Yield => {}
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
                            self.layer.live_pass.fetch_add(1, Ordering::Relaxed);
                            if !self.flush() {
                                self.layer.mark(LayerState::Blocked);
                                return Exit::Suspended;
                            }
                            if self.fuse.orphaned() {
                                return self.finish(schedule);
                            }
                            self.rouse_downstream(schedule);
                            match tier {
                                Tier::Burn => {}
                                _ => return Exit::Suspended,
                            }
                        }
                        Ok(None) => return self.finish(schedule),
                        Err(error) => {
                            self.fuse.flush();
                            schedule.runtime.record_error(error);
                            return Exit::Failed;
                        }
                    }
                }
                Ok(None) => {
                    self.fuse.flush();
                    self.layer.mark(LayerState::Starved);
                    return Exit::Suspended;
                }
                Err(Closed) => return self.finish(schedule),
            }
        }
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
        layer: &Arc<Layer>,
    ) -> Box<dyn Assignment>
    where
        Assembled<Stores, A, W>: Send + 'static,
    {
        Box::new(Task {
            apply: apply.clone(),
            gather: gather.clone(),
            fuse: Fuse::new(downstream.clone()),
            layer: Arc::clone(layer),
            finalized: false,
        })
    }

    fn flush(&mut self) -> bool {
        if self.fuse.flush() {
            return true;
        }
        let patience = self.layer.live_patience.patience();
        for _ in 0..patience {
            std::hint::spin_loop();
            if self.fuse.flush() {
                self.layer.live_patience.hit();
                return true;
            }
        }
        self.layer.live_patience.miss();
        false
    }

    fn rouse_downstream(&self, schedule: &Schedule) {
        if let Some(down) = self.layer.build_downstream.get() {
            if down.ready() == LayerState::Starved && down.rouse(LayerState::Starved) {
                schedule.wake();
            }
        }
    }

    fn rouse_upstream(&self, schedule: &Schedule) {
        let Some(upstream) = self.layer.build_upstream.get() else {
            return;
        };
        for producer in upstream.iter() {
            if producer.ready() == LayerState::Blocked && producer.rouse(LayerState::Blocked) {
                schedule.wake();
            }
        }
    }

    fn finish(&mut self, schedule: &Schedule) -> Exit {
        if !self.finalized {
            self.finalized = true;
            if let Err(error) = self.apply.finish() {
                schedule.runtime.record_error(error);
            }
        }
        let clean = self.fuse.flush();
        if self.fuse.orphaned() {
            tracing::warn!("finalize output discarded: consumer gone");
        }
        if clean {
            Exit::Finished
        } else {
            self.layer.mark(LayerState::Blocked);
            Exit::Suspended
        }
    }
}
