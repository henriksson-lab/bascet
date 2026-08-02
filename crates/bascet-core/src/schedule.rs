pub(crate) mod layer;
pub mod preempt;
pub(crate) mod wait;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::apply::Error;
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::layer::{Assignment, Capacity, Layer, LayerState};
use crate::schedule::wait::Waiters;
use crate::worker::State;

pub(crate) struct Schedule {
    pub(crate) scheduler: Scheduler,
    pub(crate) waiters: Waiters,
}

pub(crate) struct Scheduler {
    pub(crate) layers: Box<[std::sync::Arc<Layer>]>,
    pub(crate) upstream: Box<[Box<[usize]>]>,
    pub(crate) suspended: Box<[ArrayQueue<Box<dyn Assignment>>]>,
    pub(crate) live: AtomicU64,
}

impl Scheduler {
    pub(crate) fn runnable(&self, index: usize) -> bool {
        let layer = &self.layers[index];
        layer.ready() == LayerState::Runnable
            && (layer.workers.load(Ordering::SeqCst) < layer.capacity.load(Ordering::SeqCst)
                || !self.suspended[index].is_empty())
    }

    pub(crate) fn pick(&self, previous: Option<usize>) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for index in 0..self.layers.len() {
            if !self.runnable(index) {
                continue;
            }
            let current = self.layers[index].pass.load(Ordering::Relaxed);
            let replace = match best {
                None => true,
                Some((_, pass)) => current < pass || (current == pass && Some(index) == previous),
            };
            if replace {
                best = Some((index, current));
            }
        }
        best.map(|(index, _)| index)
    }

    pub(crate) fn terminal(&self, index: usize) -> bool {
        let layer = &self.layers[index];
        layer.capacity.load(Ordering::SeqCst) == Capacity::Finished.pack()
            && layer.workers.load(Ordering::SeqCst) == 0
            && self.suspended[index].is_empty()
    }

    pub(crate) fn finished(&self) -> bool {
        self.live.load(Ordering::SeqCst) == 0
    }
}

impl Schedule {
    pub(crate) fn wake(&self) {
        self.waiters.unpark_one();
    }

    pub(crate) fn wake_all(&self) {
        self.waiters.unpark_all();
    }

    pub(crate) fn wake_join(&self) {
        self.waiters.unpark_join();
    }

    fn retire(&self, index: usize) {
        if !self.scheduler.terminal(index) || !self.scheduler.layers[index].claim() {
            return;
        }
        self.scheduler.live.fetch_sub(1, Ordering::SeqCst);
        self.wake_join();
        if self.scheduler.finished() {
            self.wake_all();
        }
    }

    pub(crate) fn participate(&self, runtime: &Weak<RuntimeInner>, tier: Tier) {
        let scheduler = &self.scheduler;
        let id = self.waiters.register();
        let mut current: Option<Box<dyn Assignment>> = None;
        let mut previous: Option<usize> = None;
        loop {
            if let Some(assignment) = current.take() {
                let index = assignment.layer();
                let status = assignment.state();
                let layer = &scheduler.layers[index];
                layer.workers.fetch_sub(1, Ordering::SeqCst);
                match status {
                    State::Finished | State::Failed => {
                        layer
                            .capacity
                            .store(Capacity::Finished.pack(), Ordering::SeqCst);
                        layer.seal();
                        drop(assignment);
                        if let Some(down) = layer.downstream.get() {
                            if down.rouse(LayerState::Starved) {
                                self.wake();
                            }
                        }
                    }
                    _ => {
                        if scheduler.suspended[index].push(assignment).is_err() {
                            panic!("suspended queue overflow: capacity is the worker count");
                        }
                    }
                }
                self.retire(index);
            }
            if scheduler.finished() {
                return;
            }
            match scheduler.pick(previous) {
                Some(index) => {
                    let layer = &scheduler.layers[index];
                    layer.workers.fetch_add(1, Ordering::SeqCst);
                    layer.pass.fetch_add(1, Ordering::Relaxed);
                    let dispatch = layer.dispatch.clone();
                    let resumed = scheduler.suspended[index].pop();
                    previous = Some(index);
                    let outcome = catch_unwind(AssertUnwindSafe(|| {
                        let mut assignment = match resumed {
                            Some(assignment) => assignment,
                            None => dispatch.lock().as_mut().map(|mint| mint())?,
                        };
                        assignment.drive(self, tier);
                        Some(assignment)
                    }));
                    match outcome {
                        Ok(Some(driven)) => current = Some(driven),
                        Ok(None) => {
                            scheduler.layers[index]
                                .workers
                                .fetch_sub(1, Ordering::SeqCst);
                            self.retire(index);
                        }
                        Err(payload) => {
                            scheduler.layers[index]
                                .workers
                                .fetch_sub(1, Ordering::SeqCst);
                            if let Some(inner) = runtime.upgrade() {
                                let message = payload
                                    .downcast_ref::<&str>()
                                    .map(|s| s.to_string())
                                    .or_else(|| payload.downcast_ref::<String>().cloned())
                                    .unwrap_or_else(|| "unknown".to_string());
                                inner.record_error(Error::Panic(message));
                                inner.shutdown.trigger();
                            }
                        }
                    }
                }
                None => match tier {
                    Tier::Burn => {
                        if scheduler.finished() {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                    _ => {
                        self.waiters.park(id, || {
                            scheduler.pick(previous).is_some() || scheduler.finished()
                        });
                    }
                },
            }
        }
    }

    pub(crate) fn join_wait(&self, sink: usize) {
        loop {
            if self.scheduler.terminal(sink) || self.scheduler.finished() {
                return;
            }
            self.waiters.register_join();
            if self.scheduler.terminal(sink) || self.scheduler.finished() {
                return;
            }
            std::thread::park();
        }
    }
}
