pub(crate) mod layer;
pub mod preempt;
pub(crate) mod wait;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::apply::Error;
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::layer::{Capacity, Exit, Layer, LayerState};
use crate::schedule::preempt::Cooperate;
use crate::schedule::wait::Waiters;

pub(crate) struct Schedule {
    pub(crate) layers: Box<[Arc<Layer>]>,
    pub(crate) live: AtomicU64,
    pub(crate) waiters: Waiters,
    pub(crate) runtime: Arc<RuntimeInner>,
}

impl Schedule {
    pub(crate) fn pick(&self, sticky: Option<usize>) -> Option<(usize, Arc<Layer>)> {
        let (index, best) = self
            .layers
            .iter()
            .enumerate()
            .filter(|(_, layer)| layer.runnable())
            .min_by_key(|(_, layer)| layer.live_pass.load(Ordering::Relaxed))?;
        if let Some(last) = sticky {
            let warm = &self.layers[last];
            let starving = self.layers.iter().enumerate().any(|(other, layer)| {
                other != last
                    && layer.runnable()
                    && layer.live_workers.load(Ordering::Relaxed) == 0
            });
            if warm.runnable() && !starving {
                return Some((last, Arc::clone(warm)));
            }
        }
        Some((index, Arc::clone(best)))
    }

    pub(crate) fn finished(&self) -> bool {
        self.live.load(Ordering::SeqCst) == 0
    }

    pub(crate) fn wake(&self) {
        self.waiters.unpark_one();
    }

    pub(crate) fn wake_all(&self) {
        self.waiters.unpark_all();
    }

    pub(crate) fn wake_join(&self) {
        self.waiters.unpark_join();
    }

    pub(crate) fn shutdown(&self) {
        for layer in self.layers.iter() {
            layer
                .live_preempt
                .store(Cooperate::Shutdown as u8, Ordering::Relaxed);
            layer.mark(LayerState::Runnable);
        }
        self.wake_all();
        self.wake_join();
    }

    fn retire(&self, layer: &Layer) {
        if !layer.terminal() || !layer.claim() {
            return;
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.wake_join();
        if self.finished() {
            self.wake_all();
        }
    }

    pub(crate) fn join_wait(&self, sink: &Layer) {
        loop {
            if sink.terminal() || self.finished() {
                return;
            }
            self.waiters.register_join();
            if sink.terminal() || self.finished() {
                return;
            }
            std::thread::park();
        }
    }
}

pub(crate) struct Worker {
    tier: Tier,
    last: Option<usize>,
}

impl Worker {
    pub(crate) fn new(tier: Tier) -> Self {
        Self { tier, last: None }
    }

    pub(crate) fn run(&mut self, schedule: &Schedule) {
        let tier = self.tier;
        let id = schedule.waiters.register();
        loop {
            if schedule.finished() {
                return;
            }
            let sticky = match tier {
                Tier::Burn => self.last,
                _ => None,
            };
            match schedule.pick(sticky) {
                Some((index, layer)) => {
                    self.last = Some(index);
                    layer.live_workers.fetch_add(1, Ordering::SeqCst);
                    let resumed = layer.live_suspended.pop();
                    let driven = catch_unwind(AssertUnwindSafe(|| {
                        let mut assignment = match resumed {
                            Some(assignment) => assignment,
                            None => layer
                                .build_dispatch
                                .lock()
                                .as_mut()
                                .map(|mint| mint(&layer))?,
                        };
                        let exit = assignment.drive(schedule, tier);
                        Some((assignment, exit))
                    }));
                    layer.live_workers.fetch_sub(1, Ordering::SeqCst);
                    match driven {
                        Ok(Some((assignment, Exit::Finished | Exit::Failed))) => {
                            layer
                                .live_capacity
                                .store(u64::from(Capacity::Finished), Ordering::SeqCst);
                            layer.seal();
                            drop(assignment);
                            if let Some(down) = layer.build_downstream.get() {
                                if down.rouse(LayerState::Starved) {
                                    schedule.wake();
                                }
                            }
                            schedule.retire(&layer);
                        }
                        Ok(Some((assignment, Exit::Suspended))) => {
                            if layer.live_suspended.push(assignment).is_err() {
                                panic!("suspended queue overflow: capacity is the worker count");
                            }
                            schedule.retire(&layer);
                        }
                        Ok(None) => schedule.retire(&layer),
                        Err(payload) => {
                            let message = payload
                                .downcast_ref::<&str>()
                                .map(|s| s.to_string())
                                .or_else(|| payload.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "unknown".to_string());
                            schedule.runtime.record_error(Error::Panic(message));
                            schedule.shutdown();
                        }
                    }
                }
                None => match tier {
                    Tier::Burn => {
                        if schedule.finished() {
                            return;
                        }
                        std::hint::spin_loop();
                    }
                    _ => {
                        schedule
                            .waiters
                            .park(id, || schedule.pick(None).is_some() || schedule.finished());
                    }
                },
            }
        }
    }
}
