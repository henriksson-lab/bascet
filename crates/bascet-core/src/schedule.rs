pub(crate) mod layer;
pub mod preempt;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::task::Waker;
use std::thread::Thread;

use parking_lot::Mutex;

use crate::pipeline::gather::Probe;
use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::layer::{Assignment, Layer};
use crate::schedule::preempt::Preempt;
use crate::worker::State;

pub(crate) struct Schedule {
    pub(crate) scheduler: Mutex<Scheduler>,
}

pub(crate) struct Scheduler {
    pub(crate) layers: Box<[Option<Layer>]>,
    pub(crate) upstream: Box<[Box<[usize]>]>,
    pub(crate) idle: Vec<Waker>,
    pub(crate) waiter: Option<Waker>,
}

impl Scheduler {
    pub(crate) fn runnable(&self, index: usize) -> bool {
        let Some(layer) = self.layers[index].as_ref() else {
            return false;
        };
        let assignable = layer.dispatch.is_some()
            || !layer.blocked.is_empty()
            || !layer.parked.is_empty();
        if !assignable {
            return false;
        }
        match (layer.probe)() {
            Probe::Full => false,
            Probe::Ready => true,
            Probe::Starved => !layer.blocked.is_empty(),
            Probe::Exhausted => {
                !layer.blocked.is_empty() || !layer.parked.is_empty() || layer.workers == 0
            }
        }
    }

    pub(crate) fn pick(&self, previous: Option<usize>) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for (index, entry) in self.layers.iter().enumerate() {
            let Some(layer) = entry else {
                continue;
            };
            if !self.runnable(index) {
                continue;
            }
            let replace = match best {
                None => true,
                Some((_, pass)) => {
                    layer.pass < pass || (layer.pass == pass && Some(index) == previous)
                }
            };
            if replace {
                best = Some((index, layer.pass));
            }
        }
        best.map(|(index, _)| index)
    }

    pub(crate) fn post(&self, dry: usize) {
        let mut at = dry;
        let victim = loop {
            let Some(&up) = self.upstream[at].first() else {
                break self.busiest();
            };
            if self.layers[up]
                .as_ref()
                .is_some_and(|layer| layer.workers > 0)
            {
                break Some(up);
            }
            at = up;
        };
        if let Some(index) = victim
            && let Some(layer) = self.layers[index].as_ref()
        {
            layer.preempt.store(Preempt::Halt as u8, Ordering::Relaxed);
        }
    }

    fn busiest(&self) -> Option<usize> {
        self.layers
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.as_ref().map(|layer| (index, layer)))
            .filter(|(_, layer)| layer.workers > 0)
            .max_by_key(|(_, layer)| layer.pass)
            .map(|(index, _)| index)
    }

    pub(crate) fn wake(&mut self) {
        for index in 0..self.layers.len() {
            if self.idle.is_empty() {
                return;
            }
            let unmanned = self.layers[index]
                .as_ref()
                .is_some_and(|layer| layer.workers == 0);
            if unmanned
                && self.runnable(index)
                && let Some(waker) = self.idle.pop()
            {
                waker.wake();
            }
        }
    }

    pub(crate) fn retire(&mut self, index: usize) {
        if self.layers[index].take().is_some() {
            if let Some(waker) = self.waiter.take() {
                waker.wake();
            }
            if self.finished() {
                for waker in self.idle.drain(..) {
                    waker.wake();
                }
            }
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.layers.iter().all(Option::is_none)
    }
}

pub(crate) struct Unpark(pub(crate) Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

impl Schedule {
    pub(crate) fn participate(&self, runtime: &Weak<RuntimeInner>, tier: Tier) {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut current: Option<(Box<dyn Assignment>, State)> = None;
        let mut previous: Option<usize> = None;
        let mut slept = false;
        loop {
            let mut scheduler = match tier {
                Tier::Burn => loop {
                    if !self.scheduler.is_locked()
                        && let Some(scheduler) = self.scheduler.try_lock()
                    {
                        break scheduler;
                    }
                    std::hint::spin_loop();
                },
                _ => self.scheduler.lock(),
            };
            if slept {
                slept = false;
                scheduler.idle.retain(|idler| !idler.will_wake(&waker));
            }
            let mut starved: Option<usize> = None;
            if let Some((assignment, status)) = current.take() {
                let index = assignment.layer();
                match scheduler.layers[index].as_mut() {
                    None => drop(assignment),
                    Some(layer) => {
                        layer.workers -= 1;
                        match status {
                            State::Finished => {
                                layer.dispatch = None;
                                let joined = layer.workers == 0
                                    && layer.blocked.is_empty()
                                    && layer.parked.is_empty();
                                drop(assignment);
                                if joined {
                                    scheduler.retire(index);
                                }
                            }
                            State::Blocked => layer.blocked.push_back(assignment),
                            State::Starved => {
                                layer.parked.push_back(assignment);
                                starved = Some(index);
                            }
                            State::Failed => {
                                drop(assignment);
                                scheduler.retire(index);
                            }
                            _ => layer.parked.push_back(assignment),
                        }
                    }
                }
                scheduler.wake();
            }
            if scheduler.finished() {
                return;
            }
            match scheduler.pick(previous) {
                Some(index) => {
                    let (popped, dispatch) = {
                        let layer = scheduler.layers[index].as_mut().unwrap();
                        layer.workers += 1;
                        layer.pass += 1;
                        let popped = layer
                            .blocked
                            .pop_front()
                            .or_else(|| layer.parked.pop_front());
                        let dispatch = if popped.is_some() {
                            None
                        } else {
                            layer.dispatch.clone()
                        };
                        (popped, dispatch)
                    };
                    drop(scheduler);
                    let mut assignment = match popped {
                        Some(assignment) => assignment,
                        None => (&mut *dispatch
                            .expect("picked a layer with nothing to assign")
                            .lock())(),
                    };
                    previous = Some(index);
                    let outcome =
                        catch_unwind(AssertUnwindSafe(|| assignment.drive(self, tier)));
                    match outcome {
                        Ok(status) => current = Some((assignment, status)),
                        Err(_) => {
                            if let Some(inner) = runtime.upgrade() {
                                inner.record_error(());
                            }
                            let mut scheduler = self.scheduler.lock();
                            if let Some(layer) = scheduler.layers[index].as_mut() {
                                layer.workers -= 1;
                            }
                            scheduler.retire(index);
                        }
                    }
                }
                None => {
                    if let Some(dry) = starved {
                        scheduler.post(dry);
                    }
                    match tier {
                        Tier::Burn => {
                            drop(scheduler);
                            std::hint::spin_loop();
                        }
                        _ => {
                            scheduler.idle.push(waker.clone());
                            slept = true;
                            drop(scheduler);
                            std::thread::park();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn join_wait(&self, sink: usize) {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        loop {
            let mut scheduler = self.scheduler.lock();
            if scheduler.layers[sink].is_none() {
                return;
            }
            scheduler.waiter = Some(waker.clone());
            drop(scheduler);
            std::thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicU8};

    struct Stub;

    impl Assignment for Stub {
        fn drive(&mut self, _: &Schedule, _: Tier) -> State {
            State::Finished
        }
        fn layer(&self) -> usize {
            0
        }
    }

    struct Flag(Arc<AtomicBool>);

    impl std::task::Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    fn layer(input: Arc<AtomicBool>, output: Arc<AtomicBool>, pass: u64) -> Layer {
        let dispatch: Arc<Mutex<dyn FnMut() -> Box<dyn Assignment> + Send>> =
            Arc::new(Mutex::new(|| Box::new(Stub) as Box<dyn Assignment>));
        Layer {
            dispatch: Some(dispatch),
            probe: Box::new(move || {
                if !output.load(Ordering::Relaxed) {
                    Probe::Full
                } else if input.load(Ordering::Relaxed) {
                    Probe::Ready
                } else {
                    Probe::Starved
                }
            }),
            blocked: VecDeque::new(),
            parked: VecDeque::new(),
            workers: 0,
            pass,
            preempt: Arc::new(AtomicU8::new(Preempt::Continue as u8)),
        }
    }

    fn flags() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
        )
    }

    fn scheduler(layers: Vec<Option<Layer>>, upstream: Vec<Vec<usize>>) -> Scheduler {
        Scheduler {
            layers: layers.into_boxed_slice(),
            upstream: upstream.into_iter().map(Vec::into_boxed_slice).collect(),
            idle: Vec::new(),
            waiter: None,
        }
    }

    #[test]
    fn pick_takes_minimum_pass_among_runnable() {
        let (input_a, output_a) = flags();
        let (input_b, output_b) = flags();
        let scheduler = scheduler(
            vec![
                Some(layer(input_a, output_a, 5)),
                Some(layer(input_b, output_b, 2)),
            ],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(None), Some(1));
    }

    #[test]
    fn probe_gates_the_pick() {
        let (input_a, output_a) = flags();
        let (input_b, output_b) = flags();
        input_b.store(false, Ordering::Relaxed);
        let scheduler = scheduler(
            vec![
                Some(layer(input_a, output_a, 5)),
                Some(layer(input_b, output_b, 2)),
            ],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(None), Some(0));
    }

    #[test]
    fn blocked_queue_counts_as_input() {
        let (input, output) = flags();
        input.store(false, Ordering::Relaxed);
        let mut entry = layer(input, output, 0);
        entry.blocked.push_back(Box::new(Stub));
        let scheduler = scheduler(vec![Some(entry)], vec![vec![]]);
        assert_eq!(scheduler.pick(None), Some(0));
    }

    #[test]
    fn ties_break_to_previous_then_downstream() {
        let (input_a, output_a) = flags();
        let (input_b, output_b) = flags();
        let scheduler = scheduler(
            vec![
                Some(layer(input_a, output_a, 3)),
                Some(layer(input_b, output_b, 3)),
            ],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(Some(1)), Some(1));
        assert_eq!(scheduler.pick(None), Some(0));
    }

    #[test]
    fn post_walks_past_unmanned_to_nearest_manned() {
        let (input_a, output_a) = flags();
        let (input_b, output_b) = flags();
        let (input_c, output_c) = flags();
        let mut source = layer(input_c, output_c, 0);
        source.workers = 1;
        let scheduler = scheduler(
            vec![
                Some(layer(input_a, output_a, 0)),
                Some(layer(input_b, output_b, 0)),
                Some(source),
            ],
            vec![vec![1], vec![2], vec![]],
        );
        scheduler.post(0);
        let posted = scheduler.layers[2]
            .as_ref()
            .unwrap()
            .preempt
            .load(Ordering::Relaxed);
        assert_eq!(posted, Preempt::Halt as u8);
    }

    #[test]
    fn post_falls_back_to_highest_pass_manned() {
        let (input_a, output_a) = flags();
        let (input_b, output_b) = flags();
        let mut other = layer(input_b, output_b, 9);
        other.workers = 1;
        let scheduler = scheduler(
            vec![Some(layer(input_a, output_a, 0)), Some(other)],
            vec![vec![], vec![]],
        );
        scheduler.post(0);
        let posted = scheduler.layers[1]
            .as_ref()
            .unwrap()
            .preempt
            .load(Ordering::Relaxed);
        assert_eq!(posted, Preempt::Halt as u8);
    }

    #[test]
    fn retire_wakes_the_waiter() {
        let woken = Arc::new(AtomicBool::new(false));
        let (input, output) = flags();
        let mut scheduler = scheduler(vec![Some(layer(input, output, 0))], vec![vec![]]);
        scheduler.waiter = Some(Waker::from(Arc::new(Flag(Arc::clone(&woken)))));
        scheduler.retire(0);
        assert!(scheduler.layers[0].is_none());
        assert!(scheduler.finished());
        assert!(woken.load(Ordering::Relaxed));
    }

    #[test]
    fn wake_pops_one_idler_per_unmanned_runnable_layer() {
        let woken = Arc::new(AtomicBool::new(false));
        let (input, output) = flags();
        let mut scheduler = scheduler(vec![Some(layer(input, output, 0))], vec![vec![]]);
        scheduler
            .idle
            .push(Waker::from(Arc::new(Flag(Arc::clone(&woken)))));
        scheduler.wake();
        assert!(woken.load(Ordering::Relaxed));
        assert!(scheduler.idle.is_empty());
    }
}
