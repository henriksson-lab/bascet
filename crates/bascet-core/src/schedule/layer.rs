use std::num::NonZero;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;

use crate::runtime::{RuntimeInner, Tier};
use crate::schedule::Schedule;
use crate::schedule::preempt::Cooperate;
use crate::utils::AtomicPatience;

pub(crate) trait Assignment: Send {
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> Exit;
}

pub(crate) enum Exit {
    Suspended,
    Finished,
    Failed,
}

pub(crate) type Dispatch = Box<dyn FnMut(&Arc<Layer>) -> Box<dyn Assignment> + Send>;

#[derive(Clone, Copy)]
pub(crate) enum Capacity {
    Open,
    Limited(NonZero<u64>),
    Finished,
}

impl From<Capacity> for u64 {
    fn from(capacity: Capacity) -> Self {
        match capacity {
            Capacity::Finished => 0,
            Capacity::Open => u64::MAX,
            Capacity::Limited(max) => max.get(),
        }
    }
}

impl From<u64> for Capacity {
    fn from(bits: u64) -> Self {
        match bits {
            0 => Capacity::Finished,
            u64::MAX => Capacity::Open,
            max => Capacity::Limited(NonZero::new(max).unwrap()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum LayerState {
    Runnable = 0,
    Starved = 1,
    Blocked = 2,
}

pub(crate) struct Layer {
    pub(crate) build_dispatch: Mutex<Option<Dispatch>>,
    pub(crate) build_downstream: OnceLock<Arc<Layer>>,
    pub(crate) build_upstream: OnceLock<Box<[Arc<Layer>]>>,
    pub(crate) live_capacity: AtomicU64,
    pub(crate) live_state: AtomicU8,
    pub(crate) live_suspended: ArrayQueue<Box<dyn Assignment>>,
    pub(crate) live_workers: AtomicU64,
    pub(crate) live_pass: AtomicU64,
    pub(crate) live_preempt: AtomicU8,
    pub(crate) live_patience: AtomicPatience,
    pub(crate) live_done: AtomicBool,
}

impl Layer {
    pub(crate) fn new(dispatch: Dispatch, runtime: &RuntimeInner) -> Self {
        Self {
            build_dispatch: Mutex::new(Some(dispatch)),
            build_downstream: OnceLock::new(),
            build_upstream: OnceLock::new(),
            live_capacity: AtomicU64::new(u64::from(Capacity::Open)),
            live_state: AtomicU8::new(LayerState::Runnable as u8),
            live_suspended: ArrayQueue::new(runtime.allocation.workers()),
            live_workers: AtomicU64::new(0),
            live_pass: AtomicU64::new(0),
            live_preempt: AtomicU8::new(Cooperate::Continue as u8),
            live_patience: AtomicPatience::new(),
            live_done: AtomicBool::new(false),
        }
    }

    pub(crate) fn seal(&self) {
        *self.build_dispatch.lock() = None;
    }

    pub(crate) fn claim(&self) -> bool {
        self.live_done
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub(crate) fn runnable(&self) -> bool {
        self.ready() == LayerState::Runnable
            && (self.live_workers.load(Ordering::SeqCst)
                < self.live_capacity.load(Ordering::SeqCst)
                || !self.live_suspended.is_empty())
    }

    pub(crate) fn terminal(&self) -> bool {
        self.live_capacity.load(Ordering::SeqCst) == u64::from(Capacity::Finished)
            && self.live_workers.load(Ordering::SeqCst) == 0
            && self.live_suspended.is_empty()
    }

    pub(crate) fn ready(&self) -> LayerState {
        match self.live_state.load(Ordering::SeqCst) {
            0 => LayerState::Runnable,
            1 => LayerState::Starved,
            _ => LayerState::Blocked,
        }
    }

    pub(crate) fn mark(&self, state: LayerState) {
        self.live_state.store(state as u8, Ordering::SeqCst);
    }

    pub(crate) fn rouse(&self, from: LayerState) -> bool {
        self.live_state
            .compare_exchange(
                from as u8,
                LayerState::Runnable as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }
}
