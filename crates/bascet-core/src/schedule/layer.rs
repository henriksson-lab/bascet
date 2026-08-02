use std::num::NonZero;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::runtime::Tier;
use crate::schedule::Schedule;
use crate::utils::AtomicPatience;
use crate::worker::State;

pub(crate) trait Assignment: Send {
    fn drive(&mut self, schedule: &Schedule, tier: Tier);
    fn state(&self) -> State;
    fn layer(&self) -> usize;
}

pub(crate) type Mint = Box<dyn FnMut() -> Box<dyn Assignment> + Send>;
pub(crate) type Dispatch = Arc<Mutex<Option<Mint>>>;

#[derive(Clone, Copy)]
pub(crate) enum Capacity {
    Open,
    Limited(NonZero<u64>),
    Finished,
}

impl Capacity {
    pub(crate) fn pack(self) -> u64 {
        match self {
            Capacity::Finished => 0,
            Capacity::Open => u64::MAX,
            Capacity::Limited(max) => max.get(),
        }
    }

    pub(crate) fn unpack(bits: u64) -> Self {
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
    pub(crate) dispatch: Dispatch,
    pub(crate) capacity: AtomicU64,
    pub(crate) state: AtomicU8,
    pub(crate) downstream: OnceLock<Arc<Layer>>,
    pub(crate) workers: AtomicU64,
    pub(crate) pass: AtomicU64,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) patience: Arc<AtomicPatience>,
    pub(crate) done: AtomicBool,
}

impl Layer {
    pub(crate) fn seal(&self) {
        *self.dispatch.lock() = None;
    }

    pub(crate) fn claim(&self) -> bool {
        self.done
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub(crate) fn ready(&self) -> LayerState {
        match self.state.load(Ordering::SeqCst) {
            0 => LayerState::Runnable,
            1 => LayerState::Starved,
            _ => LayerState::Blocked,
        }
    }

    pub(crate) fn mark(&self, state: LayerState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }

    pub(crate) fn rouse(&self, from: LayerState) -> bool {
        self.state
            .compare_exchange(
                from as u8,
                LayerState::Runnable as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }
}
