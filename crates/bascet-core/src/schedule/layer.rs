use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::Mutex;

use crate::pipeline::gather::Probe;
use crate::runtime::Tier;
use crate::schedule::Schedule;
use crate::utils::AtomicPatience;
use crate::worker::State;

pub(crate) trait Assignment: Send {
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> State;
    fn layer(&self) -> usize;
}

pub(crate) type Dispatch = Arc<Mutex<dyn FnMut() -> Box<dyn Assignment> + Send>>;

#[derive(Clone, Copy)]
pub(crate) enum LayerState {
    Open,
    Limited(u64),
    Finished,
}

pub(crate) struct Layer {
    pub(crate) dispatch: Dispatch,
    pub(crate) state: LayerState,
    pub(crate) probe: Box<dyn Fn() -> Probe + Send>,
    pub(crate) blocked: VecDeque<Box<dyn Assignment>>,
    pub(crate) parked: VecDeque<Box<dyn Assignment>>,
    pub(crate) workers: u64,
    pub(crate) pass: u64,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) patience: Arc<AtomicPatience>,
}

impl Layer {
    pub(crate) fn is_open(&self) -> bool {
        let open = match self.state {
            LayerState::Open => true,
            LayerState::Limited(max) => self.workers < max,
            LayerState::Finished => false,
        };
        let unblocked = self.blocked.is_empty();
        let unparked = self.parked.is_empty();

        let work = !(unblocked && unparked);
        if !(open || work) {
            return false;
        }

        match (self.probe)() {
            Probe::Full => false,
            Probe::Ready => true,
            Probe::Starved => !self.blocked.is_empty(),
            Probe::Exhausted => work || self.workers == 0,
        }
    }
}
