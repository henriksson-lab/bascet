use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::Mutex;

use crate::pipeline::gather::Probe;
use crate::runtime::Tier;
use crate::schedule::Schedule;
use crate::worker::State;

pub(crate) trait Assignment: Send {
    fn drive(&mut self, schedule: &Schedule, tier: Tier) -> State;
    fn layer(&self) -> usize;
}

pub(crate) struct Layer {
    pub(crate) dispatch: Option<Arc<Mutex<dyn FnMut() -> Box<dyn Assignment> + Send>>>,
    pub(crate) probe: Box<dyn Fn() -> Probe + Send>,
    pub(crate) blocked: VecDeque<Box<dyn Assignment>>,
    pub(crate) parked: VecDeque<Box<dyn Assignment>>,
    pub(crate) workers: usize,
    pub(crate) pass: u64,
    pub(crate) preempt: Arc<AtomicU8>,
}
