use std::num::NonZeroU32;
use std::sync::atomic::AtomicU32;

use crate::{AtomicPatience, Temper};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Strategy {
    Burn = 0,
    Job = 1,
    Task = 2,
}

impl Strategy {
    pub(crate) fn make_patience(self, initial: u32) -> AtomicPatience<AtomicU32> {
        use crate::pipeline::consts::*;
        match self {
            Strategy::Burn => AtomicPatience::new(
                AtomicU32::new(initial),
                BURN_PATIENCE_GROWTH,
                BURN_PATIENCE_DECAY,
            )
            .set_max(BURN_PATIENCE_MAX),
            Strategy::Job | Strategy::Task => AtomicPatience::new(
                AtomicU32::new(initial),
                JOB_PATIENCE_GROWTH,
                JOB_PATIENCE_DECAY,
            )
            .set_max(JOB_PATIENCE_MAX),
        }
    }

    pub(crate) fn idle(self, temper: Temper<u32>) {
        use crate::pipeline::consts::PARK_PATIENCE_MAX;
        match (self, temper) {
            (Strategy::Burn, _) => std::hint::spin_loop(),
            (Strategy::Task, _) => std::thread::park_timeout(PARK_PATIENCE_MAX),
            (Strategy::Job, Temper::Eager(_)) => std::hint::spin_loop(),
            (Strategy::Job, Temper::Patient(_)) => std::thread::park_timeout(PARK_PATIENCE_MAX),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parallelism {
    workers: NonZeroU32,
    min: NonZeroU32,
    max: NonZeroU32,
}

impl Parallelism {
    pub fn new(workers: NonZeroU32) -> Self {
        let max = NonZeroU32::new(u32::MAX).unwrap();
        Self {
            workers,
            min: workers,
            max,
        }
    }

    pub fn one() -> Self {
        Self::new(NonZeroU32::new(1).unwrap())
    }

    pub fn workers(&self) -> NonZeroU32 {
        self.workers
    }

    pub fn min(&self) -> NonZeroU32 {
        self.min
    }

    pub fn max(&self) -> NonZeroU32 {
        self.max
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode<T> {
    Auto(T),
    Manual(T),
}

impl<T> Mode<T> {
    pub fn auto(value: T) -> Self {
        Self::Auto(value)
    }

    pub fn manual(value: T) -> Self {
        Self::Manual(value)
    }

    pub fn is_auto(&self) -> bool {
        matches!(self, Self::Auto(_))
    }

    pub fn is_manual(&self) -> bool {
        matches!(self, Self::Manual(_))
    }

    pub fn value(&self) -> &T {
        match self {
            Self::Auto(value) | Self::Manual(value) => value,
        }
    }

    pub fn into_value(self) -> T {
        match self {
            Self::Auto(value) | Self::Manual(value) => value,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Schedule {
    pub strategy: Mode<Strategy>,
    pub parallelism: Mode<Parallelism>,
}

impl Schedule {
    pub fn auto() -> Self {
        Self {
            strategy: Mode::auto(Strategy::Task),
            parallelism: Mode::auto(Parallelism::one()),
        }
    }

    pub fn async_default() -> Self {
        Self {
            strategy: Mode::manual(Strategy::Task),
            parallelism: Mode::auto(Parallelism::one()),
        }
    }

    pub fn with_parallelism(mut self, parallelism: Mode<Parallelism>) -> Self {
        self.parallelism = parallelism;
        self
    }

    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = Mode::auto(strategy);
        self
    }

    pub fn manual_strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = Mode::manual(strategy);
        self
    }
}
