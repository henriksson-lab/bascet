use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

use core_affinity::CoreId;
use crossbeam::utils::CachePadded;

use crate::layer::Layer;
use crate::owned::Owned;
use crate::pipe::Pipe;
use crate::set::Set;
use crate::{AtomicPatience, Pressure};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Strategy {
    Burn = 0,
    Task = 1,
    Job = 2,
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
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Auto {
        countof_workers: NonZeroU32,
        countof_min: NonZeroU32,
        countof_max: NonZeroU32,
    },
    Manual {
        countof_workers: NonZeroU32,
        countof_min: NonZeroU32,
        countof_max: NonZeroU32,
    },
}

impl Mode {
    pub fn auto() -> Self {
        let one = NonZeroU32::new(1).unwrap();
        let max = NonZeroU32::new(u32::MAX).unwrap();
        Mode::Auto {
            countof_workers: one,
            countof_min: one,
            countof_max: max,
        }
    }

    pub fn manual() -> Self {
        let one = NonZeroU32::new(1).unwrap();
        let max = NonZeroU32::new(u32::MAX).unwrap();
        Mode::Manual {
            countof_workers: one,
            countof_min: one,
            countof_max: max,
        }
    }

    pub fn auto_countof(countof_workers: NonZeroU32) -> Self {
        let max = NonZeroU32::new(u32::MAX).unwrap();
        Mode::Auto {
            countof_workers,
            countof_min: countof_workers,
            countof_max: max,
        }
    }

    pub fn manual_countof(countof_workers: NonZeroU32) -> Self {
        let max = NonZeroU32::new(u32::MAX).unwrap();
        Mode::Manual {
            countof_workers,
            countof_min: countof_workers,
            countof_max: max,
        }
    }

    pub fn countof_workers(&self) -> NonZeroU32 {
        match self {
            Mode::Auto {
                countof_workers, ..
            }
            | Mode::Manual {
                countof_workers, ..
            } => *countof_workers,
        }
    }

    pub fn countof_min(&self) -> NonZeroU32 {
        match self {
            Mode::Auto { countof_min, .. } | Mode::Manual { countof_min, .. } => *countof_min,
        }
    }

    pub fn countof_max(&self) -> NonZeroU32 {
        match self {
            Mode::Auto { countof_max, .. } | Mode::Manual { countof_max, .. } => *countof_max,
        }
    }
}

pub struct Scheduling {
    // Scheduler-read fields are isolated from worker writes below.
    pub mode: Mode,
    pub(crate) inner_strategy: AtomicU8,
    pub(crate) inner_core_id: AtomicUsize,
    // Worker-write fields — on their own cache line to avoid invalidating the line above.
    pub(crate) countof_idle: CachePadded<AtomicU32>,
    pub(crate) inner_patience: AtomicPatience<AtomicU32>,
    pub(crate) demand: Arc<Pressure>,
}

impl Scheduling {
    pub(crate) fn new(mode: Mode, strategy: Strategy, demand: Arc<Pressure>) -> Self {
        use crate::pipeline::consts::*;
        let initial = match strategy {
            Strategy::Burn => BURN_PATIENCE_INITIAL,
            Strategy::Job | Strategy::Task => JOB_PATIENCE_INITIAL,
        };
        Self {
            mode,
            inner_strategy: AtomicU8::new(strategy as u8),
            inner_core_id: AtomicUsize::new(usize::MAX),
            countof_idle: CachePadded::new(AtomicU32::new(0)),
            inner_patience: strategy.make_patience(initial),
            demand,
        }
    }

    pub(crate) fn pinned(&self) -> bool {
        matches!(self.mode, Mode::Manual { .. })
    }

    #[inline(always)]
    pub(crate) fn strategy(&self) -> Strategy {
        match self.inner_strategy.load(Ordering::Relaxed) {
            0 => Strategy::Burn,
            1 => Strategy::Task,
            2 => Strategy::Job,
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub(crate) fn set_strategy(&self, s: Strategy) {
        self.inner_strategy.store(s as u8, Ordering::Relaxed);
    }

    pub(crate) fn core_id(&self) -> Option<CoreId> {
        let v = self.inner_core_id.load(Ordering::Relaxed);
        if v == usize::MAX {
            None
        } else {
            Some(CoreId { id: v })
        }
    }

    pub(crate) fn set_core_id(&self, id: Option<CoreId>) {
        self.inner_core_id
            .store(id.map_or(usize::MAX, |c| c.id), Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn patience(&self) -> &AtomicPatience<AtomicU32> {
        &self.inner_patience
    }

    #[inline(always)]
    pub(crate) fn demand(&self) -> &Pressure {
        &self.demand
    }
}

pub enum Emit<T> {
    None,
    Value(T),
    Error(Box<dyn std::error::Error + Send + Sync>),
}

pub enum Output<T> {
    Value(T),
    Shutdown,
    Error(Box<dyn std::error::Error + Send + Sync>),
}

pub trait Stage: Layer + Owned<Mode, Value = Mode> + Owned<Strategy, Value = Strategy> {
    type Input<'a>;
    type Output;

    fn apply<W: Set>(&mut self, input: Self::Input<'_>) -> Emit<Self::Output>;
}

impl<S: Stage> Owned<Mode> for Pipe<S, ()> {
    type Value = Mode;
    fn owned(&self) -> Mode {
        <S as Owned<Mode>>::owned(&self.0)
    }
}

impl<S, Tail> Owned<Mode> for Pipe<S, Tail>
where
    S: Stage,
    Tail: Stage,
    for<'a> &'a Tail::Output: Into<S::Input<'a>>,
{
    type Value = Mode;
    fn owned(&self) -> Mode {
        <S as Owned<Mode>>::owned(&self.0)
    }
}

impl<S: Stage> Owned<Strategy> for Pipe<S, ()> {
    type Value = Strategy;
    fn owned(&self) -> Strategy {
        <S as Owned<Strategy>>::owned(&self.0)
    }
}

impl<S, Tail> Owned<Strategy> for Pipe<S, Tail>
where
    S: Stage,
    Tail: Stage,
    for<'a> &'a Tail::Output: Into<S::Input<'a>>,
{
    type Value = Strategy;
    fn owned(&self) -> Strategy {
        <S as Owned<Strategy>>::owned(&self.0)
    }
}

impl<S: Stage> Layer for Pipe<S, ()> {
    type Provides = S::Provides;
    type Requires = S::Requires;
    type Resources = S::Resources;
}

impl<S, Tail> Layer for Pipe<S, Tail>
where
    S: Stage,
    Tail: Stage,
    for<'a> &'a Tail::Output: Into<S::Input<'a>>,
{
    type Provides = S::Provides;
    type Requires = Tail::Requires;
    type Resources = ();
}

impl<S: Stage> Stage for Pipe<S, ()> {
    type Input<'a> = S::Input<'a>;
    type Output = S::Output;

    fn apply<W: Set>(&mut self, input: Self::Input<'_>) -> Emit<Self::Output> {
        self.0.apply::<W>(input)
    }
}

impl<S, Tail> Stage for Pipe<S, Tail>
where
    S: Stage,
    Tail: Stage,
    for<'a> &'a Tail::Output: Into<S::Input<'a>>,
{
    type Input<'a> = Tail::Input<'a>;
    type Output = S::Output;

    fn apply<W: Set>(&mut self, input: Self::Input<'_>) -> Emit<Self::Output> {
        match self.1.apply::<W>(input) {
            Emit::None => Emit::None,
            Emit::Error(e) => Emit::Error(e),
            Emit::Value(out) => self.0.apply::<W>((&out).into()),
        }
    }
}
