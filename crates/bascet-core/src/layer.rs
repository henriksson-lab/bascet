use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use core_affinity::CoreId;

use crate::apply::Apply;
use crate::pipeline::consts::{
    DEMAND_DECAY, DEMAND_DECAY_MAX, DEMAND_DECAY_MIN, DEMAND_GROWTH, DEMAND_GROWTH_MAX,
    DEMAND_GROWTH_MIN, DEMAND_INITIAL, DEMAND_MIN, DEMAND_STRAIN, SENSITIVITY_DOWN, SENSITIVITY_UP,
};
use crate::pipeline::scheduler::{Id, Motivation, Signal};
use crate::schedule::{Mode, Parallelism, Schedule, Strategy};
use crate::utils::{AtomicPatience, AtomicPressure};

pub struct Layer<A: Apply> {
    apply: A,
    schedule: Schedule,
    pub(crate) parallelism: Mode<Parallelism>,
    pub(crate) strategy: Mode<Strategy>,
    pub(crate) active: Arc<AtomicU64>,
    pub(crate) handles: Vec<Handle>,
    pub(crate) requested: Arc<AtomicU64>,
    pub(crate) fulfilled: Arc<AtomicU64>,
    pub(crate) demand: Arc<AtomicPressure>,
    pub(crate) activity: Arc<Tally>,
    pressure_scale: AtomicU32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum Activity {
    Busy = 0,
    Starved = 1,
    Backpressure = 2,
    Exiting = 3,
}

#[derive(Default)]
pub(crate) struct Tally {
    busy: AtomicU64,
    starved: AtomicU64,
    backpressure: AtomicU64,
    exiting: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Snapshot {
    pub(crate) busy: u64,
    pub(crate) starved: u64,
    pub(crate) backpressure: u64,
}

impl Tally {
    pub(crate) fn increment(&self, activity: Activity) {
        self.counter(activity).fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decrement(&self, activity: Activity) {
        self.counter(activity).fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            busy: self.busy.load(Ordering::Relaxed),
            starved: self.starved.load(Ordering::Relaxed),
            backpressure: self.backpressure.load(Ordering::Relaxed),
        }
    }

    fn counter(&self, activity: Activity) -> &AtomicU64 {
        match activity {
            Activity::Busy => &self.busy,
            Activity::Starved => &self.starved,
            Activity::Backpressure => &self.backpressure,
            Activity::Exiting => &self.exiting,
        }
    }
}

pub(crate) struct WorkerState {
    pub parallelism: Mode<Parallelism>,
    pub(crate) inner_strategy: AtomicU8,
    pub(crate) inner_core_id: AtomicUsize,
    pub(crate) inner_patience: AtomicPatience<AtomicU32>,
    pub(crate) requested: Arc<AtomicU64>,
    pub(crate) fulfilled: Arc<AtomicU64>,
    pub(crate) demand: Arc<AtomicPressure>,
    pub(crate) activity: Arc<Tally>,
    pub(crate) inner_activity: AtomicU8,
}

impl WorkerState {
    pub(crate) fn new(
        parallelism: Mode<Parallelism>,
        strategy: Strategy,
        demand: Arc<AtomicPressure>,
        requested: Arc<AtomicU64>,
        fulfilled: Arc<AtomicU64>,
        activity: Arc<Tally>,
    ) -> Self {
        use crate::pipeline::consts::*;
        let initial = match strategy {
            Strategy::Burn => BURN_PATIENCE_INITIAL,
            Strategy::Job | Strategy::Task => JOB_PATIENCE_INITIAL,
        };
        Self {
            parallelism,
            inner_strategy: AtomicU8::new(strategy as u8),
            inner_core_id: AtomicUsize::new(usize::MAX),
            inner_patience: strategy.make_patience(initial),
            requested,
            fulfilled,
            demand,
            activity,
            inner_activity: AtomicU8::new(u8::MAX),
        }
    }

    pub(crate) fn pinned(&self) -> bool {
        self.parallelism.is_manual()
    }

    #[inline(always)]
    pub(crate) fn strategy(&self) -> Strategy {
        match self.inner_strategy.load(Ordering::Relaxed) {
            0 => Strategy::Burn,
            1 => Strategy::Job,
            2 => Strategy::Task,
            _ => unreachable!(),
        }
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

    pub(crate) fn set_activity(&self, activity: Activity) {
        let next = activity as u8;
        let prev = self.inner_activity.swap(next, Ordering::Relaxed);
        if prev == next {
            return;
        }
        if let Some(prev) = Self::decode_activity(prev) {
            self.activity.decrement(prev);
        }
        self.activity.increment(activity);
    }

    pub(crate) fn clear_activity(&self) {
        let prev = self.inner_activity.swap(u8::MAX, Ordering::Relaxed);
        if let Some(prev) = Self::decode_activity(prev) {
            self.activity.decrement(prev);
        }
    }

    fn decode_activity(value: u8) -> Option<Activity> {
        match value {
            0 => Some(Activity::Busy),
            1 => Some(Activity::Starved),
            2 => Some(Activity::Backpressure),
            3 => Some(Activity::Exiting),
            _ => None,
        }
    }

    #[inline(always)]
    pub(crate) fn request(&self) -> Option<Signal> {
        self.requested.fetch_add(1, Ordering::Relaxed);
        self.demand
            .miss()
            .map(|level| Signal::Demand(Arc::clone(&self.demand), level))
    }

    #[inline(always)]
    pub(crate) fn fulfill(&self) {
        self.fulfilled.fetch_add(1, Ordering::Relaxed);
        if self.outstanding() == 0 {
            self.demand.recover();
        } else {
            self.demand.hit();
        }
    }

    #[inline(always)]
    pub(crate) fn outstanding(&self) -> u64 {
        let requested = self.requested.load(Ordering::Relaxed);
        let fulfilled = self.fulfilled.load(Ordering::Relaxed);
        requested.saturating_sub(fulfilled)
    }
}

impl Drop for WorkerState {
    fn drop(&mut self) {
        self.clear_activity();
    }
}

pub(crate) struct Handle {
    pub(crate) id: Id,
    pub(crate) tier: Strategy,
    pub(crate) signal: Option<Signal>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Feedback {
    Eager,
    Late,
    Stable,
}

const PRESSURE_SCALE_INITIAL: u32 = 1_000;
const PRESSURE_SCALE_MIN: u32 = 125;
const PRESSURE_SCALE_MAX: u32 = 8_000;

impl<A: Apply> Layer<A> {
    pub(crate) fn new(apply: A, schedule: Schedule) -> Self {
        Self {
            apply,
            schedule,
            parallelism: schedule.parallelism,
            strategy: schedule.strategy,
            active: Arc::new(AtomicU64::new(0)),
            handles: Vec::new(),
            requested: Arc::new(AtomicU64::new(0)),
            fulfilled: Arc::new(AtomicU64::new(0)),
            activity: Arc::new(Tally::default()),
            demand: Arc::new(AtomicPressure::new(
                DEMAND_INITIAL,
                DEMAND_MIN,
                std::num::NonZeroU32::new(DEMAND_STRAIN).unwrap(),
                DEMAND_GROWTH,
                DEMAND_DECAY,
            )),
            pressure_scale: AtomicU32::new(PRESSURE_SCALE_INITIAL),
        }
    }

    pub(crate) fn apply(&self) -> &A {
        &self.apply
    }

    pub(crate) fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub(crate) fn make_state(&self, strategy: Strategy) -> WorkerState {
        WorkerState::new(
            self.parallelism,
            strategy,
            Arc::clone(&self.demand),
            Arc::clone(&self.requested),
            Arc::clone(&self.fulfilled),
            Arc::clone(&self.activity),
        )
    }

    pub(crate) fn push(&mut self, handle: Handle) {
        self.handles.push(handle);
    }

    pub(crate) fn manual_parallelism(&self) -> bool {
        self.parallelism.is_manual()
    }

    pub(crate) fn active_count(&self) -> usize {
        self.handles.len()
    }

    pub(crate) fn leased(&self) -> usize {
        self.handles.len()
    }

    pub(crate) fn headroom(&self) -> usize {
        if self.manual_parallelism() {
            0
        } else {
            (self.parallelism.value().max().get() as usize).saturating_sub(self.active_count())
        }
    }

    pub(crate) fn surplus(&self) -> usize {
        if self.manual_parallelism() {
            0
        } else {
            self.leased()
                .saturating_sub(self.parallelism.value().min().get() as usize)
        }
    }

    pub(crate) fn learn(&self, signal: &Signal, feedback: Feedback) {
        let pressure = signal.pressure();
        match (signal.motivation(), feedback) {
            (Motivation::Demand, Feedback::Eager) => {
                Self::tune(
                    &pressure.growth,
                    SENSITIVITY_DOWN,
                    DEMAND_GROWTH_MIN,
                    DEMAND_GROWTH_MAX,
                );
                Self::tune(
                    &pressure.decay,
                    SENSITIVITY_UP,
                    DEMAND_DECAY_MIN,
                    DEMAND_DECAY_MAX,
                );
            }
            (Motivation::Demand, Feedback::Late) => {
                Self::tune(
                    &pressure.growth,
                    SENSITIVITY_UP,
                    DEMAND_GROWTH_MIN,
                    DEMAND_GROWTH_MAX,
                );
                Self::tune(
                    &pressure.decay,
                    SENSITIVITY_DOWN,
                    DEMAND_DECAY_MIN,
                    DEMAND_DECAY_MAX,
                );
            }
            (Motivation::Pressure, Feedback::Eager) => {
                Self::tune(
                    &self.pressure_scale,
                    SENSITIVITY_UP,
                    PRESSURE_SCALE_MIN,
                    PRESSURE_SCALE_MAX,
                );
            }
            (Motivation::Pressure, Feedback::Late) => {
                Self::tune(
                    &self.pressure_scale,
                    SENSITIVITY_DOWN,
                    PRESSURE_SCALE_MIN,
                    PRESSURE_SCALE_MAX,
                );
            }
            (_, Feedback::Stable) => {}
        }
    }

    pub(crate) fn runnable(&self) -> bool {
        self.activity.snapshot().busy > 0
    }

    pub(crate) fn useful_width(&self) -> usize {
        let activity = self.activity.snapshot();
        let min = self.parallelism.value().min().get() as usize;
        let max = self.parallelism.value().max().get() as usize;
        let busy = activity.busy as usize;
        let limited = activity.starved > activity.busy || activity.backpressure > activity.busy;
        let useful = if limited {
            busy
        } else {
            busy.saturating_add(1)
        };

        useful.clamp(min, max)
    }

    fn tune(value: &AtomicU32, factor: (u32, u32), min: u32, max: u32) {
        let (num, den) = factor;
        value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                let scaled = (old as u64).saturating_mul(num as u64) / den as u64;
                let mut next = scaled.clamp(min as u64, max as u64) as u32;

                if num > den && next == old && old < max {
                    next = old + 1;
                } else if num < den && next == old && old > min {
                    next = old - 1;
                }

                Some(next)
            })
            .ok();
    }

    pub(crate) fn desired_for(&self, signal: &Signal) -> usize {
        match signal.motivation() {
            Motivation::Demand => signal.pressure().strain(),
            Motivation::Pressure => self.desired_for_pressure(signal),
        }
    }

    fn desired_for_pressure(&self, signal: &Signal) -> usize {
        let active = self
            .active_count()
            .max(self.parallelism.value().min().get() as usize)
            .max(1);
        let active_for = self.active_for(signal, Strategy::Task);
        let snapshot = signal.pressure().snapshot();
        let scale = self.pressure_scale.load(Ordering::Relaxed).max(1) as f64 / 1_000.0;
        let ratio = snapshot.pressure as f64 / (snapshot.strain.max(1) as f64 * active as f64);
        let delta = ((active as f64) * ratio / scale).ceil() as usize;

        active_for
            .saturating_add(delta)
            .min(self.parallelism.value().max().get() as usize)
    }

    pub(crate) fn learn_handle(&mut self, handle: &Handle, processed: u64) {
        let Some(signal) = handle.signal.as_ref() else {
            return;
        };

        let feedback = if processed == 0 {
            Feedback::Eager
        } else {
            Feedback::Stable
        };
        self.learn(signal, feedback);
    }

    pub(crate) fn get(&self, id: Id) -> Option<&Handle> {
        self.handles.iter().find(|handle| handle.id == id)
    }

    pub(crate) fn active_for(&self, signal: &Signal, target: Strategy) -> usize {
        self.handles
            .iter()
            .filter_map(|handle| {
                let other = handle.signal.as_ref()?;
                let covered = match target {
                    Strategy::Burn => handle.tier == Strategy::Burn,
                    Strategy::Job => matches!(handle.tier, Strategy::Burn | Strategy::Job),
                    Strategy::Task => true,
                };
                (signal.same_source(other) && covered).then_some(())
            })
            .count()
    }

    pub(crate) fn recovered(&self, signal: &Signal) -> bool {
        signal.recovered()
    }

    pub(crate) fn candidate_for_signal(&self, strategy: Strategy, signal: &Signal) -> Option<Id> {
        self.handles.iter().find_map(|handle| {
            (handle.tier == strategy
                && handle
                    .signal
                    .as_ref()
                    .is_some_and(|other| signal.same_source(other)))
            .then_some(handle.id)
        })
    }

    pub(crate) fn take_handle(&mut self, id: Id) -> Option<Handle> {
        let idx = self.handles.iter().position(|handle| handle.id == id)?;
        Some(self.handles.remove(idx))
    }
}
