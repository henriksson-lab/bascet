use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone, Copy, Debug)]
pub struct Pressure {
    pressure: u32,
    level: u32,
    min: u32,
    strain: NonZeroU32,
    growth: u32,
    decay: u32,
}

pub struct AtomicPressure {
    pressure: AtomicU32,
    level: AtomicU32,
    min: u32,
    strain: NonZeroU32,
    pub(crate) growth: AtomicU32,
    pub(crate) decay: AtomicU32,
}

pub(crate) struct PressureSnapshot {
    pub(crate) pressure: u32,
    pub(crate) strain: u32,
}

impl Pressure {
    pub fn new(initial: u32, min: u32, strain: NonZeroU32, growth: u32, decay: u32) -> Self {
        let initial = initial.max(min);
        Self {
            pressure: initial,
            level: band(initial, strain.get()),
            min,
            strain,
            growth,
            decay,
        }
    }

    pub fn miss(&mut self) -> Option<NonZeroU32> {
        self.pressure = self.pressure.saturating_add(self.growth);
        let next = band(self.pressure, self.strain.get());
        if next <= self.level {
            return None;
        }
        self.level = next;
        NonZeroU32::new(next)
    }

    pub fn hit(&mut self) {
        self.pressure = self.pressure.saturating_sub(self.decay).max(self.min);
        self.level = self.level.min(band(self.pressure, self.strain.get()));
    }

    pub fn recover(&mut self) {
        self.pressure = self.min;
        self.level = band(self.min, self.strain.get());
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub fn strain(&self) -> usize {
        strain(self.level)
    }
}

impl AtomicPressure {
    pub fn new(initial: u32, min: u32, strain: NonZeroU32, growth: u32, decay: u32) -> Self {
        let initial = initial.max(min);
        Self {
            pressure: AtomicU32::new(initial),
            level: AtomicU32::new(band(initial, strain.get())),
            min,
            strain,
            growth: AtomicU32::new(growth),
            decay: AtomicU32::new(decay),
        }
    }

    #[inline(always)]
    pub fn miss(&self) -> Option<NonZeroU32> {
        let strain = self.strain.get();
        let growth = self.growth.load(Ordering::Relaxed);
        let mut next = self.min;

        self.pressure
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                next = old.saturating_add(growth);
                Some(next)
            })
            .ok();

        let next_level = band(next, strain);

        loop {
            let level = self.level.load(Ordering::Acquire);
            if next_level <= level {
                return None;
            }

            if self
                .level
                .compare_exchange(level, next_level, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return NonZeroU32::new(next_level);
            }
        }
    }

    #[inline(always)]
    pub fn level(&self) -> u32 {
        self.level.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn snapshot(&self) -> PressureSnapshot {
        PressureSnapshot {
            pressure: self.pressure.load(Ordering::Acquire),
            strain: self.strain.get(),
        }
    }

    #[inline(always)]
    pub fn strain(&self) -> usize {
        strain(self.level())
    }

    #[inline(always)]
    pub fn hit(&self) {
        let min = self.min;
        let decay = self.decay.load(Ordering::Relaxed);
        let strain = self.strain.get();
        let mut next = min;

        self.pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                next = old.saturating_sub(decay).max(min);
                Some(next)
            })
            .ok();

        self.lower(band(next, strain));
    }

    #[inline(always)]
    pub fn recover(&self) {
        self.pressure.store(self.min, Ordering::Relaxed);
        self.lower(band(self.min, self.strain.get()));
    }

    #[inline(always)]
    fn lower(&self, target: u32) {
        self.level
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (target < old).then_some(target)
            })
            .ok();
    }
}

#[inline(always)]
fn strain(level: u32) -> usize {
    match level {
        0 => 0,
        level => 1usize
            .checked_shl(level.saturating_sub(1))
            .unwrap_or(usize::MAX),
    }
}

#[inline(always)]
fn band(pressure: u32, strain: u32) -> u32 {
    if pressure < strain {
        0
    } else {
        1 + (pressure / strain).ilog2()
    }
}
