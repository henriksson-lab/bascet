use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Pressure {
    pressure: AtomicU32,
    level: AtomicU32,
    min: u32,
    strain: NonZeroU32,
    pub(crate) growth: AtomicU32,
    pub(crate) decay: AtomicU32,
}

impl Pressure {
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
    pub fn miss(&self) -> bool {
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
                return false;
            }

            if self
                .level
                .compare_exchange(level, next_level, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
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
    fn lower(&self, target: u32) {
        self.level
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (target < old).then_some(target)
            })
            .ok();
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
