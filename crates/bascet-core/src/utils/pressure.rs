use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Pressure {
    pressure: AtomicU32,
    level: AtomicU32,
    min: u32,
    strain: NonZeroU32,
    growth: u32,
    decay: u32,
}

impl Pressure {
    pub fn new(initial: u32, min: u32, strain: NonZeroU32, growth: u32, decay: u32) -> Self {
        let initial = initial.max(min);
        Self {
            pressure: AtomicU32::new(initial),
            level: AtomicU32::new(band(initial, strain.get())),
            min,
            strain,
            growth,
            decay,
        }
    }

    #[inline(always)]
    pub fn miss(&self) -> Option<NonZeroU32> {
        let mut next = self.min;
        self.pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                next = old.saturating_add(self.growth);
                Some(next)
            })
            .ok();
        let next_level = band(next, self.strain.get());
        loop {
            let level = self.level.load(Ordering::Relaxed);
            if next_level <= level {
                return None;
            }
            if self
                .level
                .compare_exchange(level, next_level, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return NonZeroU32::new(next_level);
            }
        }
    }

    #[inline(always)]
    pub fn hit(&self) {
        let mut next = self.min;
        self.pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                next = old.saturating_sub(self.decay).max(self.min);
                Some(next)
            })
            .ok();
        self.lower(band(next, self.strain.get()));
    }

    pub fn recover(&self) {
        let mut next = self.min;
        self.pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                next = (old / 2).max(self.min);
                Some(next)
            })
            .ok();
        self.lower(band(next, self.strain.get()));
    }

    #[inline(always)]
    fn lower(&self, target: u32) {
        self.level
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (target < old).then_some(target)
            })
            .ok();
    }

    #[inline(always)]
    pub fn level(&self) -> u32 {
        self.level.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn value(&self) -> u32 {
        self.pressure.load(Ordering::Relaxed).max(self.min)
    }

    #[inline(always)]
    pub fn strain(&self) -> usize {
        strain(self.level())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    fn pressure() -> Pressure {
        Pressure::new(0, 0, NonZeroU32::new(4).unwrap(), 1, 1)
    }

    #[test]
    fn miss_emits_only_on_band_increase() {
        let p = pressure();
        let mut emissions = 0;
        for _ in 0..64 {
            if p.miss().is_some() {
                emissions += 1;
            }
        }
        assert!(emissions >= 2);
        assert!(emissions <= 6);
        assert_eq!(p.level(), band(64, 4));
    }

    #[test]
    fn hit_decays_and_lowers_level() {
        let p = pressure();
        for _ in 0..64 {
            p.miss();
        }
        for _ in 0..64 {
            p.hit();
        }
        assert_eq!(p.level(), 0);
    }

    #[test]
    fn hit_clamps_at_min() {
        let p = Pressure::new(2, 2, NonZeroU32::new(4).unwrap(), 1, 1);
        for _ in 0..16 {
            p.hit();
        }
        assert_eq!(p.value(), 2);
    }
}
