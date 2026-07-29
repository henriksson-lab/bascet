use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temper<T> {
    Eager(T),
    Patient(T),
}

pub struct AtomicPatience {
    patience: AtomicU32,
    growth: u32,
    decay: u32,
    min: u32,
    max: u32,
}

impl AtomicPatience {
    pub fn new(patience: u32, growth: u32, decay: u32) -> Self {
        Self {
            patience: AtomicU32::new(patience),
            growth,
            decay,
            min: u32::MIN,
            max: u32::MAX,
        }
    }

    pub fn set_min(mut self, min: u32) -> Self {
        self.min = min;
        self
    }

    pub fn set_max(mut self, max: u32) -> Self {
        self.max = max;
        self
    }

    #[inline(always)]
    pub fn hit(&self) -> u32 {
        let new = self
            .patience
            .load(Ordering::Relaxed)
            .saturating_add(self.growth)
            .min(self.max);
        self.patience.store(new, Ordering::Relaxed);
        new
    }

    #[inline(always)]
    pub fn miss(&self) -> Temper<u32> {
        let new = self
            .patience
            .load(Ordering::Relaxed)
            .saturating_sub(self.decay)
            .max(self.min);
        self.patience.store(new, Ordering::Relaxed);
        if new <= self.min {
            Temper::Patient(new)
        } else {
            Temper::Eager(new)
        }
    }

    #[inline(always)]
    pub fn patience(&self) -> u32 {
        self.patience.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn min(&self) -> u32 {
        self.min
    }

    #[inline(always)]
    pub fn max(&self) -> u32 {
        self.max
    }
}
