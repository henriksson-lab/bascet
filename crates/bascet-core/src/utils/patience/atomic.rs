use std::sync::atomic::Ordering;

use atomic_traits::{Atomic, NumOps};
use num_traits::{Bounded, SaturatingAdd, SaturatingSub};

use crate::utils::patience::Temper;

pub struct AtomicPatience<A: NumOps>
where
    <A as Atomic>::Type: Copy + SaturatingAdd + SaturatingSub + Ord + Bounded,
{
    patience: A,
    growth: <A as Atomic>::Type,
    decay: <A as Atomic>::Type,
    min: <A as Atomic>::Type,
    max: <A as Atomic>::Type,
}

impl<A: NumOps> AtomicPatience<A>
where
    <A as Atomic>::Type: Copy + SaturatingAdd + SaturatingSub + Ord + Bounded,
{
    pub fn new(patience: A, growth: <A as Atomic>::Type, decay: <A as Atomic>::Type) -> Self {
        Self {
            patience,
            growth,
            decay,
            min: <A as Atomic>::Type::min_value(),
            max: <A as Atomic>::Type::max_value(),
        }
    }

    pub fn set_min(mut self, min: <A as Atomic>::Type) -> Self {
        self.min = min;
        self
    }

    pub fn set_max(mut self, max: <A as Atomic>::Type) -> Self {
        self.max = max;
        self
    }

    #[inline(always)]
    pub fn hit(&self) -> <A as Atomic>::Type {
        let growth = self.growth;
        let max = self.max;
        let mut new = max;
        let _ = self
            .patience
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                new = v.saturating_add(&growth).min(max);
                Some(new)
            });
        new
    }

    #[inline(always)]
    pub fn miss(&self) -> Temper<<A as Atomic>::Type> {
        let decay = self.decay;
        let min = self.min;
        let mut new = min;
        let _ = self
            .patience
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                new = v.saturating_sub(&decay).max(min);
                Some(new)
            });
        if new <= min {
            Temper::Patient(new)
        } else {
            Temper::Eager(new)
        }
    }

    #[inline(always)]
    pub fn patience(&self) -> <A as Atomic>::Type {
        self.patience.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn min(&self) -> <A as Atomic>::Type {
        self.min
    }

    #[inline(always)]
    pub fn max(&self) -> <A as Atomic>::Type {
        self.max
    }
}
