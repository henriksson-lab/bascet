use num_traits::{SaturatingAdd, SaturatingSub};

pub struct Patience<T> {
    patience: T,
    growth: T,
    decay: T,
}

impl<T: Copy + SaturatingAdd + SaturatingSub> Patience<T> {
    pub fn new(init: T, growth: T, decay: T) -> Self {
        Self {
            patience: init,
            growth,
            decay,
        }
    }

    #[inline(always)]
    pub fn hit(&mut self) {
        self.patience = self.patience.saturating_add(&self.growth);
    }

    #[inline(always)]
    pub fn miss(&mut self) {
        self.patience = self.patience.saturating_sub(&self.decay);
    }

    #[inline(always)]
    pub fn patience(&self) -> T {
        self.patience
    }
}
