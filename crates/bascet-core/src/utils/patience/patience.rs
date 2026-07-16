use std::num::NonZeroU64;

pub struct Patience<T> {
    patience: T,
    growth: T,
    decay: T,
    min: T,
    max: T,
}

pub trait Value: Copy + Ord {
    fn min_value() -> Self;
    fn max_value() -> Self;
    fn add(self, rhs: Self) -> Self;
    fn sub(self, rhs: Self) -> Self;
}

impl<T: Value> Patience<T> {
    pub fn new(init: T, growth: T, decay: T) -> Self {
        Self {
            patience: init,
            growth,
            decay,
            min: T::min_value(),
            max: T::max_value(),
        }
    }

    pub fn set_min(mut self, min: T) -> Self {
        self.min = min;
        self
    }

    pub fn set_max(mut self, max: T) -> Self {
        self.max = max;
        self
    }

    #[inline(always)]
    pub fn hit(&mut self) -> T {
        self.patience = self.patience.add(self.growth).min(self.max);
        self.patience
    }

    #[inline(always)]
    pub fn miss(&mut self) -> T {
        self.patience = self.patience.sub(self.decay).max(self.min);
        self.patience
    }

    #[inline(always)]
    pub fn patience(&self) -> T {
        self.patience
    }

    #[inline(always)]
    pub fn min(&self) -> T {
        self.min
    }

    #[inline(always)]
    pub fn max(&self) -> T {
        self.max
    }
}

impl Value for u64 {
    fn min_value() -> Self {
        u64::MIN
    }

    fn max_value() -> Self {
        u64::MAX
    }

    fn add(self, rhs: Self) -> Self {
        self.saturating_add(rhs)
    }

    fn sub(self, rhs: Self) -> Self {
        self.saturating_sub(rhs)
    }
}

impl Value for u32 {
    fn min_value() -> Self {
        u32::MIN
    }

    fn max_value() -> Self {
        u32::MAX
    }

    fn add(self, rhs: Self) -> Self {
        self.saturating_add(rhs)
    }

    fn sub(self, rhs: Self) -> Self {
        self.saturating_sub(rhs)
    }
}

impl Value for usize {
    fn min_value() -> Self {
        usize::MIN
    }

    fn max_value() -> Self {
        usize::MAX
    }

    fn add(self, rhs: Self) -> Self {
        self.saturating_add(rhs)
    }

    fn sub(self, rhs: Self) -> Self {
        self.saturating_sub(rhs)
    }
}

impl Value for NonZeroU64 {
    fn min_value() -> Self {
        NonZeroU64::MIN
    }

    fn max_value() -> Self {
        NonZeroU64::MAX
    }

    fn add(self, rhs: Self) -> Self {
        NonZeroU64::new(self.get().saturating_add(rhs.get())).unwrap_or(NonZeroU64::MAX)
    }

    fn sub(self, rhs: Self) -> Self {
        NonZeroU64::new(self.get().saturating_sub(rhs.get()).max(1)).unwrap()
    }
}
