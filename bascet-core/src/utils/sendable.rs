#[repr(transparent)]
pub struct Sendable<T>(T);

unsafe impl<T> Send for Sendable<T> {}
impl<T> Sendable<T> {
    #[inline(always)]
    pub const unsafe fn new(value: T) -> Self {
        Sendable(value)
    }

    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.0
    }

    #[inline(always)]
    pub fn as_ref(&self) -> &T {
        &self.0
    }

    #[inline(always)]
    pub fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> std::ops::Deref for Sendable<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Sendable<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
