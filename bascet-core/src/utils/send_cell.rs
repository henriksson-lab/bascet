#[repr(transparent)]
pub struct SendCell<T>(T);

unsafe impl<T> Send for SendCell<T> {}

impl<T> SendCell<T> {
    #[inline(always)]
    pub const fn new(value: T) -> Self {
        SendCell(value)
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

impl<T> std::ops::Deref for SendCell<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for SendCell<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
