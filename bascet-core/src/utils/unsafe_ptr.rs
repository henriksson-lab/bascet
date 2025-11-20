use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

#[repr(transparent)]
pub struct UnsafePtr<T>(NonNull<T>);
impl<T> Copy for UnsafePtr<T> {}
unsafe impl<T> Send for UnsafePtr<T> {}
unsafe impl<T> Sync for UnsafePtr<T> {}

impl<T> UnsafePtr<T> {
    #[inline(always)]
    pub fn new(ptr: *mut T) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }

    #[inline(always)]
    pub unsafe fn new_unchecked(ptr: *mut T) -> Self {
        Self(NonNull::new_unchecked(ptr))
    }

    #[inline(always)]
    pub fn from_ref(r: &T) -> Self {
        Self(NonNull::from(r))
    }

    #[inline(always)]
    pub fn from_mut(r: &mut T) -> Self {
        Self(NonNull::from(r))
    }
}

impl<T> Deref for UnsafePtr<T> {
    type Target = NonNull<T>;

    #[inline(always)]
    fn deref(&self) -> &NonNull<T> {
        &self.0
    }
}

impl<T> DerefMut for UnsafePtr<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut NonNull<T> {
        &mut self.0
    }
}

impl<T> Clone for UnsafePtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> From<&T> for UnsafePtr<T> {
    fn from(r: &T) -> Self {
        Self::from_ref(r)
    }
}

impl<T> From<&mut T> for UnsafePtr<T> {
    fn from(r: &mut T) -> Self {
        Self::from_mut(r)
    }
}
