#[derive(Clone, Copy)]
pub struct UnsafePtr<T>(*const T);

#[derive(Clone, Copy)]
pub struct UnsafeMutPtr<T>(*mut T);

unsafe impl<T> Send for UnsafePtr<T> {}
unsafe impl<T> Sync for UnsafePtr<T> {}
unsafe impl<T> Send for UnsafeMutPtr<T> {}
unsafe impl<T> Sync for UnsafeMutPtr<T> {}

impl<T> UnsafePtr<T> {
    #[inline(always)]
    pub fn new(ptr: *const T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const T {
        self.0
    }
}

impl<T> UnsafeMutPtr<T> {
    #[inline(always)]
    pub fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *mut T {
        self.0
    }
}
