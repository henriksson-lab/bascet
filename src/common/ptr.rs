#[derive(PartialEq)]
pub struct UnsafePtr<T>(*const T);

#[derive(PartialEq)]
pub struct UnsafeMutPtr<T>(*mut T);

unsafe impl<T> Send for UnsafePtr<T> {}
unsafe impl<T> Sync for UnsafePtr<T> {}
unsafe impl<T> Send for UnsafeMutPtr<T> {}
unsafe impl<T> Sync for UnsafeMutPtr<T> {}

impl<T> Clone for UnsafePtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Clone for UnsafeMutPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for UnsafePtr<T> {}
impl<T> Copy for UnsafeMutPtr<T> {}

impl<T> UnsafePtr<T> {
    #[inline(always)]
    pub fn new(ptr: *const T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self(std::ptr::null())
    }

    #[inline(always)]
    pub fn ptr(self) -> *const T {
        self.0
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

impl<T> UnsafeMutPtr<T> {
    #[inline(always)]
    pub fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self(std::ptr::null_mut())
    }

    #[inline(always)]
    pub fn mut_ptr(self) -> *mut T {
        self.0
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
}
