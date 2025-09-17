pub struct UnsafePtr<T>(*mut T);
impl<T> UnsafePtr<T> {
    #[inline(always)]
    pub fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    #[inline(always)]
    pub fn from_const(ptr: *const T) -> Self {
        Self(ptr as *mut T)
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self(std::ptr::null_mut())
    }

    #[inline(always)]
    pub fn as_ptr(self) -> *const T {
        self.0 as *const T
    }

    #[inline(always)]
    pub fn as_mut_ptr(self) -> *mut T {
        self.0
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    #[inline(always)]
    pub unsafe fn offset_from(&self, other: Self) -> isize {
        self.0.offset_from(*other)
    }

    #[inline(always)]
    pub unsafe fn add(&self, count: usize) -> Self {
        Self(self.0.add(count))
    }
}

unsafe impl<T> Send for UnsafePtr<T> {}
unsafe impl<T> Sync for UnsafePtr<T> {}

impl<T> std::ops::Deref for UnsafePtr<T> {
    type Target = *mut T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for UnsafePtr<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Clone for UnsafePtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for UnsafePtr<T> {}
