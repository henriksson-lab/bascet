use crate::common;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{dealloc, Layout};
use std::mem::MaybeUninit;

pub struct PageBuffer<T> {
    pub inner: Box<[T]>,
    inner_ptr: usize,
    capacity: usize,
    pub ref_count: AtomicUsize,
}

impl<T> PageBuffer<T> {
    pub fn with_capacity(cap: usize) -> Self {
        unsafe {
            let layout = Layout::array::<T>(cap).unwrap();
            let ptr = std::alloc::alloc(layout) as *mut T;
            let buffer = Box::from_raw(std::slice::from_raw_parts_mut(ptr, cap));
            Self {
                inner: buffer,
                inner_ptr: 0,
                capacity: cap,
                ref_count: AtomicUsize::new(0),
            }
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self, count: usize) -> *mut T {
        let start = self.inner_ptr;
        let end = start + count;
        assert!(end <= self.capacity);

        self.inner_ptr = end;
        unsafe { self.inner.as_mut_ptr().add(start) }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.capacity - self.inner_ptr
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    pub fn try_reset(&mut self) -> bool {
        if self.available() {
            self.inner_ptr = 0;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn available(&self) -> bool {
        self.ref_count.load(Ordering::Acquire) == 0
    }

    #[inline(always)]
    pub fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::AcqRel);
    }

    #[inline(always)]
    pub fn dec_ref(&self) {
        self.ref_count.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Drop for PageBuffer<T> {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::array::<T>(self.capacity).unwrap();
            let ptr = Box::into_raw(std::mem::take(&mut self.inner));
            dealloc(ptr as *mut u8, layout);
        }
    }
}

pub struct PageBufferPool<T, const N: usize> {
    pub inner_pages: [MaybeUninit<PageBuffer<T>>; N],
    pub inner_index: usize,
    pub num_pages: usize,
}

pub struct PageBufferAllocResult<T> {
    buffer_slice_ptr: *mut T,
    buffer_slice_len: usize,
    buffer_page_ptr: *mut PageBuffer<T>,
}

impl<T> PageBufferAllocResult<T> {
    pub fn buffer_slice_ptr(&self) -> *const T {
        self.buffer_slice_ptr
    }

    pub fn buffer_slice_mut_ptr(&self) -> *mut T {
        self.buffer_slice_ptr
    }

    pub fn buffer_slice_len(&self) -> usize {
        self.buffer_slice_len
    }

    pub fn buffer_page_ptr(&self) -> *const PageBuffer<T> {
        self.buffer_page_ptr
    }

    pub fn buffer_page_mut_ptr(&self) -> *mut PageBuffer<T> {
        self.buffer_page_ptr
    }
}

impl<T, const N: usize> PageBufferPool<T, N> {
    pub fn new(num_pages: usize, page_size: usize) -> Self {
        assert!(num_pages <= N, "num_pages cannot exceed const N");
        
        let mut pages: [MaybeUninit<PageBuffer<T>>; N] = unsafe { MaybeUninit::uninit().assume_init() };
        
        // Initialize only the pages we need
        for i in 0..num_pages {
            pages[i] = MaybeUninit::new(PageBuffer::with_capacity(page_size));
        }
        
        Self {
            inner_pages: pages,
            inner_index: 0,
            num_pages,
        }
    }

    pub fn active(&self) -> &PageBuffer<T> {
        unsafe { self.inner_pages[self.inner_index].assume_init_ref() }
    }

    pub fn active_mut(&mut self) -> &mut PageBuffer<T> {
        unsafe { self.inner_pages[self.inner_index].assume_init_mut() }
    }

    pub fn active_mut_ptr(&mut self) -> *mut PageBuffer<T> {
        unsafe { self.inner_pages[self.inner_index].assume_init_mut() as *mut PageBuffer<T> }
    }

    pub fn alloc(&mut self, count: usize) -> PageBufferAllocResult<T> {
        let current_page = unsafe { self.inner_pages[self.inner_index].assume_init_mut() };

        if current_page.remaining() >= count {
            let ptr = current_page.alloc(count);
            return PageBufferAllocResult {
                buffer_slice_ptr: ptr,
                buffer_slice_len: count,
                buffer_page_ptr: current_page as *mut PageBuffer<T>,
            };
        }

        // Need new page
        let mut spin_counter = 0;
        loop {
            for i in 0..self.num_pages {
                let idx = (self.inner_index + i) % self.num_pages;
                let new_page = unsafe { self.inner_pages[idx].assume_init_mut() };
                if new_page.remaining() >= count || new_page.try_reset() {
                    self.inner_index = idx;

                    let ptr = new_page.alloc(count);
                    return PageBufferAllocResult {
                        buffer_slice_ptr: ptr,
                        buffer_slice_len: count,
                        buffer_page_ptr: new_page as *mut PageBuffer<T>,
                    };
                }
            }

            common::spin_or_park(&mut spin_counter, 100);
        }
    }
}

impl<T, const N: usize> Drop for PageBufferPool<T, N> {
    fn drop(&mut self) {
        // HACK: Spin until all page refs are zero => otherwise stream gets dropped and the slices
        // pointing to the page buffers are invalidated. Not the best solution but the most frictionless.
        let mut spin_counter = 0;
        loop {
            if self
                .inner_pages
                .iter()
                .take(self.num_pages)
                .all(|p| unsafe { p.assume_init_ref().ref_count.load(Ordering::Relaxed) == 0 })
            {
                break;
            }
            spin_counter += 1;
            common::spin_or_park(&mut spin_counter, 100);
        }
    }
}
