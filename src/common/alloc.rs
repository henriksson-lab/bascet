use crate::common::spin_or_park;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct PageBuffer {
    pub inner: Vec<u8>,
    inner_ptr: usize,
    pub ref_count: AtomicUsize,
}

impl PageBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        let mut data = Vec::with_capacity(cap);
        unsafe {
            data.set_len(cap);
        }
        Self {
            inner: data,
            inner_ptr: 0,
            ref_count: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn alloc_unchecked(&mut self, size: usize) -> *const u8 {
        let start = self.inner_ptr;
        let end = start + size;
        self.inner_ptr = end;

        unsafe { self.inner.as_mut_ptr().add(start) }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.inner.capacity() - self.inner_ptr
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline(always)]
    pub fn available(&self) -> bool {
        self.ref_count.load(Ordering::Acquire) == 0
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
    pub fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn dec_ref(&self) {
        self.ref_count.fetch_sub(1, Ordering::Release);
    }
}

pub struct PageBufferPool {
    pub inner_pages: Vec<PageBuffer>,
    pub inner_index: usize,
}

pub struct PageBufferAllocResult {
    buffer_slice_ptr: *const u8,
    buffer_slice_len: usize,
    buffer_page_ptr: *mut PageBuffer,
}

impl PageBufferAllocResult {
    pub fn buffer_slice_ptr(&self) -> *const u8 {
        self.buffer_slice_ptr
    }

    pub fn buffer_slice_mut_ptr(&self) -> *mut u8 {
        self.buffer_slice_ptr as *mut u8
    }

    pub fn buffer_slice_len(&self) -> usize {
        self.buffer_slice_len
    }

    pub fn buffer_page_ptr(&self) -> *const PageBuffer {
        self.buffer_page_ptr
    }

    pub fn buffer_page_mut_ptr(&self) -> *mut PageBuffer {
        self.buffer_page_ptr
    }
}

impl PageBufferPool {
    pub fn new(num_pages: usize, page_size: usize) -> Self {
        let mut pages = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            pages.push(PageBuffer::with_capacity(page_size));
        }

        Self {
            inner_pages: pages,
            inner_index: 0,
        }
    }

    pub fn active(&self) -> &PageBuffer {
        return &self.inner_pages[self.inner_index];
    }

    pub fn active_mut(&mut self) -> &mut PageBuffer {
        return &mut self.inner_pages[self.inner_index];
    }

    pub fn active_mut_ptr(&mut self) -> *mut PageBuffer {
        return &mut self.inner_pages[self.inner_index] as *mut PageBuffer;
    }

    pub fn alloc(&mut self, bytes: usize) -> PageBufferAllocResult {
        let current_page = &mut self.inner_pages[self.inner_index];

        if current_page.remaining() >= bytes {
            let ptr = current_page.alloc_unchecked(bytes);
            return PageBufferAllocResult {
                buffer_slice_ptr: ptr,
                buffer_slice_len: bytes,
                buffer_page_ptr: current_page as *mut PageBuffer,
            };
        }

        // Need new page
        let mut spin_counter = 0;
        loop {
            for i in 0..self.inner_pages.len() {
                let idx = (self.inner_index + i) % self.inner_pages.len();
                let new_page = &mut self.inner_pages[idx];
                if new_page.remaining() >= bytes || new_page.try_reset() {
                    self.inner_index = idx;

                    let ptr = new_page.alloc_unchecked(bytes);
                    return PageBufferAllocResult {
                        buffer_slice_ptr: ptr,
                        buffer_slice_len: bytes,
                        buffer_page_ptr: new_page as *mut PageBuffer,
                    };
                }
            }

            spin_or_park(&mut spin_counter, 100);
        }
    }
}
