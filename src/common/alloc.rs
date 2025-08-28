use crate::{common::spin_or_park};
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
    pub fn alloc_unchecked(&mut self, size: usize) -> *mut u8 {
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

    #[inline(always)]
    pub fn bounds(&self) -> (*const u8, *const u8) {
        let start = self.inner.as_ptr();
        let end = unsafe { start.add(self.inner.capacity()) };
        (start, end)
    }
    #[inline(always)]
    pub unsafe fn incr_ptr_unchecked(&mut self, bytes: usize) {
        self.inner_ptr += bytes;
    }
}

pub struct PageBufferPool {
    pub inner_pages: Vec<PageBuffer>,
    pub inner_index: usize,
}

pub enum PageBufferAllocResult {
    Continue {
        ptr: *mut u8,
        len: usize,
        buffer_page_ptr: *mut PageBuffer,
        buffer_start: *const u8,
        buffer_end: *const u8,
    },
    NewPage {
        ptr: *mut u8,
        len: usize,
        buffer_page_ptr: *mut PageBuffer,
        buffer_start: *const u8,
        buffer_end: *const u8,
    },
}

impl PageBufferAllocResult {
    pub unsafe fn as_slice_mut(&self) -> &mut [u8] {
        match self {
            PageBufferAllocResult::Continue { ptr, len, .. }
            | PageBufferAllocResult::NewPage { ptr, len, .. } => {
                std::slice::from_raw_parts_mut(*ptr, *len)
            }
        }
    }

    pub fn ptr_mut(&self) -> *mut u8 {
        match self {
            PageBufferAllocResult::Continue { ptr, .. }
            | PageBufferAllocResult::NewPage { ptr, .. } => *ptr,
        }
    }

    pub fn buffer_page_ptr(&self) -> *mut PageBuffer {
        match self {
            PageBufferAllocResult::Continue {
                buffer_page_ptr, ..
            }
            | PageBufferAllocResult::NewPage {
                buffer_page_ptr, ..
            } => *buffer_page_ptr,
        }
    }

    pub fn buffer_bounds(&self) -> (*const u8, *const u8) {
        match self {
            PageBufferAllocResult::Continue {
                buffer_start,
                buffer_end,
                ..
            }
            | PageBufferAllocResult::NewPage {
                buffer_start,
                buffer_end,
                ..
            } => (*buffer_start, *buffer_end),
        }
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

    pub fn alloc(&mut self, bytes: usize) -> PageBufferAllocResult {
        let current_page = &mut self.inner_pages[self.inner_index];

        if current_page.remaining() >= bytes {
            let ptr = current_page.alloc_unchecked(bytes);
            let buffer_start = current_page.inner.as_ptr();
            let buffer_end = unsafe { buffer_start.add(current_page.inner.capacity()) };

            return PageBufferAllocResult::Continue {
                ptr,
                len: bytes,
                buffer_page_ptr: current_page as *mut PageBuffer,
                buffer_start,
                buffer_end,
            };
        }

        // Need new page
        let mut spin_counter = 0;
        loop {
            for i in 0..self.inner_pages.len() {
                let idx = (self.inner_index + i) % self.inner_pages.len();
                let new_page = &mut self.inner_pages[idx];

                // println!("guards: {}", Arc::strong_count(&new_page.guard));

                if new_page.remaining() >= bytes || new_page.try_reset() {
                    let ptr = new_page.alloc_unchecked(bytes);
                    let buffer_start = new_page.inner.as_ptr();
                    let buffer_end = unsafe { buffer_start.add(new_page.inner.capacity()) };

                    self.inner_index = idx;
                    return PageBufferAllocResult::NewPage {
                        ptr,
                        len: bytes,
                        buffer_page_ptr: new_page as *mut PageBuffer,
                        buffer_start,
                        buffer_end,
                    };
                }
            }

            spin_or_park(&mut spin_counter, 100);
        }
    }
}
