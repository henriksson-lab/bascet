use crate::common;
use crate::runtime::Error;
use crate::threading::UnsafePtr;

use bytemuck::Pod;
use memmap2::MmapMut;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct PageBuffer<T> {
    inner_ptr_base: *mut T,
    inner_capacity: usize,
    inner_ptr: usize,
    inner_ref_count: AtomicUsize,
}

impl<T: Pod> PageBuffer<T> {
    pub fn from_slice(ptr: *mut T, capacity: usize) -> Self {
        Self {
            inner_ptr_base: ptr,
            inner_capacity: capacity,
            inner_ptr: 0,
            inner_ref_count: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self, count: usize) -> *mut T {
        let start = self.inner_ptr;
        let end = start + count;
        assert!(end <= self.inner_capacity);

        self.inner_ptr = end;
        unsafe { self.inner_ptr_base.add(start) }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.inner_capacity - self.inner_ptr
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.inner_capacity
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
        self.inner_ref_count.load(Ordering::Acquire) == 0
    }

    #[inline(always)]
    pub fn inc_ref(&self) {
        self.inner_ref_count.fetch_add(1, Ordering::AcqRel);
    }

    #[inline(always)]
    pub fn dec_ref(&self) {
        self.inner_ref_count.fetch_sub(1, Ordering::Release);
    }
}

pub struct PageBufferPool<T: Pod, const N: usize> {
    _inner_mmap: MmapMut,
    _inner_capacity: usize,
    inner_pages_n: usize,
    inner_pages: [MaybeUninit<PageBuffer<T>>; N],
    inner_page_active_index: usize,
}

pub struct PageBufferAllocResult<T: Pod> {
    buf_len: usize,
    pub buf_ptr: UnsafePtr<T>,
    pub page_ptr: UnsafePtr<PageBuffer<T>>,
}

impl<T: Pod, const N: usize> PageBufferPool<T, N> {
    pub fn new(num_pages: usize, page_size: usize) -> Result<Self, Error> {
        assert!(num_pages <= N, "num_pages cannot exceed const N");

        let mut mmap = MmapMut::map_anon(num_pages * page_size * std::mem::size_of::<T>())
            .map_err(|e| Error::io_error(e))?;
        let mmap_mut_ptr = mmap.as_mut_ptr() as *mut T;

        unsafe {
            let mut pages: [MaybeUninit<PageBuffer<T>>; N] = MaybeUninit::uninit().assume_init();

            // Give each page a slice of the large buffer for contigous access!
            for i in 0..num_pages {
                let page_start = mmap_mut_ptr.add(i * page_size);

                // SAFETY: MaybeUninit does NOT drop the pagebuffer. Manual drop REQUIRED!
                pages[i] = MaybeUninit::new(PageBuffer::from_slice(page_start, page_size));
            }

            Ok(Self {
                _inner_mmap: mmap,
                _inner_capacity: num_pages * page_size,
                inner_pages: pages,
                inner_page_active_index: 0,
                inner_pages_n: num_pages,
            })
        }
    }

    pub fn active(&self) -> &PageBuffer<T> {
        unsafe { self.inner_pages[self.inner_page_active_index].assume_init_ref() }
    }

    pub fn active_mut(&mut self) -> &mut PageBuffer<T> {
        unsafe { self.inner_pages[self.inner_page_active_index].assume_init_mut() }
    }

    pub fn active_mut_ptr(&mut self) -> *mut PageBuffer<T> {
        unsafe {
            self.inner_pages[self.inner_page_active_index].assume_init_mut() as *mut PageBuffer<T>
        }
    }

    pub fn alloc(&mut self, count: usize) -> PageBufferAllocResult<T> {
        let current_page =
            unsafe { self.inner_pages[self.inner_page_active_index].assume_init_mut() };

        if current_page.remaining() >= count {
            let ptr = current_page.alloc(count);
            return PageBufferAllocResult {
                buf_len: count,
                buf_ptr: UnsafePtr::new(ptr),
                page_ptr: UnsafePtr::new(current_page as *mut PageBuffer<T>),
            };
        }

        // Need new page
        let mut spin_counter = 0;
        loop {
            for i in 0..self.inner_pages_n {
                let idx = (self.inner_page_active_index + i) % self.inner_pages_n;
                let new_page = unsafe { self.inner_pages[idx].assume_init_mut() };
                if new_page.remaining() >= count || new_page.try_reset() {
                    self.inner_page_active_index = idx;

                    let ptr = new_page.alloc(count);
                    return PageBufferAllocResult {
                        buf_len: count,
                        buf_ptr: UnsafePtr::new(ptr),
                        page_ptr: UnsafePtr::new(new_page as *mut PageBuffer<T>),
                    };
                }
            }

            common::spin_or_park(&mut spin_counter, 100);
        }
    }
}

impl<T: Pod, const N: usize> Drop for PageBufferPool<T, N> {
    fn drop(&mut self) {
        // MmapMut will automatically unmap when dropped, no need to manage this here
        // HACK: Wait for all refs to reach zero, otherwise this results in a segfault
        let mut spin_counter = 0;
        while (0..self.inner_pages_n).any(|i| unsafe {
            self.inner_pages[i]
                .assume_init_ref()
                .inner_ref_count
                .load(Ordering::Relaxed)
                != 0
        }) {
            common::spin_or_park(&mut spin_counter, 100);
        }

        for i in 0..self.inner_pages_n {
            unsafe {
                self.inner_pages[i].assume_init_drop();
            }
        }
    }
}
