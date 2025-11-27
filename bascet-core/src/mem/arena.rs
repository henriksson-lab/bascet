use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Index;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};

use bytemuck::Pod;
use bytesize::ByteSize;
use memmap2::MmapMut;

use crate::utils::spinpark_loop::{self, spinpark_loop, SpinPark};
use crate::{likely_unlikely, SendPtr, DEFAULT_MIN_SIZEOF_ARENA, DEFAULT_MIN_SIZEOF_BUFFER};

pub struct ArenaSlice<T>
where
    T: Pod,
{
    slice: *mut [T],
    view: ArenaView<T>,
    _not_sync: PhantomData<*const ()>,
}

impl<T> ArenaSlice<T>
where
    T: Pod,
{
    #[inline(always)]
    pub unsafe fn new(slice: &mut [T], arena: SendPtr<Arena<T>>) -> Self {
        Self {
            slice: slice as *mut [T],
            view: ArenaView::new(arena),
            _not_sync: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn truncate(mut self, len: usize) -> Self {
        let ptr = self.slice as *mut T;
        self.slice = std::slice::from_raw_parts_mut(ptr, len);
        self
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { &*self.slice }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { &mut *self.slice }
    }

    #[inline(always)]
    pub fn src_ptr(&self) -> SendPtr<Arena<T>> {
        self.view.inner_src
    }

    #[inline(always)]
    pub fn clone_view(&self) -> ArenaView<T> {
        self.view.clone()
    }
}

unsafe impl<T> Send for ArenaSlice<T> where T: Pod + Send + Sync {}

impl<T> Clone for ArenaSlice<T>
where
    T: Pod,
{
    fn clone(&self) -> Self {
        Self {
            slice: self.slice,
            view: self.view.clone(),
            _not_sync: PhantomData,
        }
    }
}

impl<T, I> Index<I> for ArenaSlice<T>
where
    T: Pod,
    I: SliceIndex<[T]>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.as_slice()[index]
    }
}

pub struct ArenaView<T>
where
    T: Pod,
{
    pub(crate) inner_src: SendPtr<Arena<T>>,
    _not_sync: PhantomData<*const ()>,
}

impl<T> ArenaView<T>
where
    T: Pod,
{
    #[inline(always)]
    pub fn new(arena: SendPtr<Arena<T>>) -> Self {
        unsafe { (*arena).as_ref().increment_strong_count() };
        Self {
            inner_src: arena,
            _not_sync: PhantomData,
        }
    }
}

unsafe impl<T> Send for ArenaView<T> where T: Pod + Send + Sync {}

impl<T> Clone for ArenaView<T>
where
    T: Pod,
{
    fn clone(&self) -> Self {
        unsafe { (*self.inner_src).as_ref().increment_strong_count() };
        Self {
            inner_src: self.inner_src,
            _not_sync: PhantomData,
        }
    }
}

impl<T> Drop for ArenaView<T>
where
    T: Pod,
{
    fn drop(&mut self) {
        unsafe { (*self.inner_src).as_ref().decrement_strong_count() };
    }
}

#[repr(C, align(64))]
pub struct Arena<T> {
    // allocator hot path (cache line 1)
    ptr: *mut T,
    len: u64,
    off: u64,
    avl: AtomicBool,
    _pad: MaybeUninit<[u8; 39]>,

    // consumer hot path (cache line 2)
    cnt: AtomicU16,
}

impl<T: Pod> Arena<T> {
    pub fn from_slice(ptr: *mut T, cap: usize) -> Self {
        Self {
            ptr,
            len: cap as u64,
            off: 0,
            avl: AtomicBool::new(true),
            _pad: MaybeUninit::uninit(),
            cnt: AtomicU16::new(0),
        }
    }

    #[inline(always)]
    pub fn available(&mut self, len: usize) -> bool {
        if self
            .avl
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        if self.remaining() >= len || self.try_reset() {
            return true;
        }
        self.avl.store(true, Ordering::Release);
        false
    }

    #[inline(always)]
    pub fn release(&self) {
        self.avl.store(true, Ordering::Release);
    }

    #[inline(always)]
    pub fn alloc(&mut self, sizeof_alloc: usize) -> *mut T {
        // DEBUG: Verify lock is held
        debug_assert!(
            !self.avl.load(Ordering::Relaxed),
            "Arena::alloc called without holding lock!"
        );

        let sizeof_alloc = sizeof_alloc as u64;
        let start = self.off;
        let end = start
            .checked_add(sizeof_alloc)
            .expect("Arena::alloc: overflow");

        assert!(end <= self.len, "Arena::alloc: insufficient capacity");

        self.off = end;
        unsafe { self.ptr.add(start as usize) }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        (self.len - self.off) as usize
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub fn try_reset(&mut self) -> bool {
        // SAFETY: Acquire on success synchronizes with Release in dec_ref,
        // ensuring we see all writes to arena data before reusing it
        match self
            .cnt
            .compare_exchange(0, 0, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => {
                self.off = 0;
                true
            }
            Err(_) => false,
        }
    }

    #[inline(always)]
    pub fn increment_strong_count(&self) {
        // SAFETY: just incrementing no data sync needed here as the value of this is not needed anywhere
        let cnt = self.cnt.fetch_add(1, Ordering::Relaxed);
        debug_assert!(cnt !=  u16::MAX, "Arena refcount underflow");
    }

    #[inline(always)]
    pub fn decrement_strong_count(&self) {
        // SAFETY: Release ensures all writes to arena data happen before refcnt reaches 0
        let cnt = self.cnt.fetch_sub(1, Ordering::Release);
        debug_assert!(cnt != 0, "Arena refcount underflow");
    }
}

pub struct ArenaPool<T: Pod> {
    _mmap: MmapMut,
    inner_buf_arenas: Vec<UnsafeCell<Arena<T>>>,
    inner_len_arenas: usize,
    inner_idx_hint: AtomicUsize,
}

unsafe impl<T: Pod + Send> Send for ArenaPool<T> {}
unsafe impl<T: Pod + Sync> Sync for ArenaPool<T> {}

impl<T: Pod> ArenaPool<T> {
    pub fn new(sizeof_buffer: ByteSize, sizeof_arena: ByteSize) -> Result<Self, ()> {
        let num_arenas = (sizeof_buffer.as_u64() / sizeof_arena.as_u64()) as usize;

        //TODO: return errors
        assert!(
            num_arenas >= 2,
            "need at least 2 arenas to prevent stalls (higher strongly recommended)"
        );
        assert!(
            sizeof_buffer >= DEFAULT_MIN_SIZEOF_BUFFER,
            "buffer size should be at least {:?} (higher strongly recommended)",
            DEFAULT_MIN_SIZEOF_BUFFER
        );
        assert!(
            sizeof_arena >= DEFAULT_MIN_SIZEOF_ARENA,
            "arena size should be at least {:?} (higher strongly recommended)",
            DEFAULT_MIN_SIZEOF_ARENA
        );

        unsafe {
            let mut arenas = Vec::with_capacity(num_arenas);
            let len_arenas = sizeof_arena.as_u64() as usize / size_of::<T>();
            // TODO: construct with MmapOptions for explicit use of Huge Pages?
            let mut mmap = MmapMut::map_anon(sizeof_buffer.as_u64() as usize).unwrap();
            let ptr_arenas_base = mmap.as_mut_ptr() as *mut T;
            for i in 0..num_arenas {
                let ptr_arena_start = ptr_arenas_base.add(i * len_arenas);
                arenas.push(UnsafeCell::new(Arena::from_slice(
                    ptr_arena_start,
                    len_arenas,
                )));
            }

            Ok(Self {
                _mmap: mmap,
                inner_len_arenas: len_arenas,
                inner_buf_arenas: arenas,
                inner_idx_hint: AtomicUsize::new(0),
            })
        }
    }

    pub fn alloc(&self, len: usize) -> ArenaSlice<T> {
        assert!(
            len <= self.inner_len_arenas,
            "alloc size exceeds arena size"
        );

        let num_arenas = self.inner_buf_arenas.len();
        let mut cnt_spin = 0;
        let mut cnt_park = 0;

        loop {
            for i in 0..num_arenas {
                let idx = (self.inner_idx_hint.load(Ordering::Relaxed) + i) % num_arenas;
                // SAFETY   The atomic lock in available() ensures exclusive access
                let arena = unsafe { &mut *self.inner_buf_arenas[idx].get() };
                if arena.available(len) {
                    self.inner_idx_hint.store(idx, Ordering::Relaxed);
                    let ptr = arena.alloc(len);

                    // SAFETY   ArenaPool outlives all ArenaSlices due to drop impl
                    //          ArenaSlice::new increments strong count
                    let slice = unsafe {
                        ArenaSlice::new(
                            std::slice::from_raw_parts_mut(ptr, len),
                            SendPtr::new_unchecked(arena as *mut Arena<T>),
                        )
                    };
                    arena.release();

                    return slice;
                }
            }
            if likely_unlikely::unlikely(spinpark_loop::<100>(&mut cnt_spin) == SpinPark::Park) {
                cnt_park += 1;
                if likely_unlikely::unlikely(cnt_park >= spinpark_loop::PARKS_BEFORE_WARN) {
                    // TODO: emit warning - consumers not releasing fast enough
                    cnt_park = 0;
                }
            }
        }
    }
}

impl<T: Pod> Drop for ArenaPool<T> {
    fn drop(&mut self) {
        let mut cnt_spin = 0;
        let mut cnt_park = 0;

        'wait: loop {
            for arena_cell in &self.inner_buf_arenas {
                // SAFETY: We're in drop, no other threads can access arenas
                let arena = unsafe { &*arena_cell.get() };
                if arena.cnt.load(Ordering::Relaxed) != 0 {
                    if likely_unlikely::unlikely(
                        spinpark_loop::<100>(&mut cnt_spin) == SpinPark::Park,
                    ) {
                        cnt_park += 1;
                        if likely_unlikely::unlikely(cnt_park >= spinpark_loop::PARKS_BEFORE_WARN) {
                            // TODO: emit warning - consumers not releasing fast enough
                            cnt_park = 0;
                        }
                    }
                    continue 'wait;
                }
            }
            break;
        }

        std::sync::atomic::fence(Ordering::Acquire);
    }
}
