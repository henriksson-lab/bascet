use bytesize::ByteSize;
use crossbeam::utils::CachePadded;
use event_listener::{Event, Listener};
use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::Index;
use std::ptr::NonNull;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicUsize, Ordering};
use tracing::warn;

use super::consts::*;
use crate::utils::AtomicPatience;
use crate::utils::send::SendPtr;
use crate::utils::threading::spinpark_loop::{self, SPINPARK_COUNTOF_PARKS_BEFORE_WARN, SpinPark};

#[derive(Debug)]
pub enum AllocError {
    Oversized { requested: usize, slab_cap: usize },
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocError::Oversized {
                requested,
                slab_cap,
            } => write!(
                f,
                "allocation of {requested} bytes exceeds slab capacity of {slab_cap} bytes"
            ),
        }
    }
}

impl std::error::Error for AllocError {}

pub struct ArenaSlice {
    inner: NonNull<[u8]>,
    view: ArenaView,
    _not_sync: PhantomData<*const ()>,
}

impl ArenaSlice {
    #[inline(always)]
    pub unsafe fn from_raw_parts(
        slice: &mut [u8],
        arena: SendPtr<Arena>,
        event: *const Event,
        waiters: *const AtomicU32,
    ) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(slice as *mut [u8]) },
            view: ArenaView::new(arena, event, waiters),
            _not_sync: PhantomData,
        }
    }

    #[inline(always)]
    pub unsafe fn truncate(mut self, len: usize) -> Self {
        unsafe {
            debug_assert!(len <= self.inner.as_ref().len());
            let ptr = self.inner.as_ptr() as *mut u8;
            self.inner =
                NonNull::new_unchecked(std::slice::from_raw_parts_mut(ptr, len) as *mut [u8]);
        }
        self
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { self.inner.as_ref() }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.inner.as_mut() }
    }

    #[inline(always)]
    pub fn src_ptr(&self) -> SendPtr<Arena> {
        self.view.inner_src
    }

    #[inline(always)]
    pub fn clone_view(&self) -> ArenaView {
        self.view.clone()
    }
}

unsafe impl Send for ArenaSlice {}

impl Clone for ArenaSlice {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            view: self.view.clone(),
            _not_sync: PhantomData,
        }
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for ArenaSlice {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.as_slice()[index]
    }
}

pub struct ArenaView {
    pub(crate) inner_src: SendPtr<Arena>,
    event: *const Event,
    waiters: *const AtomicU32,
    _not_sync: PhantomData<*const ()>,
}

impl ArenaView {
    #[inline(always)]
    pub fn new(arena: SendPtr<Arena>, event: *const Event, waiters: *const AtomicU32) -> Self {
        unsafe { (*arena).as_ref().increment_strong_count() };
        Self {
            inner_src: arena,
            event,
            waiters,
            _not_sync: PhantomData,
        }
    }
}

unsafe impl Send for ArenaView {}

impl Clone for ArenaView {
    fn clone(&self) -> Self {
        unsafe { (*self.inner_src).as_ref().increment_strong_count() };
        Self {
            inner_src: self.inner_src,
            event: self.event,
            waiters: self.waiters,
            _not_sync: PhantomData,
        }
    }
}

impl Drop for ArenaView {
    fn drop(&mut self) {
        let prev = unsafe { (*self.inner_src).as_ref().decrement_strong_count() };
        // SAFETY   waiters/event pointers are valid for the lifetime of the pool,
        //          which outlives all views (enforced by Drop on ArenaPool)
        if prev == 1 {
            unsafe {
                if (*self.waiters).load(Ordering::Relaxed) > 0 {
                    (*self.event).notify(1);
                }
            }
        }
    }
}

struct ArenaInner {
    ptr: NonNull<u8>,
    len: usize,
    off: usize,
    avl: AtomicBool,
}

#[repr(C)]
pub struct Arena {
    // allocator hot path (cache line 1)
    inner: CachePadded<ArenaInner>,
    // consumer hot path (cache line 2)
    cnt: CachePadded<AtomicU16>,
}

impl Arena {
    pub unsafe fn from_slice(ptr: *mut u8, cap: usize) -> Self {
        Self {
            inner: CachePadded::new(ArenaInner {
                ptr: unsafe { NonNull::new_unchecked(ptr) },
                len: cap,
                off: 0,
                avl: AtomicBool::new(true),
            }),
            cnt: CachePadded::new(AtomicU16::new(0)),
        }
    }

    #[inline(always)]
    pub fn try_alloc(&mut self, len: usize) -> Option<*mut u8> {
        if self.inner.avl.load(Ordering::Relaxed) == false {
            return None;
        }
        if self
            .inner
            .avl
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        if self.remaining() < len {
            if self.cnt.load(Ordering::Acquire) != 0 {
                self.inner.avl.store(true, Ordering::Release);
                return None;
            }
            self.inner.off = 0;
        }
        let start = self.inner.off as usize;
        self.inner.off += len;
        unsafe {
            debug_assert!(self.inner.off <= self.inner.len);
            std::hint::assert_unchecked(self.inner.off <= self.inner.len);
            Some(self.inner.ptr.as_ptr().add(start))
        }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        (self.inner.len - self.inner.off) as usize
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.inner.len as usize
    }

    #[inline(always)]
    pub fn increment_strong_count(&self) {
        // SAFETY: just incrementing no data sync needed here as the value of this is not needed anywhere
        let cnt = self.cnt.fetch_add(1, Ordering::Relaxed);
        debug_assert!(cnt < u16::MAX);
    }

    #[inline(always)]
    pub fn decrement_strong_count(&self) -> u16 {
        // SAFETY: Release ensures all writes to arena data happen before refcnt reaches 0
        let cnt = self.cnt.fetch_sub(1, Ordering::Release);
        debug_assert!(cnt > 0);
        cnt
    }
}

#[repr(C)]
pub struct ArenaPool {
    _mmap: MmapMut,
    inner_buf_arenas: Box<[UnsafeCell<Arena>]>,
    inner_cap_arenas: usize,

    inner_idx_hint: CachePadded<AtomicUsize>,
    inner_patience: CachePadded<AtomicPatience<AtomicU32>>,

    inner_retry_alloc: Box<Event>,
    inner_retry_waiters: Box<AtomicU32>,

    sizeof_buffer: ByteSize,
    sizeof_arena: ByteSize,
}

unsafe impl Send for ArenaPool {}
unsafe impl Sync for ArenaPool {}

impl ArenaPool {
    pub fn new(sizeof_buffer: ByteSize, sizeof_arena: ByteSize) -> Self {
        let countof_arenas = (sizeof_buffer.as_u64() / sizeof_arena.as_u64()) as usize;
        let capof_arenas = sizeof_arena.as_u64() as usize;

        //TODO: return errors
        assert!(
            countof_arenas >= 2,
            "need at least 2 arenas to prevent stalls (higher strongly recommended)"
        );

        unsafe {
            let mut vec_arenas = Vec::with_capacity(countof_arenas);
            let mut mmap = MmapOptions::new()
                .len(sizeof_buffer.as_u64() as usize)
                .huge(None)
                .map_anon()
                .unwrap_or_else(|_| {
                    MmapOptions::new()
                        .len(sizeof_buffer.as_u64() as usize)
                        .map_anon()
                        .unwrap()
                });

            // Fault in all pages up front — unconditionally eliminates page faults on the hot path
            {
                let base = mmap.as_mut_ptr();
                let total = sizeof_buffer.as_u64() as usize;
                let mut offset = 0;
                while offset < total {
                    base.add(offset).write_volatile(0);
                    offset += 4096;
                }
            }

            let ptrbase = mmap.as_mut_ptr();
            for i in 0..countof_arenas {
                let ptr = ptrbase.add(i * capof_arenas);
                vec_arenas.push(UnsafeCell::new(Arena::from_slice(ptr, capof_arenas)));
            }

            Self {
                _mmap: mmap,
                inner_cap_arenas: capof_arenas,
                inner_buf_arenas: vec_arenas.into_boxed_slice(),
                sizeof_buffer,
                sizeof_arena,
                inner_idx_hint: CachePadded::new(AtomicUsize::new(0)),
                inner_patience: CachePadded::new(
                    AtomicPatience::new(
                        AtomicU32::new(PATIENCE_INIT),
                        PATIENCE_GROWTH,
                        PATIENCE_DECAY,
                    )
                    .set_min(PATIENCE_MIN)
                    .set_max(PATIENCE_MAX),
                ),
                inner_retry_alloc: Box::new(Event::new()),
                inner_retry_waiters: Box::new(AtomicU32::new(0)),
            }
        }
    }

    pub fn try_alloc(&self, len: usize) -> Option<ArenaSlice> {
        let countof = self.inner_buf_arenas.len();
        unsafe {
            std::hint::assert_unchecked(countof > 0);
        }

        let hint = self.inner_idx_hint.load(Ordering::Relaxed);
        let patience = self.inner_patience.patience();
        for _ in 0..patience {
            let arena = unsafe { &mut *self.inner_buf_arenas.get_unchecked(hint).get() };
            if let Some(ptr) = arena.try_alloc(len) {
                // SAFETY   ArenaPool outlives all ArenaSlices due to drop impl
                //          ArenaSlice::new increments strong count
                let slice = unsafe {
                    ArenaSlice::from_raw_parts(
                        std::slice::from_raw_parts_mut(ptr, len),
                        SendPtr::new_unchecked(arena),
                        &*self.inner_retry_alloc as *const Event,
                        &*self.inner_retry_waiters as *const AtomicU32,
                    )
                };
                arena.inner.avl.store(true, Ordering::Release);
                return Some(slice);
            }
            std::hint::spin_loop();
        }

        let hint = self.inner_idx_hint.load(Ordering::Relaxed);
        for i in 0..countof {
            let idx = (hint + i) % countof;
            unsafe {
                debug_assert!(idx < countof);
                std::hint::assert_unchecked(idx < countof);
            }

            // SAFETY   The atomic lock in try_alloc() ensures exclusive access
            let arena = unsafe { &mut *self.inner_buf_arenas.get_unchecked(idx).get() };
            if let Some(ptr) = arena.try_alloc(len) {
                self.inner_idx_hint.store(idx, Ordering::Relaxed);
                // SAFETY   ArenaPool outlives all ArenaSlices due to drop impl
                //          ArenaSlice::new increments strong count
                let slice = unsafe {
                    ArenaSlice::from_raw_parts(
                        std::slice::from_raw_parts_mut(ptr, len),
                        SendPtr::new_unchecked(arena),
                        &*self.inner_retry_alloc as *const Event,
                        &*self.inner_retry_waiters as *const AtomicU32,
                    )
                };
                arena.inner.avl.store(true, Ordering::Release);

                // Cold path    hint slab was briefly busy: good locality, grow patience.
                //              Found elsewhere: bad locality, decay patience.
                if idx == hint {
                    self.inner_patience.hit();
                } else {
                    self.inner_patience.miss();
                }
                return Some(slice);
            }

            let next1 = (idx + 1) % countof;
            unsafe {
                branches::prefetch_read_data::<Arena, 0>(
                    self.inner_buf_arenas.get_unchecked(next1).get() as *const Arena,
                )
            };
            let next2 = (idx + 2) % countof;
            unsafe {
                branches::prefetch_read_data::<Arena, 0>(
                    self.inner_buf_arenas.get_unchecked(next2).get() as *const Arena,
                )
            };
        }

        None
    }

    pub fn waiters(&self) -> u32 {
        self.inner_retry_waiters.load(Ordering::Relaxed)
    }

    pub async fn alloc_await(&self, len: usize) -> Result<ArenaSlice, AllocError> {
        if len > self.inner_cap_arenas {
            return Err(AllocError::Oversized {
                requested: len,
                slab_cap: self.inner_cap_arenas,
            });
        }

        loop {
            if let Some(slice) = self.try_alloc(len) {
                return Ok(slice);
            }

            self.inner_retry_waiters.fetch_add(1, Ordering::Relaxed);
            let listener = self.inner_retry_alloc.listen();
            if let Some(slice) = self.try_alloc(len) {
                self.inner_retry_waiters.fetch_sub(1, Ordering::Relaxed);
                return Ok(slice);
            }
            listener.await;
            self.inner_retry_waiters.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn alloc_blocking(&self, len: usize) -> Result<ArenaSlice, AllocError> {
        if len > self.inner_cap_arenas {
            return Err(AllocError::Oversized {
                requested: len,
                slab_cap: self.inner_cap_arenas,
            });
        }

        loop {
            if let Some(slice) = self.try_alloc(len) {
                return Ok(slice);
            }

            self.inner_retry_waiters.fetch_add(1, Ordering::Relaxed);
            let listener = self.inner_retry_alloc.listen();
            if let Some(slice) = self.try_alloc(len) {
                self.inner_retry_waiters.fetch_sub(1, Ordering::Relaxed);
                return Ok(slice);
            }
            listener.wait();
            self.inner_retry_waiters.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for ArenaPool {
    fn drop(&mut self) {
        let mut count_spun = 0;

        'wait: loop {
            for arena_cell in &self.inner_buf_arenas {
                // SAFETY: We're in drop, no other threads can access arenas
                let arena = unsafe { &*arena_cell.get() };
                if arena.cnt.load(Ordering::Relaxed) != 0 {
                    match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(
                        &mut count_spun,
                    ) {
                        SpinPark::Warn => {
                            warn!(source = "ArenaPool::drop", "waiting for arena to be freed")
                        }
                        _ => {}
                    }
                    continue 'wait;
                }
            }
            break;
        }

        std::sync::atomic::fence(Ordering::Acquire);
    }
}
