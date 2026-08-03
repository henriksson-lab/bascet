use bytesize::ByteSize;
use crossbeam_utils::CachePadded;
use event_listener::{Event, Listener};
use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::Index;
use std::ptr::NonNull;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread::park_timeout;
use std::time::Duration;
use tracing::warn;

use super::consts::*;
use crate::utils::AtomicPatience;

#[derive(Debug, thiserror::Error)]
pub enum Exception {
    #[error("allocation of {requested} bytes exceeds slab capacity of {slab_cap} bytes")]
    Oversized { requested: usize, slab_cap: usize },
}

impl crate::exception::Raise for Exception {
    fn level(&self) -> tracing::Level {
        match self {
            Exception::Oversized { .. } => tracing::Level::ERROR,
        }
    }
}

pub struct ArenaSlice {
    inner: NonNull<[u8]>,
    view: ArenaView,
    _not_sync: PhantomData<*const ()>,
}

impl ArenaSlice {
    #[inline(always)]
    pub unsafe fn from_raw_parts(
        slice: &mut [u8],
        arena: NonNull<Arena>,
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

    // NOTE   unsound paired with `Clone`: a cloned `ArenaSlice` shares the same
    //        `inner: NonNull<[u8]>`, so two clones can each hand out `&mut [u8]` to the
    //        same bytes — aliasing `&mut` from a safe API (UB). Fix (spec §11): split a
    //        non-Clone `ArenaSliceMut` that owns `as_mut_slice` and `freeze()`s into this
    //        read-only Clone view (bytes-crate `BytesMut::freeze` precedent).
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.inner.as_mut() }
    }

    #[inline(always)]
    pub fn src_ptr(&self) -> NonNull<Arena> {
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
    pub(crate) inner_src: NonNull<Arena>,
    event: *const Event,
    waiters: *const AtomicU32,
    _not_sync: PhantomData<*const ()>,
}

impl ArenaView {
    #[inline(always)]
    pub fn new(arena: NonNull<Arena>, event: *const Event, waiters: *const AtomicU32) -> Self {
        unsafe { arena.as_ref().increment_strong_count() };
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
        unsafe { self.inner_src.as_ref().increment_strong_count() };
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
        let prev = unsafe { self.inner_src.as_ref().decrement_strong_count() };
        // SAFETY   waiters/event pointers are valid for the lifetime of the pool,
        //          which outlives all views (enforced by Drop on ArenaPool)
        // NOTE     the above claim has a use-after-free window: decrement_strong_count()
        //          drops `cnt` to 0 BEFORE the `(*self.waiters)`/`(*self.event)` derefs
        //          below, and ArenaPool::drop only waits on `cnt == 0` — so the pool can
        //          observe not-busy, break its loop, and free the Event+waiters while this
        //          view is mid-epilogue. Fix at wire-in: hold the retry Event+waiters in an
        //          `Arc` cloned into each view (last drop keeps them alive), deleting these
        //          raw pointers.
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
    cnt: CachePadded<AtomicU64>,
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
            cnt: CachePadded::new(AtomicU64::new(0)),
        }
    }

    // NOTE   `&mut self` here is unsound as used by `ArenaPool::try_alloc`, which forms
    //        `&mut Arena` from a shared `UnsafeCell<Arena>`: two threads racing the same
    //        slab hold aliasing `&mut Arena` before the `avl` CAS below picks a winner.
    //        The CAS serialises the data write, but forming aliasing `&mut` is itself UB
    //        under Stacked/Tree Borrows (Miri rejects it), independent of the CAS. Latent
    //        only because the arena is not yet wired to a hot path. Fix at wire-in: take
    //        `&self`, hold `off` in interior mutability mutated only by the CAS winner, and
    //        return `*mut u8` — never construct `&mut Arena`.
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
        let start = self.inner.off;
        self.inner.off += len;
        unsafe {
            debug_assert!(self.inner.off <= self.inner.len);
            std::hint::assert_unchecked(self.inner.off <= self.inner.len);
            Some(self.inner.ptr.as_ptr().add(start))
        }
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.inner.len - self.inner.off
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.inner.len
    }

    #[inline(always)]
    pub fn increment_strong_count(&self) {
        // SAFETY: just incrementing no data sync needed here as the value of this is not needed anywhere
        let cnt = self.cnt.fetch_add(1, Ordering::Relaxed);
        debug_assert!(cnt < u64::MAX);
    }

    #[inline(always)]
    pub fn decrement_strong_count(&self) -> u64 {
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
    inner_patience: CachePadded<AtomicPatience>,

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
                let step = page_size::get();
                let mut offset = 0;
                while offset < total {
                    base.add(offset).write_volatile(0);
                    offset += step;
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
                inner_patience: CachePadded::new(AtomicPatience::new()),
                inner_retry_alloc: Box::new(Event::new()),
                inner_retry_waiters: Box::new(AtomicU32::new(0)),
            }
        }
    }

    pub fn try_alloc(&self, len: usize) -> Option<ArenaSlice> {
        if len > self.inner_cap_arenas {
            return None;
        }
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
                        NonNull::new_unchecked(arena),
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
            // NOTE     exclusive to the *data* only; `&mut *cell.get()` still forms an
            //          aliasing `&mut Arena` across racing threads (see Arena::try_alloc).
            let arena = unsafe { &mut *self.inner_buf_arenas.get_unchecked(idx).get() };
            if let Some(ptr) = arena.try_alloc(len) {
                self.inner_idx_hint.store(idx, Ordering::Relaxed);
                // SAFETY   ArenaPool outlives all ArenaSlices due to drop impl
                //          ArenaSlice::new increments strong count
                let slice = unsafe {
                    ArenaSlice::from_raw_parts(
                        std::slice::from_raw_parts_mut(ptr, len),
                        NonNull::new_unchecked(arena),
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

    pub async fn alloc_await(&self, len: usize) -> Result<ArenaSlice, Exception> {
        if len > self.inner_cap_arenas {
            return Err(Exception::Oversized {
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

    pub fn alloc_blocking(&self, len: usize) -> Result<ArenaSlice, Exception> {
        if len > self.inner_cap_arenas {
            return Err(Exception::Oversized {
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
        let mut millis = 0;
        loop {
            // SAFETY: we're in drop, no other threads can access arenas
            let busy = self
                .inner_buf_arenas
                .iter()
                .any(|cell| unsafe { (*cell.get()).cnt.load(Ordering::Relaxed) != 0 });

            if !busy {
                break;
            }

            park_timeout(Duration::from_millis(100));
            millis += 100;

            if millis % 15_000 == 0 {
                warn!(source = "ArenaPool::drop", "waiting for arena to be freed");
            }
        }

        std::sync::atomic::fence(Ordering::Acquire);
    }
}
