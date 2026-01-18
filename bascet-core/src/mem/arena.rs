use bytesize::ByteSize;
use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Index;
use std::ptr::NonNull;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};

use crate::spinpark_loop::SPINPARK_PARKS_BEFORE_WARN;
use crate::utils::spinpark_loop;
use crate::{likely_unlikely, SendPtr, DEFAULT_MIN_SIZEOF_ARENA, DEFAULT_MIN_SIZEOF_BUFFER};

pub struct ArenaSlice<T>
where
    T: bytemuck::Pod,
{
    inner: NonNull<[T]>,
    view: ArenaView<T>,
    _not_sync: PhantomData<*const ()>,
}

impl<T> ArenaSlice<T>
where
    T: bytemuck::Pod,
{
    #[inline(always)]
    pub unsafe fn from_raw_parts(slice: &mut [T], arena: SendPtr<Arena<T>>) -> Self {
        Self {
            inner: NonNull::new_unchecked(slice as *mut [T]),
            view: ArenaView::new(arena),
            _not_sync: PhantomData,
        }
    }

    #[inline(always)]
    pub unsafe fn truncate(mut self, len: usize) -> Self {
        debug_assert!(len <= self.inner.as_ref().len());
        let ptr = self.inner.as_ptr() as *mut T;
        self.inner = NonNull::new_unchecked(std::slice::from_raw_parts_mut(ptr, len) as *mut [T]);
        self
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { self.inner.as_ref() }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { self.inner.as_mut() }
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

unsafe impl<T> Send for ArenaSlice<T> where T: bytemuck::Pod + Send + Sync {}

impl<T> Clone for ArenaSlice<T>
where
    T: bytemuck::Pod,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            view: self.view.clone(),
            _not_sync: PhantomData,
        }
    }
}

impl<T, I> Index<I> for ArenaSlice<T>
where
    T: bytemuck::Pod,
    I: SliceIndex<[T]>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.as_slice()[index]
    }
}

pub struct ArenaView<T>
where
    T: bytemuck::Pod,
{
    pub(crate) inner_src: SendPtr<Arena<T>>,
    _not_sync: PhantomData<*const ()>,
}

impl<T> ArenaView<T>
where
    T: bytemuck::Pod,
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

unsafe impl<T> Send for ArenaView<T> where T: bytemuck::Pod + Send + Sync {}

impl<T> Clone for ArenaView<T>
where
    T: bytemuck::Pod,
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
    T: bytemuck::Pod,
{
    fn drop(&mut self) {
        unsafe { (*self.inner_src).as_ref().decrement_strong_count() };
    }
}

#[repr(C, align(64))]
pub struct Arena<T> {
    // allocator hot path (cache line 1)
    ptr: NonNull<T>,
    len: u64,
    off: u64,
    avl: AtomicBool,
    _pad: MaybeUninit<[u8; 39]>,

    // consumer hot path (cache line 2)
    cnt: AtomicU16,
}

impl<T: bytemuck::Pod> Arena<T> {
    pub unsafe fn from_slice(ptr: *mut T, cap: usize) -> Self {
        Self {
            ptr: NonNull::new_unchecked(ptr),
            len: cap as u64,
            off: 0,
            avl: AtomicBool::new(true),
            _pad: MaybeUninit::uninit(),
            cnt: AtomicU16::new(0),
        }
    }

    #[inline(always)]
    pub fn is_available(&mut self, len: usize) -> bool {
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
    pub fn make_available(&self) {
        self.avl.store(true, Ordering::Release);
    }

    #[inline(always)]
    pub fn alloc(&mut self, sizeof_alloc: usize) -> *mut T {
        debug_assert!(
            self.avl.load(Ordering::Relaxed) == false,
            "Arena::alloc called without holding lock!"
        );

        let sizeof_alloc = sizeof_alloc as u64;
        let start = self.off;
        let end = start + sizeof_alloc;

        unsafe {
            debug_assert!(end <= self.len);
            std::hint::assert_unchecked(end <= self.len);
        }

        self.off = end;
        unsafe { self.ptr.as_ptr().add(start as usize) }
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
        debug_assert!(cnt < u16::MAX);
    }

    #[inline(always)]
    pub fn decrement_strong_count(&self) {
        // SAFETY: Release ensures all writes to arena data happen before refcnt reaches 0
        let cnt = self.cnt.fetch_sub(1, Ordering::Release);
        debug_assert!(cnt > 0);
    }
}

pub struct ArenaPool<T: bytemuck::Pod> {
    _mmap: memmap2::MmapMut,
    inner_buf_arenas: Vec<UnsafeCell<Arena<T>>>,
    inner_cap_arenas: usize,
    inner_idx_hint: AtomicUsize,

    sizeof_buffer: ByteSize,
    sizeof_arena: ByteSize,
}

unsafe impl<T: bytemuck::Pod + Send> Send for ArenaPool<T> {}
unsafe impl<T: bytemuck::Pod + Sync> Sync for ArenaPool<T> {}

impl<T: bytemuck::Pod> ArenaPool<T> {
    pub fn new(sizeof_buffer: bytesize::ByteSize, sizeof_arena: bytesize::ByteSize) -> Self {
        let countof_arenas = (sizeof_buffer.as_u64() / sizeof_arena.as_u64()) as usize;
        let capof_arenas = sizeof_arena.as_u64() as usize / size_of::<T>();

        //TODO: return errors
        assert!(
            countof_arenas >= 2,
            "need at least 2 arenas to prevent stalls (higher strongly recommended)"
        );
        // assert!(
        //     sizeof_buffer >= DEFAULT_MIN_SIZEOF_BUFFER,
        //     "buffer size should be at least {:?} (higher strongly recommended)",
        //     DEFAULT_MIN_SIZEOF_BUFFER
        // );
        // assert!(
        //     sizeof_arena >= DEFAULT_MIN_SIZEOF_ARENA,
        //     "arena size should be at least {:?} (higher strongly recommended)",
        //     DEFAULT_MIN_SIZEOF_ARENA
        // );

        unsafe {
            let mut vec_arenas = Vec::with_capacity(countof_arenas);
            // TODO: construct with MmapOptions for explicit use of Huge Pages?
            let mut mmap = MmapOptions::new()
                .len(sizeof_buffer.as_u64() as usize)
                .huge(None)
                .map_anon()
                .unwrap_or_else(|_| { 
                    let mmap = MmapOptions::new()
                        .len(sizeof_buffer.as_u64() as usize)
                        .map_anon()
                        .unwrap();
                    #[cfg(target_os = "linux")]
                    {
                        let _ = mmap.advise(memmap2::Advice::HugePage);
                    }
                    mmap
                });

            #[cfg(target_os = "linux")] 
            {
                let _ = mmap.advise(memmap2::Advice::WillNeed);
            }

            let ptrbase_arena = mmap.as_mut_ptr() as *mut T;
            for i in 0..countof_arenas {
                let ptr_arena_start = ptrbase_arena.add(i * capof_arenas);
                vec_arenas.push(UnsafeCell::new(Arena::from_slice(
                    ptr_arena_start,
                    capof_arenas,
                )));
            }

            Self {
                _mmap: mmap,
                inner_cap_arenas: capof_arenas,
                inner_buf_arenas: vec_arenas,
                inner_idx_hint: AtomicUsize::new(0),

                sizeof_buffer: sizeof_buffer,
                sizeof_arena: sizeof_arena,
            }
        }
    }

    pub fn alloc(&self, len: usize) -> ArenaSlice<T> {
        assert!(
            len <= self.inner_cap_arenas,
            "{:?} > {:?}",
            len,
            self.inner_cap_arenas
        );
        let countof_arenas = self.inner_buf_arenas.len();

        unsafe {
            debug_assert!(countof_arenas > 0);
            std::hint::assert_unchecked(countof_arenas > 0);
        }
        let mut count_spun = 0;

        loop {
            for i in 0..countof_arenas {
                let idx = (self.inner_idx_hint.load(Ordering::Relaxed) + i) % countof_arenas;
                unsafe {
                    debug_assert!(idx < countof_arenas);
                    std::hint::assert_unchecked(idx < countof_arenas);
                }

                // SAFETY   The atomic lock in available() ensures exclusive access
                let arena = unsafe { &mut *self.inner_buf_arenas[idx].get() };
                if arena.is_available(len) {
                    self.inner_idx_hint.store(idx, Ordering::Relaxed);
                    let ptr = arena.alloc(len);

                    // SAFETY   ArenaPool outlives all ArenaSlices due to drop impl
                    //          ArenaSlice::new increments strong count
                    let slice = unsafe {
                        ArenaSlice::from_raw_parts(
                            std::slice::from_raw_parts_mut(ptr, len),
                            SendPtr::new_unchecked(arena as *mut Arena<T>),
                        )
                    };
                    arena.make_available();

                    return slice;
                }
            }

            spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                &mut count_spun,
                "ArenaPool (alloc): waiting for arena to be freed",
            );
        }
    }
}

impl<T: bytemuck::Pod> Drop for ArenaPool<T> {
    fn drop(&mut self) {
        let mut count_spun = 0;

        'wait: loop {
            for arena_cell in &self.inner_buf_arenas {
                // SAFETY: We're in drop, no other threads can access arenas
                let arena = unsafe { &*arena_cell.get() };
                if arena.cnt.load(Ordering::Relaxed) != 0 {
                    spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                        &mut count_spun,
                        "ArenaPool (drop): waiting for arena to be freed",
                    );
                    continue 'wait;
                }
            }
            break;
        }

        std::sync::atomic::fence(Ordering::Acquire);
    }
}
