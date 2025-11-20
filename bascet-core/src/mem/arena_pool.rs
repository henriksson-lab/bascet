use std::mem::MaybeUninit;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

use bytemuck::Pod;
use bytesize::ByteSize;
use memmap2::MmapMut;

use crate::utils::spinpark_loop::{self, spinpark_loop, SpinPark};
use crate::{likely_unlikely, UnsafePtr, DEFAULT_MIN_SIZEOF_ARENA, DEFAULT_MIN_SIZEOF_BUFFER};

#[derive(Clone, Copy)]
pub struct ArenaSlice<'a, T> {
    pub inner: &'a [T],
    pub inner_ptr_src: UnsafePtr<Arena<T>>,
}

#[repr(C, align(64))]
pub struct Arena<T> {
    // allocator hot path
    ptr: *mut T,
    len: u64,
    off: u64,

    // cache line pad because allocator modifies offset_alloc, consumers modify refcnt
    _pad: MaybeUninit<[u8; 128 - 24]>,

    // consumer hot path
    refcnt: AtomicU64,
}

impl<T: Pod> Arena<T> {
    pub fn from_slice(ptr: *mut T, cap: usize) -> Self {
        Self {
            ptr,
            len: cap as u64,
            off: 0,
            _pad: MaybeUninit::uninit(),
            refcnt: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self, sizeof_alloc: usize) -> *mut T {
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
            .refcnt
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
    pub fn inc_ref(&self) {
        // SAFETY: just incrementing no data sync needed here as the value of this is not needed anywhere
        self.refcnt.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn dec_ref(&self) {
        // SAFETY: Release ensures all writes to arena data happen before refcnt reaches 0
        self.refcnt.fetch_sub(1, Ordering::Release);
    }
}

pub struct ArenaPool<T: Pod> {
    _mmap: MmapMut,

    inner_buf_arenas: Vec<Arena<T>>,
    inner_len_arenas: usize,
    inner_num_arenas_init: usize,
    inner_idx_current: usize,
    inner_idx_hint: usize,
}

unsafe impl<T: Pod + Send> Send for ArenaPool<T> {}
unsafe impl<T: Pod + Sync> Sync for ArenaPool<T> {}

impl<T: Pod> ArenaPool<T> {
    pub fn new(sizeof_buffer: ByteSize, sizeof_arena: ByteSize) -> std::io::Result<Self> {
        let num_arenas_init = (sizeof_buffer.as_u64() / sizeof_arena.as_u64()) as usize;

        assert!(
            num_arenas_init >= 2,
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
            let mut arenas = Vec::with_capacity(num_arenas_init);
            let len_arenas = sizeof_arena.as_u64() as usize / size_of::<T>();
            // TODO: construct with MmapOptions for explicit use of Huge Pages? Should be using huge pages anyway?
            let mut mmap = MmapMut::map_anon(sizeof_buffer.as_u64() as usize)?;
            let ptr_arenas_base = mmap.as_mut_ptr() as *mut T;
            for i in 0..num_arenas_init {
                let ptr_arena_start = ptr_arenas_base.add(i * len_arenas);
                arenas.push(Arena::from_slice(ptr_arena_start, len_arenas));
            }

            Ok(Self {
                _mmap: mmap,
                inner_num_arenas_init: num_arenas_init,
                inner_len_arenas: len_arenas,
                inner_buf_arenas: arenas,
                inner_idx_current: 0,
                inner_idx_hint: 0,
            })
        }
    }

    #[inline(always)]
    pub fn active(&self) -> &Arena<T> {
        &self.inner_buf_arenas[self.inner_idx_current]
    }

    #[inline(always)]
    pub fn active_mut(&mut self) -> &mut Arena<T> {
        &mut self.inner_buf_arenas[self.inner_idx_current]
    }

    pub fn alloc(&mut self, len: usize) -> ArenaSlice<'static, T> {
        assert!(
            len <= self.inner_len_arenas,
            "alloc size ({:?}, len {:?}) exceeds arena size ({:?}, cap {:?})",
            ByteSize::b((len * size_of::<T>()) as u64),
            len,
            ByteSize::b((self.inner_len_arenas * size_of::<T>()) as u64),
            self.active().capacity()
        );

        unsafe {
            let arena_current = self.active_mut();
            if arena_current.remaining() >= len {
                let ptr = arena_current.alloc(len);
                return ArenaSlice::<T> {
                    inner: std::slice::from_raw_parts(ptr, len),
                    inner_ptr_src: UnsafePtr::new_unchecked(arena_current),
                };
            }
        }

        let mut cnt_spin = 0;
        let mut cnt_park = 0;

        loop {
            unsafe {
                for i in 0..self.inner_num_arenas_init {
                    // start searching at hinted index
                    let idx = (self.inner_idx_hint + i) % self.inner_num_arenas_init;
                    let arena_found = &mut self.inner_buf_arenas[idx];

                    if arena_found.remaining() >= len || arena_found.try_reset() {
                        self.inner_idx_current = idx;
                        self.inner_idx_hint = (idx + 1) % self.inner_num_arenas_init;

                        let ptr = arena_found.alloc(len);
                        return ArenaSlice::<T> {
                            inner: std::slice::from_raw_parts(ptr, len),
                            inner_ptr_src: UnsafePtr::new_unchecked(arena_found),
                        };
                    }
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
            for i in 0..self.inner_num_arenas_init {
                if self.inner_buf_arenas[i].refcnt.load(Ordering::Relaxed) != 0 {
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
