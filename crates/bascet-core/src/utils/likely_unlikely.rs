#[cold]
#[inline(never)]
const fn cold() {
    std::hint::black_box(());
}

#[inline(always)]
pub const fn likely(b: bool) -> bool {
    if !b {
        cold()
    }
    b
}

#[inline(always)]
pub fn unlikely(b: bool) -> bool {
    if b {
        cold()
    }
    b
}
