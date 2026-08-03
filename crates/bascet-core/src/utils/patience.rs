use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temper<T> {
    Eager(T),
    Patient(T),
}

pub struct AtomicPatience {
    word: AtomicU64,
}

impl AtomicPatience {
    pub fn new() -> Self {
        Self {
            word: AtomicU64::new(pack(1, u32::from(u16::MAX))),
        }
    }

    #[inline(always)]
    pub fn hit(&self) {
        let _ = self
            .word
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |word| {
                let (patience, thresh) = unpack(word);
                let patience = if patience < thresh {
                    patience.saturating_mul(2)
                } else {
                    patience.saturating_add(1)
                }
                .min(u32::from(u16::MAX));
                Some(pack(patience, thresh))
            });
    }

    #[inline(always)]
    pub fn miss(&self) -> Temper<u32> {
        let prev = self
            .word
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |word| {
                let thresh = unpack(word).0 >> 1;
                Some(pack(thresh, thresh))
            })
            .unwrap();
        let thresh = unpack(prev).0 >> 1;
        if thresh == 0 {
            Temper::Eager(thresh)
        } else {
            Temper::Patient(thresh)
        }
    }

    #[inline(always)]
    pub fn patience(&self) -> u32 {
        unpack(self.word.load(Ordering::Relaxed)).0
    }
}

#[inline(always)]
fn pack(patience: u32, thresh: u32) -> u64 {
    u64::from(patience) | (u64::from(thresh) << 32)
}

#[inline(always)]
fn unpack(word: u64) -> (u32, u32) {
    (word as u32, (word >> 32) as u32)
}
