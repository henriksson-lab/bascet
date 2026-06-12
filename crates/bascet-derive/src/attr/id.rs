use std::hash::{Hash, Hasher};

// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
pub const PRIME: u64 = 0x00000100000001b3;

pub fn hash(s: &str) -> u64 {
    let mut h = fnv::FnvHasher::default();
    s.hash(&mut h);
    h.finish()
}
