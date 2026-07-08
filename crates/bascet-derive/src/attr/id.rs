use std::hash::{Hash, Hasher};

// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
pub const PRIME: u64 = 0x00000100000001b3;

pub struct AttrId;

impl AttrId {
    pub fn from_name(name: &str) -> u64 {
        let mut h = fnv::FnvHasher::default();
        name.hash(&mut h);
        h.finish()
    }
}
