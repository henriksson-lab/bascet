use std::sync::LazyLock;

use bytesize::ByteSize;

// NOTE: not true on ALL systems. Most common.
pub const SIZEOF_CACHE_LINE: usize = 64;

pub static DEFAULT_SIZEOF_ARENA: LazyLock<ByteSize> = LazyLock::new(|| {
    cache_size::l3_cache_size()
        .map(|size| ByteSize::b(size as u64))
        .unwrap_or(DEFAULT_MIN_SIZEOF_ARENA)
        .max(DEFAULT_MIN_SIZEOF_ARENA)
});
pub const DEFAULT_MIN_SIZEOF_ARENA: ByteSize = ByteSize::mib(1);

pub const DEFAULT_SIZEOF_BUFFER: ByteSize = ByteSize::gib(4);
pub const DEFAULT_MIN_SIZEOF_BUFFER: ByteSize = ByteSize::mib(32);
