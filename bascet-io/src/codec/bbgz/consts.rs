use bounded_integer::BoundedI32;
use bytesize::ByteSize;

pub const MIN_SIZEOF_BLOCK: ByteSize = ByteSize::kib(4);
pub const usize_MIN_SIZEOF_BLOCK: usize = MIN_SIZEOF_BLOCK.as_u64() as usize;
pub const MAX_SIZEOF_BLOCK: ByteSize = ByteSize::kib(64);
pub const usize_MAX_SIZEOF_BLOCK: usize = MAX_SIZEOF_BLOCK.as_u64() as usize;
pub const MAX_SIZEOF_FEXTRA: ByteSize = ByteSize::kib(64);
