use bytesize::ByteSize;

pub const DEFAULT_SIZEOF_ARENA: ByteSize = ByteSize::mib(8);
pub const DEFAULT_MIN_SIZEOF_ARENA: ByteSize = ByteSize::mib(1);

pub const DEFAULT_SIZEOF_BUFFER: ByteSize = ByteSize::gib(1);
pub const DEFAULT_MIN_SIZEOF_BUFFER: ByteSize = ByteSize::mib(32);

pub const PATIENCE_INIT: u32 = 32;
pub const PATIENCE_GROWTH: u32 = 4;
pub const PATIENCE_DECAY: u32 = 1;
pub const PATIENCE_MIN: u32 = 1;
pub const PATIENCE_MAX: u32 = 128;
