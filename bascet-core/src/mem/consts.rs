use bytesize::ByteSize;

pub const DEFAULT_SIZEOF_ARENA: ByteSize = ByteSize::mib(32);
pub const DEFAULT_MIN_SIZEOF_ARENA: ByteSize = ByteSize::mib(1);

pub const DEFAULT_SIZEOF_BUFFER: ByteSize = ByteSize::gib(1);
pub const DEFAULT_MIN_SIZEOF_BUFFER: ByteSize = ByteSize::mib(32);
