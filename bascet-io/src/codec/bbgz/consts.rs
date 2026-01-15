use bytesize::ByteSize;

use crate::{BBGZExtra, BBGZHeaderBase, BBGZTrailer, BGZFExtra};

pub const MAX_SIZEOF_BLOCK: ByteSize = ByteSize::kib(64);
#[allow(non_upper_case_globals)]
pub const MAX_SIZEOF_BLOCKusize: usize = MAX_SIZEOF_BLOCK.as_u64() as usize;

#[allow(non_upper_case_globals)]
pub const MIN_SIZEOF_BLOCKusize: usize =
    BBGZHeaderBase::SSIZE + BBGZExtra::SSIZE + BGZFExtra::SSIZE + BBGZTrailer::SSIZE;
pub const MIN_SIZEOF_BLOCK: ByteSize = ByteSize(MIN_SIZEOF_BLOCKusize as u64);

#[allow(non_upper_case_globals)]
pub const MIN_SIZEOF_HEADERusize: usize =
    BBGZHeaderBase::SSIZE + BBGZExtra::SSIZE + BGZFExtra::SSIZE;
pub const MIN_SIZEOF_HEADER: ByteSize = ByteSize(MIN_SIZEOF_HEADERusize as u64);

pub const MAX_SIZEOF_FEXTRA: ByteSize = ByteSize::kib(64);

pub const MARKER_EOF: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];
