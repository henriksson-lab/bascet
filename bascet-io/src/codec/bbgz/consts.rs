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
#[allow(non_upper_case_globals)]
pub const MAX_SIZEOF_FEXTRAusize: usize = MAX_SIZEOF_FEXTRA.as_u64() as usize;

pub const MARKER_DEFLATE_ALIGN_BYTES: [u8; 5] = [0x00, 0x00, 0x00, 0xFF, 0xFF];
#[allow(non_upper_case_globals)]
pub const SIZEOF_MARKER_DEFLATE_ALIGN_BYTESusize: usize = MARKER_DEFLATE_ALIGN_BYTES.len();

pub const MARKER_EOF: [u8; 28] = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];
#[allow(non_upper_case_globals)]
pub const SIZEOF_MARKER_EOFusize: usize = MARKER_EOF.len();

// The windowBits parameter is the base two logarithm of the window size (the size of the history buffer).
// windowBits can be –8..–15 for raw deflate. In this case, -windowBits determines the window size. 
// deflate() will then generate raw deflate data with no zlib header or trailer, and will not compute a check value. 
pub(crate) const ZLIB_WINDOW_SIZE: i8 = -15;
// The memLevel parameter specifies how much memory should be allocated for the internal compression state.
// memLevel=1 uses minimum memory but is slow and reduces compression ratio; 
// memLevel=9 uses maximum memory for optimal speed. 
// See zconf.h for total memory usage as a function of windowBits and memLevel. 
pub(crate) const ZLIB_MEM_LEVEL: i8 = 9;