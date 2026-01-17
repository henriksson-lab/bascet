use std::{io::Write, u64};

use bytemuck::{Pod, Zeroable};

// GZIP trailer
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[allow(non_snake_case)]
pub struct BBGZTrailer {
    /// CRC-32 of uncompressed data
    pub CRC32: u32,
    /// Size of uncompressed data (mod 2^32)
    pub ISIZE: u32,
}

impl BBGZTrailer {
    pub const SSIZE: usize = 8;

    #[inline]
    pub fn new(crc32: u32, isize: u32) -> Self {
        Self {
            CRC32: crc32,
            ISIZE: isize,
        }
    }

    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
        match bytes.get(..Self::SSIZE) {
            Some(b) => match bytemuck::try_from_bytes(b) {
                Ok(v) => Ok(*v),
                Err(_) => Err(()),
            },
            None => Err(()),
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    #[inline]
    pub fn write_with<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(self.as_bytes())
    }

    pub fn merge(&mut self, other: Self) -> Result<&mut Self, ()> {
        let mut hasher_self = crc32fast::Hasher::new_with_initial_len(self.CRC32, self.ISIZE as u64);
        let hasher_other = crc32fast::Hasher::new_with_initial_len(other.CRC32, other.ISIZE as u64);

        hasher_self.combine(&hasher_other);
        self.CRC32 = hasher_self.finalize();
        self.ISIZE = match self.ISIZE.checked_add(other.ISIZE) {
            Some(isize) => isize,
            None => return Err(())
        };

        return Ok(self);
    }
}
