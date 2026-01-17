use std::io::Write;

use bytemuck::{Pod, Zeroable};

use crate::{BBGZTrailer, MAX_SIZEOF_BLOCKusize};

// NOTE this is very much an incomplete and unsound implementation of the _general_ gzip protocol
//      and the bgzf protocol. However, we right now generate this data as the sole source
//      and therefore for now, we are able to ignore certain things like the FLG field or
//      additional trailer fields. This may change in the future

// Complete GZIP header with extra fields
#[derive(Debug, Clone)]
#[allow(non_snake_case)]
pub struct BBGZHeader {
    pub BASE: BBGZHeaderBase,
    pub BC: BGZFExtra,
    pub FEXTRA: Vec<BBGZExtra>,

    pub size: usize,
}

impl BBGZHeader {
    pub fn new() -> Self {
        Self {
            BASE: BBGZHeaderBase::TEMPLATE,
            BC: BGZFExtra::TEMPLATE,
            FEXTRA: Vec::new(),

            size: BBGZHeaderBase::SSIZE + BGZFExtra::SSIZE,
        }
    }

    pub unsafe fn add_extra_unchecked(&mut self, id: &[u8; 2], data: Vec<u8>) -> &mut Self {
        self.size += BBGZExtra::SSIZE + data.len();
        self.FEXTRA.push(BBGZExtra::new(id, data));
        return self;
    }

    pub fn add_extra(&mut self, id: &[u8; 2], data: Vec<u8>) -> Result<&mut Self, ()> {
        if self.FEXTRA.iter().any(|e| e.SI1 == id[0] && e.SI2 == id[1]) {
            return Err(());
        }
        self.size += BBGZExtra::SSIZE + data.len();
        self.FEXTRA.push(BBGZExtra::new(id, data));
        return Ok(self);
    }

    pub fn size(&self) -> usize {
        return self.size;
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
        let base = BBGZHeaderBase::from_bytes(bytes)?;
        let xlen = base.XLEN as usize;

        let extras_start = BBGZHeaderBase::SSIZE;
        let extras_end = extras_start + xlen;
        if bytes.len() < extras_end {
            return Err(());
        }

        let mut header = Self {
            BASE: base,
            BC: BGZFExtra::TEMPLATE,
            FEXTRA: Vec::new(),
            size: BBGZHeaderBase::SSIZE + BGZFExtra::SSIZE,
        };

        let mut cursor = extras_start;
        while cursor + BBGZExtra::SSIZE <= extras_end {
            let si1 = bytes[cursor];
            let si2 = bytes[cursor + 1];
            let len = u16::from_le_bytes([bytes[cursor + 2], bytes[cursor + 3]]) as usize;

            let data_start = cursor + BBGZExtra::SSIZE;
            let data_end = data_start + len;
            if data_end > extras_end {
                return Err(());
            }

            match (si1, si2) {
                (b'B', b'C') => header.BC = BGZFExtra::from_bytes(&bytes[cursor..])?,
                _ => {
                    let _ = header.add_extra(&[si1, si2], bytes[data_start..data_end].to_vec());
                }
            }
            cursor = data_end;
        }

        Ok(header)
    }

    pub fn merge(&mut self, other: Self) -> Result<&mut Self, ()> {
        // if ((self.BC.BSIZE as usize) + (other.BC.BSIZE as usize) + 1) > MAX_SIZEOF_BLOCKusize {
        //     let ss = self.BC.BSIZE;
        //     let os = other.BC.BSIZE;
        //     eprintln!("{:?} + {:?} = {:?}", ss, os, (ss as u64) + (os as u64) + 1);
        //     return Err(());
        // }
        self.BASE.MTIME = self.BASE.MTIME.max(other.BASE.MTIME);
        for fmerge in other.FEXTRA {
            // NOTE I do not think checking if xlen > usize_MAX_SIZEOF_FEXTRA is neccessary
            //      because BSIZE is total blocksize
            // NOTE add_extra only returns if it added the field successfully
            let _ = self.add_extra(&[fmerge.SI1, fmerge.SI2], fmerge.DATA);
        }

        return Ok(self);
    }

    pub unsafe fn merge_unchecked(&mut self, other: Self) -> &mut Self {
        self.BASE.MTIME = self.BASE.MTIME.max(other.BASE.MTIME);
        for fmerge in other.FEXTRA {
            self.add_extra_unchecked(&[fmerge.SI1, fmerge.SI2], fmerge.DATA);
        }

        return self;
    }

    pub fn write_with_csize<W: Write>(
        &mut self,
        writer: &mut W,
        csize: usize,
    ) -> std::io::Result<()> {
        self.BASE.XLEN = (self.size() - BBGZHeaderBase::SSIZE).try_into().expect("Overflow");
        self.BC.BSIZE = (self.size() + csize + BBGZTrailer::SSIZE - 1).try_into().expect("Overflow");

        writer.write_all(self.BASE.as_bytes())?;

        for extra in &self.FEXTRA {
            writer.write_all(&[extra.SI1, extra.SI2])?;
            writer.write_all(&(extra.DATA.len() as u16).to_le_bytes())?;
            writer.write_all(&extra.DATA)?;
        }

        // HACK bgzf extra field must be written last, otherwise bgzip with multiple threads breaks
        writer.write_all(self.BC.as_bytes())?;
        Ok(())
    }
}

/// Base BGZF/GZIP header (without FEXTRA)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[allow(non_snake_case)]
pub struct BBGZHeaderBase {
    /// Magic number (0x1F)
    pub ID1: u8,
    /// Magic number (0x8B)
    pub ID2: u8,
    /// Compression method (8 = Deflate)
    pub CM: u8,
    /// Flags (FTEXT | FEXTRA = 0x05)
    pub FLG: u8,
    /// Unix timestamp
    pub MTIME: u32,
    /// Extra flags (2 = best compression)
    pub XFL: u8,
    /// Filesystem (255 = unknown)
    pub OS: u8,
    /// Size of extra field
    pub XLEN: u16,
}

impl BBGZHeaderBase {
    pub const SSIZE: usize = 12;
    pub const TEMPLATE: Self = Self {
        ID1: 0x1F,
        ID2: 0x8B,
        CM: 8,
        FLG: 0x05,
        MTIME: 0,
        XFL: 2,
        OS: 255,
        XLEN: 0,
    };

    #[inline]
    pub fn new(mtime: u32, xlen: u16) -> Self {
        Self {
            MTIME: mtime,
            XLEN: xlen,
            ..Self::TEMPLATE
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
}

// Generic extra field
#[derive(Debug, Clone)]
#[allow(non_snake_case)]
pub struct BBGZExtra {
    pub SI1: u8,
    pub SI2: u8,
    pub DATA: Vec<u8>,
}

impl BBGZExtra {
    pub const SSIZE: usize = 4; // SI1 + SI2 + LEN1 + LEN2

    pub fn new(id: &[u8; 2], data: Vec<u8>) -> Self {
        Self {
            SI1: id[0],
            SI2: id[1],
            DATA: data,
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        Self::SSIZE + self.DATA.len()
    }
}

// BGZF extra field
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[allow(non_snake_case)]
pub struct BGZFExtra {
    /// Subfield ID byte 1 ('B')
    pub SI1: u8,
    /// Subfield ID byte 2 ('C')
    pub SI2: u8,
    /// Length of data (always 2)
    pub LEN: u16,
    /// Total block size minus 1
    pub BSIZE: u16,
}

impl BGZFExtra {
    pub const SSIZE: usize = 6;

    pub const TEMPLATE: Self = Self {
        SI1: b'B',
        SI2: b'C',
        LEN: 2,
        BSIZE: 0,
    };

    #[inline]
    pub fn new(bsize: u16) -> Self {
        Self {
            BSIZE: bsize,
            ..Self::TEMPLATE
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
}
