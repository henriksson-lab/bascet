use std::{
    io::{Seek, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use binrw::{binrw, BinWrite};
use smart_default::SmartDefault;

#[derive(Debug, Clone, SmartDefault)]
#[binrw]
#[brw(little)]
#[allow(non_snake_case)]
pub struct BBGZHeader {
    // Magic number. Must be 0x1F
    #[default = 0x1F]
    pub ID1: u8,
    // Magic number. Must be 0x8B
    #[default = 0x8B]
    pub ID2: u8,
    // Compression method. Must be 8 (Deflate)
    #[default = 8]
    pub CM: u8,
    // Flags (FTEXT | FEXTRA)
    #[default = 0b0000_0101]
    pub FLG: u8,
    // Unix timestamp or 0 if unavailable
    pub MTIME: u32,
    // Extra flags: 0=None, 2=Best compression, 4=Fastest (not sure this matters)
    #[default = 2]
    pub XFL: u8,
    // Filesystem: 255=Unknown (this is irrelevant to us)
    #[default = 255]
    pub OS: u8,
    // Size of extra field
    pub XLEN: u16,
    // Extra field subfields
    #[brw(ignore)]
    pub FEXTRA: Vec<BBGZExtra>,
}

impl BBGZHeader {
    pub const SSIZE: usize = 12;

    pub fn new() -> Self {
        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        Self {
            MTIME: mtime,
            ..Default::default()
        }
    }

    pub fn add_extra(&mut self, id: &[u8; 2], data: Vec<u8>) -> &mut Self {
        self.FEXTRA.push(BBGZExtra::new(id, data));
        self
    }

    pub fn write_header<W>(&mut self, writer: &mut W, clen: usize) -> std::io::Result<()>
    where
        W: Write,
    {
        let xlen: usize = self.FEXTRA.iter().map(|e| e.size()).sum();
        // NOTE: BGZFExtra only has a static size
        let xlen: usize = xlen + BGZFExtra::SSIZE;
        self.XLEN = xlen as u16;

        let bsize = BBGZHeader::SSIZE + xlen + clen + BBGZTrailer::SSIZE - 1;

        // Write header manually to avoid Seek requirement
        writer.write_all(&[self.ID1])?;
        writer.write_all(&[self.ID2])?;
        writer.write_all(&[self.CM])?;
        writer.write_all(&[self.FLG])?;
        writer.write_all(&self.MTIME.to_le_bytes())?;
        writer.write_all(&[self.XFL])?;
        writer.write_all(&[self.OS])?;
        writer.write_all(&self.XLEN.to_le_bytes())?;

        // Write FEXTRA manually
        for extra in &self.FEXTRA {
            writer.write_all(&[extra.SI1])?;
            writer.write_all(&[extra.SI2])?;
            writer.write_all(&(extra.DATA.len() as u16).to_le_bytes())?;
            writer.write_all(&extra.DATA)?;
        }

        // NOTE: bgzf extra field must be written last, otherwise bgzip with multiple threads breaks
        let bgzf_extra = BGZFExtra::new(bsize as u16);
        writer.write_all(&[bgzf_extra.SI1])?;
        writer.write_all(&[bgzf_extra.SI2])?;
        writer.write_all(&bgzf_extra.LEN.to_le_bytes())?;
        writer.write_all(&bgzf_extra.BSIZE.to_le_bytes())?;

        Ok(())
    }
}

#[derive(Debug, Clone, SmartDefault)]
#[binrw]
#[brw(little)]
#[allow(non_snake_case)]
pub struct BBGZExtra {
    // Subfield ID byte 1
    pub SI1: u8,
    // Subfield ID byte 2
    pub SI2: u8,
    // Length of DATA
    pub LEN: u16,
    // Subfield data
    #[br(count = LEN)]
    pub DATA: Vec<u8>,
}

impl BBGZExtra {
    pub const SSIZE: usize = 4;

    pub fn new(id: &[u8; 2], data: Vec<u8>) -> Self {
        Self {
            SI1: id[0],
            SI2: id[1],
            LEN: data.len() as u16,
            DATA: data,
        }
    }

    pub fn size(&self) -> usize {
        Self::SSIZE + self.DATA.len()
    }
}

#[derive(Debug, Clone, SmartDefault)]
#[binrw]
#[brw(little)]
#[allow(non_snake_case)]
pub struct BGZFExtra {
    #[default = b'B']
    pub SI1: u8,
    #[default = b'C']
    pub SI2: u8,
    // Length of DATA (only 2 for BSIZE)
    #[default = 2]
    pub LEN: u16,
    // TOTAL Block size minus 1
    pub BSIZE: u16,
}

impl BGZFExtra {
    pub const SSIZE: usize = 6;

    pub fn new(bsize: u16) -> Self {
        Self {
            BSIZE: bsize,
            ..Default::default()
        }
    }
}

impl From<BGZFExtra> for BBGZExtra {
    fn from(bgzf: BGZFExtra) -> Self {
        Self {
            SI1: bgzf.SI1,
            SI2: bgzf.SI2,
            LEN: bgzf.LEN,
            DATA: bgzf.BSIZE.to_le_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
#[binrw]
#[brw(little)]
#[allow(non_snake_case)]
pub struct BBGZTrailer {
    // CRC-32 of uncompressed data
    pub CRC32: u32,
    // Size of uncompressed data (mod 2^32)
    pub ISIZE: u32,
}

impl BBGZTrailer {
    pub const SSIZE: usize = 8;

    pub fn new(crc32: u32, isize: u32) -> Self {
        Self {
            CRC32: crc32,
            ISIZE: isize,
        }
    }

    pub fn write_trailer<W>(&self, writer: &mut W) -> std::io::Result<()>
    where
        W: Write,
    {
        writer.write_all(&self.CRC32.to_le_bytes())?;
        writer.write_all(&self.ISIZE.to_le_bytes())?;
        Ok(())
    }
}
