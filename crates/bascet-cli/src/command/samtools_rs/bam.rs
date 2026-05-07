// BAM binary format: header (magic + SAM text + binary @SQ table) + records.
// Spec: https://samtools.github.io/hts-specs/SAMv1.pdf §4.2

use std::io::{self, Read, Write};

#[derive(Clone)]
pub struct Header {
    /// SAM-format header text (the @HD/@SQ/@RG/@PG/@CO lines, as bytes).
    /// Stored verbatim so we can write it back unchanged.
    pub text: Vec<u8>,
    pub refs: Vec<RefInfo>,
}

#[derive(Clone)]
pub struct RefInfo {
    /// Reference name without the trailing NUL byte.
    pub name: Vec<u8>,
    pub length: i32,
}

impl Header {
    pub fn read<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != b"BAM\x01" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a BAM file"));
        }
        let l_text = read_i32(r)?;
        if l_text < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative l_text",
            ));
        }
        let mut text = vec![0u8; l_text as usize];
        r.read_exact(&mut text)?;

        let n_ref = read_i32(r)?;
        if n_ref < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "negative n_ref"));
        }
        let mut refs = Vec::with_capacity(n_ref as usize);
        for _ in 0..n_ref {
            let l_name = read_i32(r)?;
            if l_name <= 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad l_name"));
            }
            let mut name = vec![0u8; l_name as usize];
            r.read_exact(&mut name)?;
            if name.last() != Some(&0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ref name not NUL-terminated",
                ));
            }
            name.pop();
            let length = read_i32(r)?;
            refs.push(RefInfo { name, length });
        }
        Ok(Header { text, refs })
    }

    pub fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(b"BAM\x01")?;
        write_i32(w, self.text.len() as i32)?;
        w.write_all(&self.text)?;
        write_i32(w, self.refs.len() as i32)?;
        for r in &self.refs {
            write_i32(w, (r.name.len() + 1) as i32)?;
            w.write_all(&r.name)?;
            w.write_all(&[0])?;
            write_i32(w, r.length)?;
        }
        Ok(())
    }

    /// Number of bytes `write` produces for this header. Used to seed the
    /// uncompressed-offset counter for BAI generation, since records'
    /// virtual offsets are measured from the start of the file (header
    /// included).
    pub fn serialized_len(&self) -> usize {
        let mut n = 4 + 4 + self.text.len() + 4;
        for r in &self.refs {
            n += 4 + r.name.len() + 1 + 4;
        }
        n
    }
}

/// One BAM alignment record. `data` is everything *after* the 4-byte block_size
/// prefix: a 32-byte fixed core followed by the variable section
/// (read_name, cigar, seq, qual, aux). Owning the bytes keeps sorting simple
/// — we shuffle `Record` values rather than indices into a shared buffer.
#[derive(Clone)]
pub struct Record {
    pub data: Vec<u8>,
}

impl Record {
    /// Read one record. Returns `Ok(None)` at end-of-stream.
    pub fn read<R: Read>(r: &mut R) -> io::Result<Option<Self>> {
        Self::read_into(r, Vec::new())
    }

    /// Read into a pre-allocated buffer (recycled from a pool, typically).
    /// Returns the record on success; on EOF, returns `Ok(None)` and the
    /// caller's `buf` is dropped. Avoids the zero-init cost of `vec![0; n]`
    /// — `read_exact` writes every byte before any access.
    pub fn read_into<R: Read>(r: &mut R, mut buf: Vec<u8>) -> io::Result<Option<Self>> {
        let mut bs = [0u8; 4];
        match r.read_exact(&mut bs) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let block_size = i32::from_le_bytes(bs);
        if block_size < 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BAM record block_size < 32",
            ));
        }
        let n = block_size as usize;
        buf.clear();
        buf.reserve_exact(n);
        // SAFETY: u8 has no invalid bit patterns. We immediately call
        // read_exact, which writes all `n` bytes before any access. If
        // read_exact errors, `buf` is dropped without exposing the
        // uninitialized contents (which would be sound to read anyway as
        // u8 — this just avoids the `Vec::resize` zero-write, which costs
        // ~50 ns per typical record allocation).
        unsafe {
            buf.set_len(n);
        }
        r.read_exact(&mut buf)?;
        let rec = Record { data: buf };
        rec.validate_layout()?;
        Ok(Some(rec))
    }

    pub fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        write_i32(w, self.data.len() as i32)?;
        w.write_all(&self.data)
    }

    /// Sanity-check that the variable section's lengths fit inside `data`.
    fn validate_layout(&self) -> io::Result<()> {
        let l_read_name = self.l_read_name() as usize;
        let n_cigar = self.n_cigar_op() as usize;
        let l_seq = self.l_seq();
        if l_read_name == 0 || l_seq < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad BAM record fields",
            ));
        }
        let l_seq = l_seq as usize;
        let needed = 32 + l_read_name + 4 * n_cigar + (l_seq + 1) / 2 + l_seq;
        if needed > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BAM record variable section overflows block_size",
            ));
        }
        if self.data[32 + l_read_name - 1] != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BAM read name not NUL-terminated",
            ));
        }
        Ok(())
    }

    pub fn ref_id(&self) -> i32 {
        i32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }
    pub fn set_ref_id(&mut self, ref_id: i32) {
        self.data[0..4].copy_from_slice(&ref_id.to_le_bytes());
    }
    pub fn pos(&self) -> i32 {
        i32::from_le_bytes(self.data[4..8].try_into().unwrap())
    }
    pub fn l_read_name(&self) -> u8 {
        self.data[8]
    }
    pub fn mapq(&self) -> u8 {
        self.data[9]
    }
    pub fn bin(&self) -> u16 {
        u16::from_le_bytes(self.data[10..12].try_into().unwrap())
    }
    pub fn n_cigar_op(&self) -> u16 {
        u16::from_le_bytes(self.data[12..14].try_into().unwrap())
    }
    pub fn flag(&self) -> u16 {
        u16::from_le_bytes(self.data[14..16].try_into().unwrap())
    }
    pub fn l_seq(&self) -> i32 {
        i32::from_le_bytes(self.data[16..20].try_into().unwrap())
    }
    pub fn next_ref_id(&self) -> i32 {
        i32::from_le_bytes(self.data[20..24].try_into().unwrap())
    }
    pub fn set_next_ref_id(&mut self, ref_id: i32) {
        self.data[20..24].copy_from_slice(&ref_id.to_le_bytes());
    }
    pub fn next_pos(&self) -> i32 {
        i32::from_le_bytes(self.data[24..28].try_into().unwrap())
    }
    pub fn tlen(&self) -> i32 {
        i32::from_le_bytes(self.data[28..32].try_into().unwrap())
    }

    /// Read name without the trailing NUL byte.
    pub fn read_name(&self) -> &[u8] {
        let end = 32 + self.l_read_name() as usize - 1;
        &self.data[32..end]
    }

    fn cigar_off(&self) -> usize {
        32 + self.l_read_name() as usize
    }
    fn seq_off(&self) -> usize {
        self.cigar_off() + 4 * self.n_cigar_op() as usize
    }
    fn qual_off(&self) -> usize {
        self.seq_off() + (self.l_seq() as usize + 1) / 2
    }
    fn aux_off(&self) -> usize {
        self.qual_off() + self.l_seq() as usize
    }

    pub fn cigar_raw(&self) -> &[u8] {
        &self.data[self.cigar_off()..self.seq_off()]
    }
    pub fn seq_raw(&self) -> &[u8] {
        &self.data[self.seq_off()..self.qual_off()]
    }
    pub fn qual(&self) -> &[u8] {
        &self.data[self.qual_off()..self.aux_off()]
    }
    pub fn aux(&self) -> &[u8] {
        &self.data[self.aux_off()..]
    }
}

fn read_i32<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
}

fn write_i32<W: Write>(w: &mut W, v: i32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::samtools_rs::bgzf;
    use std::io::{Cursor, Read};

    fn read_test_bam_decompressed() -> Vec<u8> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/small_unsorted.bam");
        let bytes = std::fs::read(path).unwrap();
        let mut out = Vec::new();
        bgzf::Reader::new(Cursor::new(bytes))
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn header_parses_test_bam() {
        let bytes = read_test_bam_decompressed();
        let mut cur = Cursor::new(&bytes);
        let h = Header::read(&mut cur).unwrap();
        assert_eq!(h.refs.len(), 3);
        assert_eq!(h.refs[0].name, b"chr1");
        assert_eq!(h.refs[0].length, 1000);
        assert_eq!(h.refs[1].name, b"chr2");
        assert_eq!(h.refs[1].length, 2000);
        assert_eq!(h.refs[2].name, b"chr3");
        assert_eq!(h.refs[2].length, 1500);
        // Header text contains @HD and @SQ lines.
        let text = std::str::from_utf8(&h.text).unwrap();
        assert!(text.contains("@HD"));
        assert!(text.contains("SN:chr1"));
    }

    #[test]
    fn records_parse_test_bam() {
        let bytes = read_test_bam_decompressed();
        let mut cur = Cursor::new(&bytes);
        let _h = Header::read(&mut cur).unwrap();

        let mut recs = Vec::new();
        while let Some(r) = Record::read(&mut cur).unwrap() {
            recs.push(r);
        }
        assert_eq!(recs.len(), 8);

        // Order in the unsorted file: read005, read002, read001, read004, read003, read007, read006, read008
        let names: Vec<_> = recs.iter().map(|r| r.read_name().to_vec()).collect();
        let expect: &[&[u8]] = &[
            b"read005", b"read002", b"read001", b"read004", b"read003", b"read007", b"read006",
            b"read008",
        ];
        assert_eq!(names, expect);

        // First record: read005, ref=chr2 (idx 1), pos 500 (BAM 0-based = 499), 10M cigar.
        assert_eq!(recs[0].ref_id(), 1);
        assert_eq!(recs[0].pos(), 499);
        assert_eq!(recs[0].mapq(), 60);
        assert_eq!(recs[0].l_seq(), 10);
        assert_eq!(recs[0].n_cigar_op(), 1);

        // Unmapped record: read007 with FLAG=4, ref_id=-1, pos=-1.
        let r7 = &recs[5];
        assert_eq!(r7.read_name(), b"read007");
        assert_eq!(r7.flag(), 4);
        assert_eq!(r7.ref_id(), -1);
        assert_eq!(r7.pos(), -1);
    }

    #[test]
    fn header_and_records_roundtrip_to_identical_bytes() {
        let bytes = read_test_bam_decompressed();
        let mut cur = Cursor::new(&bytes);
        let h = Header::read(&mut cur).unwrap();
        let mut recs = Vec::new();
        while let Some(r) = Record::read(&mut cur).unwrap() {
            recs.push(r);
        }
        let mut out = Vec::new();
        h.write(&mut out).unwrap();
        for r in &recs {
            r.write(&mut out).unwrap();
        }
        assert_eq!(out, bytes, "BAM round-trip not byte-identical");
    }
}
