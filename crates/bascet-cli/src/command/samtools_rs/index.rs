// BAM index (BAI) builder.
// Spec: https://samtools.github.io/hts-specs/SAMv1.pdf §5
//
// We build the index incrementally as records are written:
//   * For each record, look up its bin (already in the BAM record) and
//     extend the bin's chunk list with [uoffset_start, uoffset_end). Adjacent
//     ranges from the same bin are merged on the fly to keep the index
//     compact (matches what htslib does).
//   * For the linear index, set `linear[w] = uoffset_start` for every 16 kbp
//     window `w` the record overlaps, but only on the first record to touch
//     that window — subsequent records are guaranteed to have larger virtual
//     offsets because input is coordinate-sorted.
//
// Virtual offsets are stored as raw uncompressed offsets during the build.
// Once the BGZF writer's per-block compressed offsets are known (after
// finish), we transform every uoffset → voffset = (coffset << 16) | inblock.

use super::bam::{Header, Record};
use super::bgzf::BLOCK_SIZE;
use std::collections::BTreeMap;
use std::io::{self, Write};

/// Bin number reserved for per-reference metadata (n_mapped/n_unmapped +
/// the virtual-offset range of mapped reads). Defined by the BAM spec.
const BIN_META: u32 = 37450;

/// Width of one linear-index window (samtools uses 16 KiB).
const LINEAR_WINDOW_SHIFT: i32 = 14;

pub struct BaiBuilder {
    refs: Vec<RefIndex>,
    n_no_coor: u64,
}

#[derive(Default)]
struct RefIndex {
    /// Sorted by bin id; emit order is bin-ascending which matches what
    /// `samtools` and htslib produce.
    bins: BTreeMap<u32, BinEntry>,
    /// Linear index: linear[w] = smallest uoffset of a record overlapping
    /// the 16 kbp window starting at `w << LINEAR_WINDOW_SHIFT`. Holds
    /// `u64::MAX` for windows with no overlap (rewritten to the previous
    /// non-empty entry on serialization, matching htslib's behaviour).
    /// CSI doesn't serialize the linear index but we still build it because
    /// it's also used to derive per-bin `loffset` values for CSI.
    linear: Vec<u64>,
    /// Metadata: voffset range of mapped records for this reference.
    mapped_first: Option<u64>,
    mapped_last_end: u64,
    n_mapped: u64,
    n_unmapped: u64,
}

#[derive(Default)]
struct BinEntry {
    chunks: Vec<Chunk>,
    /// Smallest uoffset of any record assigned directly to this bin.
    /// `u64::MAX` until the first record is added. Required for CSI;
    /// ignored by BAI's on-disk format.
    loffset: u64,
}

impl BinEntry {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            loffset: u64::MAX,
        }
    }
}

#[derive(Clone, Copy)]
struct Chunk {
    beg: u64,
    end: u64,
}

impl BaiBuilder {
    pub fn new(header: &Header) -> Self {
        let refs = (0..header.refs.len())
            .map(|_| RefIndex::default())
            .collect();
        Self { refs, n_no_coor: 0 }
    }

    /// Add a record to the index. `uoffset_start`/`uoffset_end` are the
    /// uncompressed file offsets where the record's serialized bytes
    /// (including the 4-byte block_size prefix) begin and end.
    pub fn add_record(&mut self, record: &Record, uoffset_start: u64, uoffset_end: u64) {
        let unmapped_flag = (record.flag() & 0x4) != 0;
        let ref_id = record.ref_id();
        if ref_id < 0 || (ref_id as usize) >= self.refs.len() {
            self.n_no_coor += 1;
            return;
        }
        let r = &mut self.refs[ref_id as usize];
        if unmapped_flag {
            // Placed-but-unmapped reads (mate's tid + pos) still get indexed
            // under their assigned bin so that "fetch this region" recovers
            // them, but they don't update the linear index range or
            // mapped-count metadata.
            r.n_unmapped += 1;
        } else {
            r.n_mapped += 1;
            if r.mapped_first.is_none() {
                r.mapped_first = Some(uoffset_start);
            }
            r.mapped_last_end = uoffset_end;
        }

        let bin = record.bin() as u32;
        let entry = r.bins.entry(bin).or_insert_with(BinEntry::new);
        push_chunk(&mut entry.chunks, uoffset_start, uoffset_end);
        if uoffset_start < entry.loffset {
            entry.loffset = uoffset_start;
        }

        // Update linear index over windows overlapped by the record.
        let pos = record.pos().max(0);
        let end_pos = pos + record_reference_span(record).max(1) as i32;
        let first_w = (pos >> LINEAR_WINDOW_SHIFT) as usize;
        let last_w = ((end_pos - 1) >> LINEAR_WINDOW_SHIFT) as usize;
        if r.linear.len() <= last_w {
            r.linear.resize(last_w + 1, u64::MAX);
        }
        for w in first_w..=last_w {
            if r.linear[w] == u64::MAX {
                r.linear[w] = uoffset_start;
            }
        }
    }

    /// Serialize the BAI to `out`. `block_offsets[i]` must give the
    /// compressed file offset of the i-th BGZF block of the indexed BAM.
    pub fn write<W: Write>(&self, out: &mut W, block_offsets: &[u64]) -> io::Result<()> {
        out.write_all(b"BAI\x01")?;
        write_i32_le(out, self.refs.len() as i32)?;
        for r in &self.refs {
            // Counts include the metadata bin if we have any mapped/unmapped.
            let has_meta = r.n_mapped + r.n_unmapped > 0;
            let n_bin = r.bins.len() + if has_meta { 1 } else { 0 };
            write_i32_le(out, n_bin as i32)?;
            for (&bin_id, entry) in &r.bins {
                write_u32_le(out, bin_id)?;
                write_i32_le(out, entry.chunks.len() as i32)?;
                for c in &entry.chunks {
                    let beg_voff = uoffset_to_voffset(c.beg, block_offsets);
                    let end_voff = uoffset_to_voffset(c.end, block_offsets);
                    write_u64_le(out, beg_voff)?;
                    write_u64_le(out, end_voff)?;
                }
            }
            if has_meta {
                write_u32_le(out, BIN_META)?;
                write_i32_le(out, 2)?; // metadata bin always has 2 chunks
                let beg = r.mapped_first.unwrap_or(0);
                let end = r.mapped_last_end;
                let beg_v = uoffset_to_voffset(beg, block_offsets);
                let end_v = uoffset_to_voffset(end, block_offsets);
                write_u64_le(out, beg_v)?;
                write_u64_le(out, end_v)?;
                // Second "chunk" is repurposed: (n_mapped, n_unmapped).
                write_u64_le(out, r.n_mapped)?;
                write_u64_le(out, r.n_unmapped)?;
            }
            // Linear index — fill empty windows with the previous non-empty
            // entry (htslib's `hts_idx_finish_loop`); makes "find virtual
            // offset for this window" work without extra interpolation logic
            // on the reader side.
            write_i32_le(out, r.linear.len() as i32)?;
            let mut last: u64 = 0;
            for &raw in &r.linear {
                let u = if raw == u64::MAX { last } else { raw };
                last = u;
                let v = uoffset_to_voffset(u, block_offsets);
                write_u64_le(out, v)?;
            }
        }
        // n_no_coor — htslib writes it whenever > 0.
        if self.n_no_coor > 0 {
            write_u64_le(out, self.n_no_coor)?;
        }
        Ok(())
    }

    /// Serialize the CSI to `out`. Uses the BAI-equivalent bin scheme
    /// (`min_shift = 14`, `depth = 5`) so the bin numbering is identical
    /// — this means the same data we built for BAI can be re-emitted as
    /// CSI without rebuilding. (Bigger contigs would need a deeper
    /// bin tree, which we can configure later if needed.)
    pub fn write_csi<W: Write>(&self, out: &mut W, block_offsets: &[u64]) -> io::Result<()> {
        let min_shift: i32 = LINEAR_WINDOW_SHIFT;
        let depth: i32 = 5;

        out.write_all(b"CSI\x01")?;
        write_i32_le(out, min_shift)?;
        write_i32_le(out, depth)?;
        write_i32_le(out, 0)?; // l_aux = 0; no aux data
        write_i32_le(out, self.refs.len() as i32)?;
        for r in &self.refs {
            let has_meta = r.n_mapped + r.n_unmapped > 0;
            let n_bin = r.bins.len() + if has_meta { 1 } else { 0 };
            write_i32_le(out, n_bin as i32)?;
            for (&bin_id, entry) in &r.bins {
                write_u32_le(out, bin_id)?;
                let loffset_v = uoffset_to_voffset(entry.loffset, block_offsets);
                write_u64_le(out, loffset_v)?;
                write_i32_le(out, entry.chunks.len() as i32)?;
                for c in &entry.chunks {
                    let beg_voff = uoffset_to_voffset(c.beg, block_offsets);
                    let end_voff = uoffset_to_voffset(c.end, block_offsets);
                    write_u64_le(out, beg_voff)?;
                    write_u64_le(out, end_voff)?;
                }
            }
            if has_meta {
                write_u32_le(out, BIN_META)?;
                // Metadata bin's loffset is conventionally 0 — htslib does
                // the same. It isn't used by region queries.
                write_u64_le(out, 0)?;
                write_i32_le(out, 2)?;
                let beg = r.mapped_first.unwrap_or(0);
                let end = r.mapped_last_end;
                let beg_v = uoffset_to_voffset(beg, block_offsets);
                let end_v = uoffset_to_voffset(end, block_offsets);
                write_u64_le(out, beg_v)?;
                write_u64_le(out, end_v)?;
                write_u64_le(out, r.n_mapped)?;
                write_u64_le(out, r.n_unmapped)?;
            }
        }
        if self.n_no_coor > 0 {
            write_u64_le(out, self.n_no_coor)?;
        }
        Ok(())
    }
}

/// Append `[start, end)` to `chunks`, merging into the previous entry if
/// they're contiguous (this is the on-the-fly version of htslib's
/// chunk-coalescing pass).
fn push_chunk(chunks: &mut Vec<Chunk>, start: u64, end: u64) {
    if let Some(last) = chunks.last_mut() {
        if last.end == start {
            last.end = end;
            return;
        }
    }
    chunks.push(Chunk { beg: start, end });
}

fn uoffset_to_voffset(uoffset: u64, block_offsets: &[u64]) -> u64 {
    let block_idx = (uoffset / BLOCK_SIZE as u64) as usize;
    let offset_in_block = uoffset % BLOCK_SIZE as u64;
    // Defensive bound: if uoffset points exactly at the start of a
    // block past the last emitted block, fall back to the EOF location.
    let coffset = block_offsets
        .get(block_idx)
        .copied()
        .or_else(|| block_offsets.last().copied())
        .unwrap_or(0);
    (coffset << 16) | offset_in_block
}

/// Reference span = sum of CIGAR ops that consume reference: M=0, D=2,
/// N=3, =(equal)=7, X=8.
fn record_reference_span(r: &Record) -> u32 {
    let cigar = r.cigar_raw();
    let mut span: u32 = 0;
    for op in cigar.chunks_exact(4) {
        let val = u32::from_le_bytes([op[0], op[1], op[2], op[3]]);
        let op_len = val >> 4;
        let op_type = val & 0xf;
        if matches!(op_type, 0 | 2 | 3 | 7 | 8) {
            span = span.saturating_add(op_len);
        }
    }
    span
}

fn write_i32_le<W: Write>(w: &mut W, v: i32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn write_u32_le<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn write_u64_le<W: Write>(w: &mut W, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::samtools_rs::bam::RefInfo;

    fn header_with(n: usize) -> Header {
        let mut text = Vec::new();
        text.extend_from_slice(b"@HD\tVN:1.6\tSO:coordinate\n");
        let mut refs = Vec::new();
        for i in 0..n {
            let name = format!("chr{}", i + 1);
            text.extend_from_slice(format!("@SQ\tSN:{}\tLN:1000\n", name).as_bytes());
            refs.push(RefInfo {
                name: name.into_bytes(),
                length: 1_000_000,
            });
        }
        Header { text, refs }
    }

    #[test]
    fn record_reference_span_basic() {
        // Build a record with cigar "10M2D5M" → ref span = 10 + 2 + 5 = 17.
        let mut data = vec![0u8; 32 + 1 + 12]; // core + 1-byte read_name + 3 cigar ops
        data[0..4].copy_from_slice(&0i32.to_le_bytes());
        data[8] = 1;
        data[12..14].copy_from_slice(&3u16.to_le_bytes()); // n_cigar_op
        data[16..20].copy_from_slice(&0i32.to_le_bytes()); // l_seq
        // CIGAR ops: (10, M=0), (2, D=2), (5, M=0)
        let ops: [u32; 3] = [(10 << 4) | 0, (2 << 4) | 2, (5 << 4) | 0];
        for (i, &op) in ops.iter().enumerate() {
            data[33 + i * 4..33 + (i + 1) * 4].copy_from_slice(&op.to_le_bytes());
        }
        let r = Record { data };
        assert_eq!(record_reference_span(&r), 17);
    }

    #[test]
    fn empty_index_writes_minimal_bytes() {
        let h = header_with(2);
        let bai = BaiBuilder::new(&h);
        let mut out = Vec::new();
        bai.write(&mut out, &[0]).unwrap();
        // Magic + n_ref(2) + per-ref [n_distinct_bin=0, n_intv=0]; no n_no_coor.
        assert_eq!(&out[..4], b"BAI\x01");
        assert_eq!(
            i32::from_le_bytes(out[4..8].try_into().unwrap()),
            2,
            "n_ref"
        );
        // Then for each ref: n_distinct_bin=0 (4) + n_intv=0 (4) = 8 bytes.
        // Total = 4 (magic) + 4 (n_ref) + 2 * 8 = 24.
        assert_eq!(out.len(), 24);
    }
}
