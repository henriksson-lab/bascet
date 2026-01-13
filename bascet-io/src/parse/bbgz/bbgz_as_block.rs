use std::ptr::slice_from_raw_parts_mut;

use bascet_core::{ArenaSlice, ArenaView, Parse, ParseResult};
use memchr::memmem;
use smallvec::{smallvec, SmallVec};

use crate::{
    codec::bbgz::{usize_MIN_SIZEOF_HEADER, MARKER_EOF},
    parse::bbgz::{BBGZParser, Block},
    BBGZExtra, BBGZHeader, BBGZTrailer, BGZFExtra,
};

impl Parse<ArenaSlice<u8>> for BBGZParser {
    type Item = Block;

    fn parse_aligned(&mut self, decoded: &ArenaSlice<u8>) -> ParseResult<Self::Item, ()> {
        let buf_remaining = &decoded.as_slice()[self.inner_cursor..];
        let buf_remaining_len = buf_remaining.len();

        if buf_remaining_len < usize_MIN_SIZEOF_HEADER {
            return ParseResult::Partial;
        }

        // SAFETY: checked size above
        if unsafe {
            buf_remaining.get_unchecked(0) != &BBGZHeader::ID1_MAGIC ||  // cargo fmt stop unaligning these!
            buf_remaining.get_unchecked(1) != &BBGZHeader::ID2_MAGIC
        } {
            panic!(
                "Magic bytes not found, found instead: {:?}",
                [buf_remaining.get(0), buf_remaining.get(1)]
            );
            return ParseResult::Error(());
        }

        // Bytes 0-9: ID1, ID2, CM, FLG, MTIME(4), XFL, OS
        // Bytes 10-11: XLEN
        // SAFETY: checked minimum size above, cursor + 11 is within bounds
        let xlen = unsafe {
            u16::from_le_bytes([
                *buf_remaining.get_unchecked(10),
                *buf_remaining.get_unchecked(11),
            ]) as usize
        };

        // Parse extra fields (start at last static-sized byte (End of static header), continue for XLEN bytes)
        let mut cursor_fextra = BBGZHeader::SSIZE;
        let pos_end_fextra = cursor_fextra + xlen;

        if pos_end_fextra > buf_remaining_len {
            return ParseResult::Partial;
        }

        let mut slice_bc: &[u8] = &[];
        let mut slice_id: &[u8] = &[];

        while cursor_fextra < pos_end_fextra {
            // SAFETY: extra_cursor is bounded by extra_end which is derived from xlen in the header
            let (si1, si2, len) = unsafe {
                (
                    *buf_remaining.get_unchecked(cursor_fextra),
                    *buf_remaining.get_unchecked(cursor_fextra + 1),
                    u16::from_le_bytes([
                        *buf_remaining.get_unchecked(cursor_fextra + 2),
                        *buf_remaining.get_unchecked(cursor_fextra + 3),
                    ]) as usize,
                )
            };

            // DATA starts at offset + static size of an extra field, has length 'len'
            let pos_begin_data = cursor_fextra + BBGZExtra::SSIZE;
            let pos_end_data = pos_begin_data + len;
            // SAFETY: data_end is bounded by extra_end which was checked above
            let data = unsafe { buf_remaining.get_unchecked(pos_begin_data..pos_end_data) };

            match (si1, si2) {
                (b'B', b'C') => {
                    slice_bc = data;
                }
                (b'I', b'D') => {
                    slice_id = data;
                }
                // NOTE: ignore other subfields
                _ => {}
            }

            cursor_fextra = pos_end_data;
        }

        // Both BC and ID subfields must exist
        // If they're missing, check if this is the EOF marker
        if slice_bc.is_empty() || slice_id.is_empty() {
            if buf_remaining.starts_with(MARKER_EOF) {
                return ParseResult::Partial;
            }
            panic!("Missing BC/ID subfield in header (not EOF marker)");
            return ParseResult::Error(());
        }

        // SAFETY: BC subfield is guaranteed to have at least 2 bytes (BSIZE is u16)
        let bsize = unsafe {
            u16::from_le_bytes([*slice_bc.get_unchecked(0), *slice_bc.get_unchecked(1)]) as usize
                + 1
        };
        let pos_end_block = bsize;
        if pos_end_block > buf_remaining_len {
            return ParseResult::Partial;
        }

        // SAFETY: block_end bounds checked above
        let slice_header = unsafe { buf_remaining.get_unchecked(..cursor_fextra) };
        let slice_raw = unsafe {
            buf_remaining.get_unchecked(cursor_fextra..(pos_end_block - BBGZTrailer::SSIZE))
        };
        let slice_trailer = unsafe {
            buf_remaining.get_unchecked((pos_end_block - BBGZTrailer::SSIZE)..pos_end_block)
        };
        self.inner_cursor += bsize;

        let block = Block {
            id: unsafe { std::mem::transmute(slice_id) },
            header: unsafe { std::mem::transmute(slice_header) },
            raw: unsafe { std::mem::transmute(slice_raw) },
            trailer: unsafe { std::mem::transmute(slice_trailer) },
            arena_backing: smallvec![decoded.clone_view()],
        };

        ParseResult::Full(block)
    }

    fn parse_spanning<FA>(
        &mut self,
        decoded_spanning_tail: &ArenaSlice<u8>,
        decoded_spanning_head: &ArenaSlice<u8>,
        mut alloc: FA,
    ) -> ParseResult<Self::Item, ()>
    where
        FA: FnMut(usize) -> ArenaSlice<u8>,
    {
        let mut arena_backings: SmallVec<[ArenaSlice<u8>; 2]> = SmallVec::new();
        let slice_tail = decoded_spanning_tail.as_slice();
        let slice_head = decoded_spanning_head.as_slice();
        // NOTE: as_ptr_range is [start, end) => end == start'
        let is_contiguous = slice_tail.as_ptr_range().end == slice_head.as_ptr_range().start;

        // SAFETY: inner_cursor is maintained internally and always valid
        let tail_remaining = unsafe { slice_tail.get_unchecked(self.inner_cursor..) };
        let tail_len = tail_remaining.len();

        let head_len = slice_head.len();

        let slice_combined = if is_contiguous {
            arena_backings.push(decoded_spanning_tail.clone());
            arena_backings.push(decoded_spanning_head.clone());

            unsafe { std::slice::from_raw_parts(tail_remaining.as_ptr(), tail_len + head_len) }
        } else {
            let mut scratch = alloc(tail_len + head_len);
            {
                let scratch_slice = scratch.as_mut_slice();

                unsafe {
                    std::ptr::copy_nonoverlapping(
                        tail_remaining.as_ptr(),
                        scratch_slice.as_mut_ptr(),
                        tail_len,
                    );
                    std::ptr::copy_nonoverlapping(
                        slice_head.as_ptr(),
                        scratch_slice.as_mut_ptr().add(tail_len),
                        head_len,
                    );
                }
            }

            arena_backings.push(scratch);
            unsafe { arena_backings.last().unwrap_unchecked().as_slice() }
        };
        let slice_combined_len = slice_combined.len();

        if slice_combined_len < usize_MIN_SIZEOF_HEADER {
            panic!("Spanning block too small");
            return ParseResult::Error(());
        }

        // SAFETY: checked size in parse_spanning
        if unsafe {
            slice_combined.get_unchecked(0) != &BBGZHeader::ID1_MAGIC || // cargo fmt stop unaligning these!
            slice_combined.get_unchecked(1) != &BBGZHeader::ID2_MAGIC
        } {
            panic!("Magic bytes not found");
            return ParseResult::Error(());
        }

        // Bytes 0-9: ID1, ID2, CM, FLG, MTIME(4), XFL, OS
        // Bytes 10-11: XLEN
        // SAFETY: checked size in parse_spanning, cursor + 11 is within bounds
        let xlen = unsafe {
            u16::from_le_bytes([
                *slice_combined.get_unchecked(10),
                *slice_combined.get_unchecked(11),
            ]) as usize
        };

        // Parse extra fields (start at last static-sized byte (End of static header), continue for XLEN bytes)
        // slice_combined starts at 0 => do not use cursor
        let mut cursor_fextra = BBGZHeader::SSIZE;
        let pos_end_fextra = cursor_fextra + xlen;

        if pos_end_fextra > slice_combined_len {
            panic!("Spanning block too small");
            return ParseResult::Error(());
        }

        let mut slice_bc: &[u8] = &[];
        let mut slice_id: &[u8] = &[];

        while cursor_fextra < pos_end_fextra {
            // SAFETY: extra_cursor is bounded by extra_end which is derived from xlen in the header
            let (si1, si2, len) = unsafe {
                (
                    *slice_combined.get_unchecked(cursor_fextra),
                    *slice_combined.get_unchecked(cursor_fextra + 1),
                    u16::from_le_bytes([
                        *slice_combined.get_unchecked(cursor_fextra + 2),
                        *slice_combined.get_unchecked(cursor_fextra + 3),
                    ]) as usize,
                )
            };

            // DATA starts at offset + static size of an extra field, has length 'len'
            let pos_begin_data = cursor_fextra + BBGZExtra::SSIZE;
            let pos_end_data = pos_begin_data + len;
            // SAFETY: data_end is bounded by extra_end which was checked above
            let data = unsafe { slice_combined.get_unchecked(pos_begin_data..pos_end_data) };

            match (si1, si2) {
                (b'B', b'C') => {
                    slice_bc = data;
                }
                (b'I', b'D') => {
                    slice_id = data;
                }
                _ => {}
            }

            cursor_fextra = pos_end_data;
        }

        // Both BC and ID subfields must exist
        // If they're missing, check if this is the EOF marker
        if slice_bc.is_empty() || slice_id.is_empty() {
            if slice_combined.starts_with(MARKER_EOF) {
                return ParseResult::Partial;
            }
            panic!("Missing BC/ID subfield in header (not EOF marker)");
            return ParseResult::Error(());
        }

        // SAFETY: BC subfield is guaranteed to have at least 2 bytes (BSIZE is u16)
        let bsize = unsafe {
            u16::from_le_bytes([*slice_bc.get_unchecked(0), *slice_bc.get_unchecked(1)]) as usize
                + 1
        };
        let pos_end_block = bsize;
        if pos_end_block > slice_combined_len {
            panic!("Combined block too small");
            return ParseResult::Error(());
        }
        // SAFETY: block_end bounds checked above
        let slice_header = unsafe { slice_combined.get_unchecked(..cursor_fextra) };
        let slice_raw = unsafe {
            slice_combined.get_unchecked(cursor_fextra..(pos_end_block - BBGZTrailer::SSIZE))
        };
        let slice_trailer = unsafe {
            slice_combined.get_unchecked((pos_end_block - BBGZTrailer::SSIZE)..pos_end_block)
        };
        self.inner_cursor = pos_end_block.saturating_sub(tail_len);

        let block = Block {
            id: unsafe { std::mem::transmute(slice_id) },
            header: unsafe { std::mem::transmute(slice_header) },
            raw: unsafe { std::mem::transmute(slice_raw) },
            trailer: unsafe { std::mem::transmute(slice_trailer) },
            arena_backing: arena_backings.iter().map(|b| b.clone_view()).collect(),
        };

        ParseResult::Full(block)
    }

    fn parse_finish(&mut self) -> bascet_core::ParseResult<Self::Item, ()> {
        ParseResult::Finished
    }
}
