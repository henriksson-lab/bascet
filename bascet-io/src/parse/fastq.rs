use std::{ptr::NonNull, sync::atomic::AtomicU64};

use bascet_core::*;

const U8_CHAR_NEWLINE: u8 = b'\n';
const U8_CHAR_RECORD: u8 = b'@';

struct FASTQRecordParser {
    inner_cursor_ptr: UnsafePtr<u8>,

    inner_ptr_partial_src: UnsafePtr<Arena<u8>>,
    inner_ptr_partial_tail: UnsafePtr<u8>,
    inner_len_partial_tail: usize,

    inner_current_id: &'static [u8],
    inner_current_reads: Vec<&'static [u8]>,
    inner_current_qualities: Vec<&'static [u8]>,
}

// record ID, sequence (unpaired), qualities (unpaired)
struct FASTQRecord {
    id: &'static [u8],
    seq: &'static [u8],
    qal: &'static [u8],
}
impl FASTQRecordParser {
    unsafe fn record_from_raw(from: *const u8, line_positions: [usize; 4]) -> FASTQRecord {
        let hdr = std::slice::from_raw_parts(from, line_positions[0]);
        let seq = std::slice::from_raw_parts(
            from.add(line_positions[0] + 1),
            line_positions[1] - line_positions[0] - 1,
        );
        let sep = std::slice::from_raw_parts(
            from.add(line_positions[1] + 1),
            line_positions[2] - line_positions[1] - 1,
        );
        let qal = std::slice::from_raw_parts(
            from.add(line_positions[2] + 1),
            line_positions[3] - line_positions[2] - 1,
        );

        if hdr.is_empty() || hdr[0] != U8_CHAR_RECORD {
            panic!()
        }

        if sep.is_empty() || sep[0] != U8_CHAR_RECORD {
            panic!()
        }

        if seq.len() != qal.len() {
            panic!()
        }

        // SAFETY: transmute slices to static lifetime kept alive by ref counter
        FASTQRecord {
            id: std::mem::transmute(hdr),
            seq: std::mem::transmute(seq),
            qal: std::mem::transmute(qal),
        }
    }
}

impl Parse<ArenaSlice<'static, u8>> for FASTQRecordParser {
    fn parse<C, A>(&mut self, block: ArenaSlice<'static, u8>) -> bascet_core::ParseStatus<C, ()>
    where
        C: bascet_core::Composite,
        C: Default,
    {
        let len_partial_tail = self.inner_len_partial_tail;
        let (ptr_cursor, buf_block) = unsafe {
            let end_partial = self.inner_ptr_partial_tail.add(len_partial_tail);
            let ptr_cursor: *mut u8;
            let buf_block: &'static [u8];
            if len_partial_tail > 0 {
                if end_partial == *self.inner_cursor_ptr {
                    // NOTE: true => memory access is contigous
                    ptr_cursor = (*self.inner_ptr_partial_tail).as_ptr();
                    buf_block = std::slice::from_raw_parts(
                        ptr_cursor,
                        len_partial_tail + block.inner.len(),
                    );
                } else {
                    // NOTE: false => memory access is NOT contigous. memcpy required.
                    // SAFETY: block MUST still be alive at this point. This can be guaranteed by using an owned type
                    //         or by refcounts.
                    buf_block = block.inner;
                    let mut newline_iter = memchr::memchr_iter(U8_CHAR_NEWLINE, buf_block);
                    let line_positions = match (
                        newline_iter.next(),
                        newline_iter.next(),
                        newline_iter.next(),
                        newline_iter.next(),
                    ) {
                        (Some(n1), Some(n2), Some(n3), Some(n4)) => [n1, n2, n3, n4],
                        (_, _, _, _) => {
                            panic!();
                            return ParseStatus::Error(());
                        }
                    };

                    let ptr_partial_tail = (*self.inner_ptr_partial_tail).as_ptr();
                    let len_partial_tail = self.inner_len_partial_tail;
                    let mut buf_partial_complete =
                        std::slice::from_raw_parts_mut(ptr_partial_tail, len_partial_tail).to_vec();
                    buf_partial_complete.extend_from_slice(&buf_block[..line_positions[3]]);

                    let mut newline_iter =
                        memchr::memchr_iter(U8_CHAR_NEWLINE, &buf_partial_complete);
                    let line_positions = match (
                        newline_iter.next(),
                        newline_iter.next(),
                        newline_iter.next(),
                        newline_iter.next(),
                    ) {
                        (Some(n1), Some(n2), Some(n3), Some(n4)) => [n1, n2, n3, n4],
                        (_, _, _, _) => {
                            panic!();
                            return ParseStatus::Error(());
                        }
                    };
                    
                    let len_consumed = line_positions[3].min(len_partial_tail);
                    self.inner_len_partial_tail -= len_consumed;
                    if self.inner_len_partial_tail > 0 {
                        UnsafePtr::new_unchecked((*self.inner_ptr_partial_tail).as_ptr().add(len_consumed));
                    } else {
                        // SAFETY: relase old block ref to allow discarding
                        let ptr_partial_src = (*self.inner_ptr_partial_src).as_ptr();
                        (*ptr_partial_src).dec_ref();
                    }

                    let record = FASTQRecordParser::record_from_raw(
                        buf_partial_complete.as_ptr(),
                        line_positions,
                    );

                    self.inner_current_id = record.id;
                    self.inner_current_reads.push(record.seq);
                    self.inner_current_qualities.push(record.qal);
                    // let result = C::default();
                    // apply_selected!(A, result, {
                    //     Id => record.id,
                    //     Read => record.seq,
                    //     Quality => record.qal,
                    // });
                };
            }
            (ptr_cursor, buf_block)
        };

        let mut newline_iter = unsafe {
            let len_remaining = buf_block.as_ptr_range().end.offset_from(ptr_cursor) as usize;
            let buf_remaining = std::slice::from_raw_parts(ptr_cursor, len_remaining);

            memchr::memchr_iter(U8_CHAR_NEWLINE, buf_remaining)
        };

        let line_positions = match (
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
        ) {
            (Some(n1), Some(n2), Some(n3), Some(n4)) => [n1, n2, n3, n4],
            (_, _, _, _) => unsafe {
                self.inner_ptr_partial_src = block.inner_ptr_src;
                // SAFETY: keep old block alive until it can safely be discarded
                let ptr_partial_src = (*self.inner_ptr_partial_src).as_ptr();
                (*ptr_partial_src).inc_ref();

                self.inner_len_partial_tail = len_remaining;
                self.inner_ptr_partial_tail =
                    UnsafePtr::new_unchecked(buf_remaining.as_mut_ptr_range().start);
                return ParseStatus::Partial;
            },
        };

        let len_consumed = line_positions[3].min(len_partial_tail);
        unsafe {
            self.inner_len_partial_tail -= len_consumed;
            if self.inner_len_partial_tail > 0 {
                UnsafePtr::new_unchecked((*self.inner_ptr_partial_tail).as_ptr().add(len_consumed));
            } else {
                // SAFETY: relase old block ref to allow discarding
                let ptr_partial_src = (*self.inner_ptr_partial_src).as_ptr();
                (*ptr_partial_src).dec_ref();
            }
        }

        ParseStatus::Partial
    }

    fn parse_finish<C, A>(&mut self) -> bascet_core::ParseStatus<C, ()>
    where
        C: bascet_core::Composite,
    {
        todo!()
    }
}
