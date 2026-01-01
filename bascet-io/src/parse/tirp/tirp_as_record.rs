use bascet_core::*;

use crate::tirp;

impl Parse<ArenaSlice<u8>> for crate::Tirp {
    type Item = tirp::Record;

    fn parse_aligned(&mut self, decoded: &ArenaSlice<u8>) -> ParseStatus<Self::Item, ()> {
        let cursor = self.inner_cursor;
        // SAFETY: cursor is maintained internally and always valid
        let buf_cursor = unsafe { decoded.as_slice().get_unchecked(cursor..) };

        let pos_endof_record = match memchr::memchr(b'\n', buf_cursor) {
            Some(pos) => pos,
            None => {
                // NOTE: encountering a partial record indicates either end of block
                //       or a malformed file. Here we assume it is end of block
                //       if parse_spanning cannot build a complete record, however it is
                //       very likely a malformed file
                return ParseStatus::Partial;
            }
        };
        let mut iter_tab = memchr::memchr_iter(b'\t', buf_cursor);
        let pos_tab = match (
            iter_tab.next(),
            iter_tab.next(),
            iter_tab.next(),
            iter_tab.next(),
            iter_tab.next(),
            iter_tab.next(),
            iter_tab.next(),
        ) {
            (Some(p0), Some(p1), Some(p2), Some(p3), Some(p4), Some(p5), Some(p6)) => {
                [p0, p1, p2, p3, p4, p5, p6]
            }
            (_, _, _, _, _, _, _) => {
                // NOTE: encountering a partial record indicates either end of block
                //       or a malformed file. Here we assume it is end of block
                //       if parse_spanning cannot build a complete record, however it is
                //       very likely a malformed file
                return ParseStatus::Partial;
            }
        };

        self.inner_cursor = self
            .inner_cursor
            .checked_add(pos_endof_record + 1)
            .expect("overflow");

        // SAFETY: pos_endof_record was found by memchr in buf_cursor
        let buf_record = unsafe { buf_cursor.get_unchecked(..pos_endof_record) };
        let tirp_record =
            unsafe { crate::tirp::Record::from_raw(buf_record, pos_tab, decoded.clone_view()) };
        ParseStatus::Full(tirp_record)
    }

    fn parse_finish(&mut self) -> ParseStatus<Self::Item, ()> {
        ParseStatus::Finished
    }

    #[inline(always)]
    fn parse_spanning(
        &mut self,
        decoded_spanning_tail: &ArenaSlice<u8>,
        decoded_spanning_head: &ArenaSlice<u8>,
        mut alloc: impl FnMut(usize) -> ArenaSlice<u8>,
    ) -> ParseStatus<Self::Item, ()> {
        let slice_tail = decoded_spanning_tail.as_slice();
        let slice_head = decoded_spanning_head.as_slice();
        // NOTE: as_ptr_range is [start, end) and [start', end') => end == start'
        let is_contiguous = slice_tail.as_ptr_range().end == slice_head.as_ptr_range().start;

        // SAFETY: inner_cursor is maintained internally and always valid
        let tail_remaining = unsafe { slice_tail.get_unchecked(self.inner_cursor..) };
        let tail_len = tail_remaining.len();

        // Find newline marking end of record (only in head - if in tail, record would be complete)
        let pos_newline_head = memchr::memchr(b'\n', slice_head);
        let head_len = match pos_newline_head {
            Some(pos) => pos,
            None => return ParseStatus::Error(()),
        };
        let mut iter_tail = memchr::memchr_iter(b'\t', tail_remaining);
        let mut iter_head = memchr::memchr_iter(b'\t', slice_head);

        // Build array of 7 tab positions spanning tail and head
        let pos_tab_combined = match (
            iter_tail.next(),
            iter_tail.next(),
            iter_tail.next(),
            iter_tail.next(),
            iter_tail.next(),
            iter_tail.next(),
            iter_tail.next(),
        ) {
            (Some(t0), Some(t1), Some(t2), Some(t3), Some(t4), Some(t5), Some(t6)) => {
                [t0, t1, t2, t3, t4, t5, t6]
            }
            (Some(t0), Some(t1), Some(t2), Some(t3), Some(t4), Some(t5), None) => {
                match iter_head.next() {
                    Some(h6) => [t0, t1, t2, t3, t4, t5, tail_len + h6],
                    _ => return ParseStatus::Error(()),
                }
            }
            (Some(t0), Some(t1), Some(t2), Some(t3), Some(t4), None, None) => {
                match (iter_head.next(), iter_head.next()) {
                    (Some(h5), Some(h6)) => [t0, t1, t2, t3, t4, tail_len + h5, tail_len + h6],
                    _ => return ParseStatus::Error(()),
                }
            }
            (Some(t0), Some(t1), Some(t2), Some(t3), None, None, None) => {
                match (iter_head.next(), iter_head.next(), iter_head.next()) {
                    (Some(h4), Some(h5), Some(h6)) => {
                        [t0, t1, t2, t3, tail_len + h4, tail_len + h5, tail_len + h6]
                    }
                    _ => return ParseStatus::Error(()),
                }
            }
            (Some(t0), Some(t1), Some(t2), None, None, None, None) => {
                match (
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                ) {
                    (Some(h3), Some(h4), Some(h5), Some(h6)) => [
                        t0,
                        t1,
                        t2,
                        tail_len + h3,
                        tail_len + h4,
                        tail_len + h5,
                        tail_len + h6,
                    ],
                    _ => return ParseStatus::Error(()),
                }
            }
            (Some(t0), Some(t1), None, None, None, None, None) => {
                match (
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                ) {
                    (Some(h2), Some(h3), Some(h4), Some(h5), Some(h6)) => [
                        t0,
                        t1,
                        tail_len + h2,
                        tail_len + h3,
                        tail_len + h4,
                        tail_len + h5,
                        tail_len + h6,
                    ],
                    _ => return ParseStatus::Error(()),
                }
            }
            (Some(t0), None, None, None, None, None, None) => {
                match (
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                ) {
                    (Some(h1), Some(h2), Some(h3), Some(h4), Some(h5), Some(h6)) => [
                        t0,
                        tail_len + h1,
                        tail_len + h2,
                        tail_len + h3,
                        tail_len + h4,
                        tail_len + h5,
                        tail_len + h6,
                    ],
                    _ => return ParseStatus::Error(()),
                }
            }
            (None, None, None, None, None, None, None) => {
                match (
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                    iter_head.next(),
                ) {
                    (Some(h0), Some(h1), Some(h2), Some(h3), Some(h4), Some(h5), Some(h6)) => [
                        tail_len + h0,
                        tail_len + h1,
                        tail_len + h2,
                        tail_len + h3,
                        tail_len + h4,
                        tail_len + h5,
                        tail_len + h6,
                    ],
                    _ => return ParseStatus::Error(()),
                }
            }
            _ => unreachable!(),
        };

        let tirp_record = if is_contiguous {
            // Create view spanning both buffers
            let combined_slice =
                unsafe { std::slice::from_raw_parts(tail_remaining.as_ptr(), tail_len + head_len) };
            let mut record = unsafe {
                tirp::Record::from_raw(
                    combined_slice,
                    pos_tab_combined,
                    decoded_spanning_tail.clone_view(),
                )
            };
            // Add head arena view => both arenas must be kept alive
            record
                .arena_backing
                .push(decoded_spanning_head.clone_view());
            record
        } else {
            // Allocate scratch and copy
            let mut scratch = alloc(tail_len + head_len);
            let scratch_slice = scratch.as_mut_slice();

            unsafe {
                std::ptr::copy_nonoverlapping(
                    tail_remaining.as_ptr(),
                    scratch_slice.as_mut_ptr(),
                    tail_len,
                );
                // SAFETY: head_len was calculated from memchr results in slice_head
                std::ptr::copy_nonoverlapping(
                    slice_head.get_unchecked(..head_len).as_ptr(),
                    scratch_slice.as_mut_ptr().add(tail_len),
                    head_len,
                );
            }

            unsafe {
                tirp::Record::from_raw(scratch.as_slice(), pos_tab_combined, scratch.clone_view())
            }
        };

        self.inner_cursor = head_len;
        ParseStatus::Full(tirp_record)
    }
}
