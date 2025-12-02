use bascet_core::*;
use smallvec::{smallvec, SmallVec};

#[derive(Composite, Default)]
#[bascet(attrs = (Id, Sequence, Quality), backing = ArenaBacking, kind = AsRecord)]
pub struct FASTQRecord {
    pub id: &'static [u8],
    pub sequence: &'static [u8],
    pub quality: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub arena_backing: SmallVec<[ArenaView<u8>; 2]>,
}

impl Parse<ArenaSlice<u8>, AsRecord> for crate::FASTQ {
    type Item = FASTQRecord;

    fn parse_aligned<C, A>(&mut self, decoded: &ArenaSlice<u8>) -> ParseStatus<C, ()>
    where
        C: bascet_core::Composite
            + Default
            + FromParsed<A, Self::Item>
            + FromBacking<Self::Item, <C as bascet_core::Composite>::Backing>,
    {
        let cursor = self.inner_cursor;
        // SAFETY: cursor is maintained internally and always valid
        let buf_cursor = unsafe { decoded.as_slice().get_unchecked(cursor..) };

        let mut iter_newline = memchr::memchr_iter(b'\n', buf_cursor);
        let pos_newline = match (
            iter_newline.next(),
            iter_newline.next(),
            iter_newline.next(),
            iter_newline.next(),
        ) {
            (Some(p0), Some(p1), Some(p2), Some(p3)) => [p0, p1, p2, p3],
            (_, _, _, _) => {
                // NOTE: encountering a partial record indicates either end of block
                //       or a malformed file. Here we assume it is end of block
                //       if parse_spanning cannot build a complete record, however it is
                //       very likely a malformed file
                return ParseStatus::Partial;
            }
        };

        self.inner_cursor = self
            .inner_cursor
            .checked_add(pos_newline[3] + 1)
            .expect("overflow");

        // SAFETY: pos_newline[3] was found by memchr in buf_cursor
        let buf_record = unsafe { buf_cursor.get_unchecked(..pos_newline[3]) };
        let fastq_record =
            unsafe { FASTQRecord::from_raw(buf_record, pos_newline, decoded.clone_view()) };
        let mut composite_record = C::default();
        composite_record.from_parsed(&fastq_record);
        composite_record.take_backing(fastq_record);
        ParseStatus::Full(composite_record)
    }

    fn parse_finish<C, A>(&mut self) -> ParseStatus<C, ()>
    where
        C: bascet_core::Composite
            + Default
            + FromParsed<A, Self::Item>
            + FromBacking<Self::Item, <C as bascet_core::Composite>::Backing>,
    {
        return ParseStatus::Finished;
    }

    fn parse_spanning<C, A>(
        &mut self,                              //
        decoded_spanning_tail: &ArenaSlice<u8>, //
        decoded_spanning_head: &ArenaSlice<u8>, //
        mut alloc: impl FnMut(usize) -> ArenaSlice<u8>,
    ) -> ParseStatus<C, ()>
    where
        C: bascet_core::Composite
            + Default
            + FromParsed<A, Self::Item>
            + FromBacking<Self::Item, <C as bascet_core::Composite>::Backing>,
    {
        let slice_tail = decoded_spanning_tail.as_slice();
        let slice_head = decoded_spanning_head.as_slice();
        // NOTE: as_ptr_range is [start, end) and [start', end') => end == start'
        let is_contiguous = slice_tail.as_ptr_range().end == slice_head.as_ptr_range().start;

        // SAFETY: inner_cursor is maintained internally and always valid
        let tail_remaining = unsafe { slice_tail.get_unchecked(self.inner_cursor..) };
        let tail_len = tail_remaining.len();

        let mut iter_tail = memchr::memchr_iter(b'\n', tail_remaining);
        let mut iter_head = memchr::memchr_iter(b'\n', slice_head);

        let (pos_newline_combined, head_len) =
            match (iter_tail.next(), iter_tail.next(), iter_tail.next()) {
                (Some(t0), Some(t1), Some(t2)) => match iter_head.next() {
                    Some(h3) => ([t0, t1, t2, tail_len + h3], h3 + 1),
                    _ => return ParseStatus::Error(()),
                },
                (Some(t0), Some(t1), None) => match (iter_head.next(), iter_head.next()) {
                    (Some(h2), Some(h3)) => ([t0, t1, tail_len + h2, tail_len + h3], h3 + 1),
                    _ => return ParseStatus::Error(()),
                },
                (Some(t0), None, None) => {
                    match (iter_head.next(), iter_head.next(), iter_head.next()) {
                        (Some(h1), Some(h2), Some(h3)) => {
                            ([t0, tail_len + h1, tail_len + h2, tail_len + h3], h3 + 1)
                        }
                        _ => return ParseStatus::Error(()),
                    }
                }
                (None, None, None) => {
                    match (
                        iter_head.next(),
                        iter_head.next(),
                        iter_head.next(),
                        iter_head.next(),
                    ) {
                        (Some(h0), Some(h1), Some(h2), Some(h3)) => (
                            [tail_len + h0, tail_len + h1, tail_len + h2, tail_len + h3],
                            h3 + 1,
                        ),
                        _ => return ParseStatus::Error(()),
                    }
                }
                _ => unreachable!(),
            };

        let fastq_record = if is_contiguous {
            // Create view spanning both buffers
            let combined_slice =
                unsafe { std::slice::from_raw_parts(tail_remaining.as_ptr(), tail_len + head_len) };
            let mut record = unsafe {
                FASTQRecord::from_raw(
                    combined_slice,
                    pos_newline_combined,
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
                FASTQRecord::from_raw(
                    scratch.as_slice(),
                    pos_newline_combined,
                    scratch.clone_view(),
                )
            }
        };
        let mut composite_record = C::default();
        composite_record.from_parsed(&fastq_record);
        composite_record.take_backing(fastq_record);

        self.inner_cursor = head_len;
        ParseStatus::Full(composite_record)
    }
}

impl FASTQRecord {
    unsafe fn from_raw(
        buf_record: &[u8],
        pos_newline: [usize; 4],
        arena_view: ArenaView<u8>,
    ) -> FASTQRecord {
        // SAFETY: Caller guarantees pos_newline indices are valid
        let hdr = buf_record.get_unchecked(..pos_newline[0]);
        let seq = buf_record.get_unchecked(pos_newline[0] + 1..pos_newline[1]);
        let sep = buf_record.get_unchecked(pos_newline[1] + 1..pos_newline[2]);
        let qal = buf_record.get_unchecked(pos_newline[2] + 1..pos_newline[3]);

        if likely_unlikely::unlikely(hdr.get(0) != Some(&b'@')) {
            panic!(
                "Invalid FASTQ header: {:?}; record {:?}",
                String::from_utf8_lossy(hdr),
                String::from_utf8_lossy(buf_record),
            );
        }
        if likely_unlikely::unlikely(sep.get(0) != Some(&b'+')) {
            panic!(
                "Invalid FASTQ separator: {:?}",
                String::from_utf8_lossy(sep)
            );
        }
        if likely_unlikely::unlikely(seq.len() != qal.len()) {
            panic!(
                "Sequence and quality length mismatch: {} != {}",
                seq.len(),
                qal.len()
            );
        }

        // SAFETY: transmute slices to static lifetime kept alive by ArenaView refcount
        let static_id: &'static [u8] = unsafe { std::mem::transmute(hdr) };
        let static_seq: &'static [u8] = unsafe { std::mem::transmute(seq) };
        let static_qal: &'static [u8] = unsafe { std::mem::transmute(qal) };

        FASTQRecord {
            id: static_id,
            sequence: static_seq,
            quality: static_qal,
            arena_backing: smallvec![arena_view],
        }
    }
}
