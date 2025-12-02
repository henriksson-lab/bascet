use bascet_core::*;
use smallvec::{smallvec, SmallVec};

#[derive(Composite, Default)]
#[bascet(
    attrs = (
        Id,
        SequencePair = vec_sequence_pairs,
        QualityPair = vec_quality_pairs,
        Umi = vec_umis
    ), 
    backing = ArenaBacking, 
    kind = AsCell
)]
pub struct TIRPCell {
    pub id: &'static [u8],
    pub vec_sequence_pairs: Vec<(&'static [u8], &'static [u8])>,
    pub vec_quality_pairs: Vec<(&'static [u8], &'static [u8])>,
    pub vec_umis: Vec<&'static [u8]>,

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub arena_backing: SmallVec<[ArenaView<u8>; 2]>,
}

impl<K> Parse<ArenaSlice<u8>, K> for crate::TIRP<K>
where
    K: crate::tirp::TIRPMarker,
{
    type Item = K::Item;

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
        let record = unsafe { TIRPCell::from_raw(buf_record, pos_tab, decoded.clone_view()) };
        let mut composite_record = C::default();
        composite_record.from_parsed(&record);
        composite_record.take_backing(record);
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

    #[inline(always)]
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

        // Find newline marking end of record (only in head - if in tail, record would be complete)
        let pos_newline_head = memchr::memchr(b'\n', slice_head);
        let head_len = match pos_newline_head {
            Some(pos) => pos + 1,
            None => return ParseStatus::Error(()),
        };

        // Collect tabs from tail and head
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
                TIRPCell::from_raw(
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
                TIRPCell::from_raw(scratch.as_slice(), pos_tab_combined, scratch.clone_view())
            }
        };
        let mut composite_record = C::default();
        composite_record.from_parsed(&tirp_record);
        composite_record.take_backing(tirp_record);

        self.inner_cursor = head_len;
        ParseStatus::Full(composite_record)
    }
}

impl TIRPCell {
    unsafe fn from_raw(
        buf_record: &[u8],
        pos_tab: [usize; 7],
        arena_view: ArenaView<u8>,
    ) -> crate::tirp_as_record::TIRPRecord {
        // SAFETY: Caller guarantees pos_newline indices are valid
        let id = buf_record.get_unchecked(..pos_tab[0]);
        let r1 = buf_record.get_unchecked(pos_tab[2] + 1..pos_tab[3]);
        let r2 = buf_record.get_unchecked(pos_tab[3] + 1..pos_tab[4]);
        let q1 = buf_record.get_unchecked(pos_tab[4] + 1..pos_tab[5]);
        let q2 = buf_record.get_unchecked(pos_tab[5] + 1..pos_tab[6]);
        let umi = buf_record.get_unchecked(pos_tab[6] + 1..);

        if likely_unlikely::unlikely(r1.len() != q1.len()) {
            panic!("r1/q1 length mismatch: {:?} != {:?}", r1.len(), q1.len());
        }
        if likely_unlikely::unlikely(r2.len() != q2.len()) {
            panic!("r1/q1 length mismatch: {:?} != {:?}", r2.len(), q2.len());
        }

        // SAFETY: transmute slices to static lifetime kept alive by ArenaView refcount
        let static_id: &'static [u8] = unsafe { std::mem::transmute(id) };
        let static_r1: &'static [u8] = unsafe { std::mem::transmute(r1) };
        let static_r2: &'static [u8] = unsafe { std::mem::transmute(r2) };
        let static_q1: &'static [u8] = unsafe { std::mem::transmute(q1) };
        let static_q2: &'static [u8] = unsafe { std::mem::transmute(q2) };
        let static_umi: &'static [u8] = unsafe { std::mem::transmute(umi) };

        crate::tirp_as_record::TIRPRecord {
            id: static_id,
            sequence_pair: (static_r1, static_r2),
            quality_pair: (static_q1, static_q2),
            umi: static_umi,

            arena_backing: smallvec![arena_view],
        }
    }
}
