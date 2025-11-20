use std::ptr::NonNull;

use bascet_core::*;

const U8_CHAR_NEWLINE: u8 = b'\n';
const U8_CHAR_RECORD: u8 = b'@';
const U8_CHAR_SEP: u8 = b'+';

pub struct FASTQRecordParser {
    inner_ptr_partial_src: UnsafePtr<Arena<u8>>,
    inner_ptr_partial_tail: UnsafePtr<u8>,
    inner_len_partial_tail: usize,

    inner_off_cursor: usize,
    inner_last_block_ptr: *const u8,
}

#[derive(Composite)]
#[attrs(Id, Read, Quality, RefCount)]
pub struct FASTQRecord {
    id: &'static [u8],
    read: &'static [u8],
    quality: &'static [u8],

    ref_count: UnsafePtr<Arena<u8>>,
}

impl FASTQRecordParser {
    pub fn new() -> Self {
        FASTQRecordParser {
            inner_ptr_partial_src: unsafe { UnsafePtr::new_unchecked(NonNull::dangling().as_ptr()) },
            inner_ptr_partial_tail: unsafe { UnsafePtr::new_unchecked(NonNull::dangling().as_ptr()) },
            inner_len_partial_tail: 0,
            inner_off_cursor: 0,
            inner_last_block_ptr: std::ptr::null(),
        }
    }
}

impl Parse<ArenaSlice<'_, u8>> for FASTQRecordParser {
    type Output = FASTQRecord;

    fn parse<C, A>(&mut self, mut decoded: ArenaSlice<'_, u8>) -> ParseStatus<C, ()>
    where
        C: bascet_core::Composite + Default + ParseFrom<A, Self::Output> {
        // TODO:

        // pretend each record is 364 bytes
        const RECORD_SIZE: usize = 364;

        // Check if we've consumed the entire block
        if self.inner_off_cursor >= decoded.inner.len() {
            return ParseStatus::Partial;
        }

        // "Parse" one record
        self.inner_off_cursor += RECORD_SIZE;

        // Create a FASTQRecord with mock data
        // Increment ref count since we're creating a new reference to the arena
        unsafe { decoded.inner_ptr_src.as_mut().inc_ref() };
        let record = FASTQRecord {
            id: b"mock_id",
            read: b"ACGT",
            quality: b"IIII",
            ref_count: decoded.inner_ptr_src,
        };

        // Create cell and populate it from the record
        let mut cell = C::default();
        cell.from(&record);

        ParseStatus::Full(cell)
    }

    fn parse_finish<C, A>(&mut self) -> ParseStatus<C, ()>
    where
         C: bascet_core::Composite + Default + ParseFrom<A, Self::Output> {
        todo!()
    }

    fn parse_reset(&mut self) -> Result<(), ()> {
        self.inner_off_cursor = 0;
        Ok(())
    }
}