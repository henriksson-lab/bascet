use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::atomic::Ordering;

use crate::common::{PageBuffer, UnsafeMutPtr};
use crate::io::format::fastq_gz;
use crate::io::{
    self,
    format::tirp_bgzf,
    traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream},
};
use crate::{common, log_warning};

pub struct Stream<T> {
    inner_decoder: MultiGzDecoder<BufReader<File>>,

    inner_buf_pool: common::PageBufferPool<u8, 512>,
    inner_buf_ptr: common::UnsafeMutPtr<PageBuffer<u8>>,
    inner_buf_slice: &'static [u8],
    inner_buf_cursor: usize,

    inner_buf_truncated_len: usize,
    inner_buf_incomplete_len: usize,
    buffer_num_pages: usize,
    buffer_page_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T>
where
    T: BascetCell + 'static,
    T::Builder: BascetCellBuilder<Token = T>,
{
    pub fn new(file: &io::format::fastq_gz::Input) -> Result<Self, crate::runtime::Error> {
        let path = file.path();

        let file_handle = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Err(crate::runtime::Error::file_not_found(path)),
        };

        let buf_reader = BufReader::new(file_handle);
        let decoder = MultiGzDecoder::new(buf_reader);

        Ok(Stream::<T> {
            inner_decoder: decoder,
            // HACK: [JD] n pools must be > 1! Otherwise inner_pool.alloc() WILL stall!
            // the problem here is a cell getting allocated near the end of the buffer
            // will keep the buffer marked as "in use" and as such the buffer cannot be
            // reset to fit the new data
            // because the buffer cannot be reset this stalls get_next()
            // => cell is kept alive and never used, keeping the buffer alive.
            // this could be fixed at the cost of speed in some way, though i am unaware of an elegant solution
            inner_buf_pool: common::PageBufferPool::new(512, 1024 * 1024 * 8),
            inner_buf_cursor: 0,
            inner_buf_slice: &[],
            inner_buf_ptr: UnsafeMutPtr::null(),

            inner_buf_truncated_len: 0,
            inner_buf_incomplete_len: 0,
            buffer_num_pages: 512,
            buffer_page_size: 1024 * 1024 * 8,
            _marker: std::marker::PhantomData,
        })
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult<u8>>, crate::runtime::Error> {
        println!(
            "LOAD_NEXT_BUF: cursor={}, slice_len={}, truncated_len={}",
            self.inner_buf_cursor,
            self.inner_buf_slice.len(),
            self.inner_buf_truncated_len
        );

        // Allocates space for new read but does NOT write anything
        let alloc_res = self.inner_buf_pool.alloc(common::HUGE_PAGE_SIZE);
        // let (incramt, partptr, copylen, ptroffset) =
        let (alloc_ptr_offset, partial_slice_ptr, partial_copy_len) =
            match alloc_res.buffer_page_ptr() == self.inner_buf_ptr.mut_ptr() {
                // Continue case
                true => {
                    /*
                        if the buffer is the same we want to:
                            1. decrement the slice pointer with length = self.partial_len
                            2. increase the slice length with length = self.partial_len
                            3. not copy any partial data
                    */
                    (
                        self.inner_buf_truncated_len + self.inner_buf_incomplete_len,
                        alloc_res.buffer_slice_ptr(),
                        0,
                    )
                }
                // Newpage case
                false => {
                    /*
                        if the buffer is a new one we want to:
                            1. leave slice ptr unchanged
                            2. increase the slice length with length = self.partial_len
                            3. allocate extra space for partial
                            4. copy partial (needs ptr)
                    */

                    // ok so this is kind of stupid but because the used buffer is truncated to last newline,
                    // the partial is contained after the end of old_buf.
                    let buf_previous = &self.inner_buf_slice;
                    // point to start of incomplete data if present
                    let buf_previous_last_complete_line_ptr = buf_previous
                        .as_ptr_range()
                        .end
                        .sub(self.inner_buf_incomplete_len);
                    (
                        0,
                        buf_previous_last_complete_line_ptr,
                        self.inner_buf_truncated_len + self.inner_buf_incomplete_len,
                    )
                }
            };

        // copy partial data
        // copylen = 0 makes this compile down to noop => useful for when we dont want to copy
        // SAFETY: [JD] in _theory_ this CAN point to stale memory. I have verified this for correctness on
        // a ~400GiB dataset and compared the resulting slice with a cloned approach and found
        // no stale memory hits. It is _likely_ fine, but cannot promise.
        let buf_slice_ptr = alloc_res.buffer_slice_mut_ptr().sub(alloc_ptr_offset);
        // SAFETY: as long as pages are of reasonable size (largest cell possible fits in one with some extra room) this is safe.
        std::ptr::copy_nonoverlapping(partial_slice_ptr, buf_slice_ptr, partial_copy_len);

        // Read new data after partial. Here we add the entire truncated line len as partial_copy_len only
        // partains to COPY length => i.e a non-copy case will not copy the data but still need to be incremented.
        let buf_write_ptr =
            buf_slice_ptr.add(self.inner_buf_truncated_len + self.inner_buf_incomplete_len);
        let write_slice =
            unsafe { std::slice::from_raw_parts_mut(buf_write_ptr, common::HUGE_PAGE_SIZE) };

        match self.inner_decoder.read(write_slice) {
            Ok(buf_bytes_written) if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written
                    + self.inner_buf_truncated_len
                    + self.inner_buf_incomplete_len;

                let bufslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);

                // Find last \n@ sequence (FASTQ record boundary)
                let mut found_pos_char_maybe_last_record = None;
                for pos_char_maybe_last_record in
                    memchr::memrchr_iter(common::U8_CHAR_FASTQ_RECORD, bufslice)
                {
                    if pos_char_maybe_last_record > 0
                        && bufslice[pos_char_maybe_last_record - 1] == common::U8_CHAR_NEWLINE
                    {
                        // Found \n@ at position (at_pos - 1, at_pos)
                        found_pos_char_maybe_last_record = Some(pos_char_maybe_last_record - 1);
                        break;
                    }
                }

                if let Some(boundary_pos) = found_pos_char_maybe_last_record {
                    let (buf_slice_truncated_use, buf_slice_truncated_line) = (
                        &bufslice[..=boundary_pos],    // Keep up to and including the \n
                        &bufslice[boundary_pos + 1..], // Everything after \n (starts with @)
                    );

                    println!(
                        "FASTQ boundary at {}, truncated data: {:?}",
                        boundary_pos,
                        String::from_utf8_lossy(
                            &buf_slice_truncated_line[..50.min(buf_slice_truncated_line.len())]
                        )
                    );

                    self.inner_buf_slice = buf_slice_truncated_use;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_len = buf_slice_truncated_line.len();
                    self.inner_buf_incomplete_len = 0;
                    self.inner_buf_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    // No FASTQ record boundary found. Likely a malformed file or first buffer
                    return Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some(
                            "No FASTQ record boundary found in buffer. Is this a valid FASTQ file?",
                        ),
                    ));
                }
            }

            // EOF; buf_bytes_written == 0
            Ok(buf_bytes_written) => {
                assert_eq!(buf_bytes_written, 0);
                let buf_slice_len = buf_bytes_written
                    + self.inner_buf_truncated_len
                    + self.inner_buf_incomplete_len;

                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);
                    self.inner_buf_slice = eofslice;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_len = 0;
                    self.inner_buf_incomplete_len = 0;
                    self.inner_buf_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    return Ok(None);
                }
            }
            Err(e) => Err(crate::runtime::Error::parse_error(
                "decoder_read",
                Some(format!("Read error code: {:?}", e)),
            )),
        }
    }

    fn try_parse_record(
        &mut self,
        builder: T::Builder,
    ) -> Result<Option<T>, crate::runtime::Error> {
        let buf_remaining = &self.inner_buf_slice[self.inner_buf_cursor..];
        // if buf_remaining.is_empty() {
        //     return Ok(None);
        // }

        let mut newline_iter = memchr::memchr_iter(common::U8_CHAR_NEWLINE, buf_remaining);

        let line_positions = match (
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
        ) {
            (Some(p1), Some(p2), Some(p3), Some(p4)) => [p1, p2, p3, p4],
            _ => {
                // Track incomplete data length (data within current slice)
                self.inner_buf_incomplete_len = buf_remaining.len();
                println!(
                    "Set incomplete data length to {} bytes",
                    self.inner_buf_incomplete_len
                );
                // Construct slice that includes both incomplete and truncated data
                unsafe {
                    let incomplete_start_ptr = self
                        .inner_buf_slice
                        .as_ptr()
                        .add(self.inner_buf_slice.len() - self.inner_buf_incomplete_len);
                    let total_partial_len =
                        self.inner_buf_incomplete_len + self.inner_buf_truncated_len;
                    let combined_data =
                        std::slice::from_raw_parts(incomplete_start_ptr, total_partial_len);

                    println!(
                        "Combined incomplete+truncated ({} bytes): {:?}",
                        total_partial_len,
                        String::from_utf8_lossy(combined_data)
                    );
                }

                return Ok(None);
            }
        };

        let line_ends: [usize; 4] = line_positions.map(|pos| self.inner_buf_cursor + pos);
        let hdr = &self.inner_buf_slice[self.inner_buf_cursor..line_ends[0]];
        let seq = &self.inner_buf_slice[line_ends[0] + 1..line_ends[1]];
        let sep = &self.inner_buf_slice[line_ends[1] + 1..line_ends[2]];
        let qal = &self.inner_buf_slice[line_ends[2] + 1..line_ends[3]];
        self.inner_buf_cursor = line_ends[3] + 1;

        // Parse record
        let (cell_id, cell_rp) = match fastq_gz::parse_record(hdr, seq, sep, qal) {
            Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
            Err(e) => {
                log_warning!("{e}"; "header" => ?String::from_utf8_lossy(hdr));
                return Ok(None);
            }
        };

        // SAFETY: transmute slices to static lifetime kept alive by ref counter
        let static_cell_id: &'static [u8] = unsafe { std::mem::transmute(cell_id) };
        let static_r: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r1) };
        let static_q: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q1) };

        let cell = builder
            .add_page_ref(self.inner_buf_ptr)
            .add_cell_id_slice(static_cell_id)
            .add_sequence_slice(static_r)
            .add_quality_slice(static_q)
            .build();

        Ok(Some(cell))
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetCell + 'static,
    T::Builder: BascetCellBuilder<Token = T>,
{
    fn set_reader_threads(self, _n_threads: usize) -> Self {
        // MultiGzDecoder is single-threaded
        log_warning!("flate2 doesn't support setting thread count directly");
        self
    }

    fn set_pagebuffer_config(mut self, num_pages: usize, page_size: usize) -> Self {
        self.buffer_num_pages = num_pages;
        self.buffer_page_size = page_size;
        self.inner_buf_pool = common::PageBufferPool::new(num_pages, page_size);
        self
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        loop {
            let builder = T::builder();
            if let Some(cell) = self.try_parse_record(builder)? {
                return Ok(Some(cell));
            }
            unsafe {
                self.load_next_buf()?;
            }
        }
    }
}
