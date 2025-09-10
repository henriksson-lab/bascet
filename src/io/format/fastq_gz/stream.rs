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

    inner_buf_pool: common::PageBufferPool,
    inner_buf_ptr: common::UnsafeMutPtr<PageBuffer>,
    inner_buf_slice: &'static [u8],
    inner_buf_cursor: usize,

    inner_buf_truncated_line_len: usize,
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

            inner_buf_truncated_line_len: 0,
            buffer_num_pages: 512,
            buffer_page_size: 1024 * 1024 * 8,
            _marker: std::marker::PhantomData,
        })
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult>, crate::runtime::Error> {
        println!("LOAD_NEXT_BUF: cursor={}, slice_len={}, truncated_len={}", 
            self.inner_buf_cursor, self.inner_buf_slice.len(), self.inner_buf_truncated_line_len);
        
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
                        self.inner_buf_truncated_line_len,
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
                    let buf_previous_ptr = buf_previous.as_ptr().add(buf_previous.len());
                    (0, buf_previous_ptr, self.inner_buf_truncated_line_len)
                }
            };

        // copy partial data
        // copylen = 0 makes this compile down to noop => useful for when we dont want to copy
        // SAFETY: [JD] in _theory_ this CAN point to stale memory. I have verified this for correctness on
        // a ~400GiB dataset and compared the resulting slice with a cloned approach and found
        // no stale memory hits. It is _likely_ fine, but cannot promise.
        let buf_slice_ptr = alloc_res.buffer_slice_mut_ptr().sub(alloc_ptr_offset);
        
        // DEBUG: Print what we're copying across page boundary
        if partial_copy_len > 0 {
            let partial_data = unsafe { std::slice::from_raw_parts(partial_slice_ptr, partial_copy_len) };
            println!("COPYING {} bytes across page boundary: {:?}", 
                partial_copy_len, 
                String::from_utf8_lossy(partial_data));
        }
        
        std::ptr::copy_nonoverlapping(partial_slice_ptr, buf_slice_ptr, partial_copy_len);
        // SAFETY: as long as pages are of reasonable size (largest cell possible fits in one with some extra room) this is safe.
        assert_eq!(
            self.inner_buf_pool
                .alloc(partial_copy_len)
                .buffer_page_ptr(),
            alloc_res.buffer_page_ptr()
        );

        // Read new data after partial
        let buf_write_ptr = buf_slice_ptr.add(self.inner_buf_truncated_line_len);
        println!("alloc_slice_ptr: {:p}, truncated_len: {}, HUGE_PAGE_SIZE: {}", alloc_res.buffer_slice_mut_ptr(), self.inner_buf_truncated_line_len, common::HUGE_PAGE_SIZE);
        println!("buf_write_ptr: {:p}", buf_write_ptr);
        let write_slice = unsafe {
            std::slice::from_raw_parts_mut(buf_write_ptr, common::HUGE_PAGE_SIZE)
        };
        write_slice.fill(0);
        println!("created slice");
        let buf_bytes_written = self.inner_decoder.read(write_slice);
        println!("read slice");
        match buf_bytes_written {
            Ok(buf_bytes_written) if buf_bytes_written > 0 => {
                // std::ptr::copy_nonoverlapping(buf_temp.as_ptr(), buf_write_ptr, buf_bytes_written);

                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + self.inner_buf_truncated_line_len;
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

                    self.inner_buf_slice = buf_slice_truncated_use;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_line_len = buf_slice_truncated_line.len();
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
            Ok(buf_bytes_written) if buf_bytes_written == 0 => {
                // EOF
                let buf_slice_len = buf_bytes_written + self.inner_buf_truncated_line_len;
                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);
                    self.inner_buf_slice = eofslice;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_line_len = 0;
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
            _ => unreachable!(),
        }
    }

    fn try_parse_record(
        &mut self,
        builder: T::Builder,
    ) -> Result<Option<T>, crate::runtime::Error> {
        let remaining_slice = &self.inner_buf_slice[self.inner_buf_cursor..];
        let mut newline_iter = memchr::memchr_iter(common::U8_CHAR_NEWLINE, remaining_slice);

        let line_positions = match (
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
        ) {
            (Some(p1), Some(p2), Some(p3), Some(p4)) => [p1, p2, p3, p4],
            _ => {
                self.inner_buf_truncated_line_len += remaining_slice.len();
                return Ok(None);
            }
        };

        let line_ends: [usize; 4] = line_positions.map(|pos| self.inner_buf_cursor + pos);
        let hdr = &self.inner_buf_slice[self.inner_buf_cursor..line_ends[0]];
        let seq = &self.inner_buf_slice[line_ends[0] + 1..line_ends[1]];
        let sep = &self.inner_buf_slice[line_ends[1] + 1..line_ends[2]];
        let qal = &self.inner_buf_slice[line_ends[2] + 1..line_ends[3]];

        // Parse record
        let (cell_id, cell_rp) = match fastq_gz::parse_record(hdr, seq, sep, qal) {
            Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
            Err(e) => {
                log_warning!("{e}"; "header" => ?String::from_utf8_lossy(hdr));
                self.inner_buf_cursor = line_ends[3] + 1;
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

        self.inner_buf_cursor = line_ends[3] + 1;
        Ok(Some(cell))
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        // HACK: Spin until all page refs are zero => otherwise stream gets dropped and the slices
        // pointing to the page buffers are invalidated. Not the best solution but the most frictionless.
        let mut spin_counter = 0;
        loop {
            if self
                .inner_buf_pool
                .inner_pages
                .iter()
                .all(|p| p.ref_count.load(Ordering::Relaxed) == 0)
            {
                break;
            }
            spin_counter += 1;
            common::spin_or_park(&mut spin_counter, 100);
        }
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
            println!("Loading next buffer");
            // records do not cross pages. no need to do this here!
            // let previous_buf_ptr = self.inner_buf_ptr;
            match unsafe { self.load_next_buf()? } {
                Some(_) => {
                    // if self.inner_buf_ptr.mut_ptr() != previous_buf_ptr.mut_ptr() {
                    //     builder = builder.add_page_ref(self.inner_buf_ptr);
                    // }
                }
                None => return Ok(None), // EOF
            }
        }
    }
}
