use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{BufReader, Read};

use crate::common::{PageBuffer, UnsafeMutPtr};
use crate::{common, log_critical, log_warning};

use crate::io;
use crate::io::format::fastq_gz;
use crate::io::traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream};

pub struct Stream<T> {
    inner_decoder: MultiGzDecoder<BufReader<File>>,

    inner_buf_pool: common::PageBufferPool<u8, { common::PAGE_BUFFER_MAX_PAGES }>,
    inner_buf_ptr: common::UnsafeMutPtr<PageBuffer<u8>>,
    inner_buf_slice: &'static [u8],
    inner_buf_cursor: usize,

    inner_buf_incomplete_start_ptr: *const u8,
    inner_buf_truncated_end_ptr: *const u8,
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
            inner_buf_pool: common::PageBufferPool::new(0, 0)?,
            inner_buf_cursor: 0,
            inner_buf_slice: &[],
            inner_buf_ptr: UnsafeMutPtr::null(),

            inner_buf_incomplete_start_ptr: std::ptr::null(),
            inner_buf_truncated_end_ptr: std::ptr::null(),
            _marker: std::marker::PhantomData,
        })
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult<u8>>, crate::runtime::Error> {
        // Allocates space for new read but does NOT write anything
        let alloc_res = self.inner_buf_pool.alloc(common::HUGE_PAGE_SIZE);

        let (buf_ptr, partial_copy_len) =
            match alloc_res.buffer_page_ptr() == self.inner_buf_ptr.mut_ptr() {
                // Continue case: use existing incomplete data pointer as slice start
                true => {
                    (
                        self.inner_buf_incomplete_start_ptr as *mut u8,
                        0, // No copy needed
                    )
                }
                // New page case: copy partial data to new buffer
                false => {
                    let partial_copy_len = self
                        .inner_buf_truncated_end_ptr
                        .offset_from(self.inner_buf_incomplete_start_ptr)
                        as usize;
                    (
                        alloc_res.buffer_slice_mut_ptr(),
                        partial_copy_len,
                    )
                }
            };

        // Copy partial data (no-op in continue case where partial_copy_len = 0)
        // SAFETY: [JD] in _theory_ this CAN point to stale memory. I have verified this for correctness on
        // a ~400GiB dataset and compared the resulting slice with a cloned approach and found
        // no stale memory hits. It is _likely_ fine, but cannot promise.
        std::ptr::copy_nonoverlapping(self.inner_buf_incomplete_start_ptr, buf_ptr, partial_copy_len);

        // Read new data after partial data
        let carry_data_len =
            self.inner_buf_truncated_end_ptr
                .offset_from(self.inner_buf_incomplete_start_ptr) as usize;
        let buf_write_ptr = buf_ptr.add(carry_data_len);
        let write_slice =
            unsafe { std::slice::from_raw_parts_mut(buf_write_ptr, common::HUGE_PAGE_SIZE) };

        match self.inner_decoder.read(write_slice) {
            Ok(buf_bytes_written) if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + carry_data_len;

                let bufslice = std::slice::from_raw_parts(buf_ptr, buf_slice_len);

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
                    self.inner_buf_truncated_end_ptr = buf_slice_truncated_line.as_ptr_range().end;
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
                let buf_slice_len = buf_bytes_written + carry_data_len;

                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts(buf_ptr, buf_slice_len);
                    self.inner_buf_slice = eofslice;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_end_ptr = std::ptr::null();
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
        if buf_remaining.is_empty() {
            return Ok(None);
        }

        let mut newline_iter = memchr::memchr_iter(common::U8_CHAR_NEWLINE, buf_remaining);

        let line_positions = match (
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
            newline_iter.next(),
        ) {
            (Some(p1), Some(p2), Some(p3), Some(p4)) => [p1, p2, p3, p4],
            _ => {
                self.inner_buf_incomplete_start_ptr = buf_remaining.as_ptr();
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
        let inner_buf_pool_res = common::PageBufferPool::new(num_pages, page_size);
        self.inner_buf_pool = match inner_buf_pool_res {
            Ok(mut pool) => {
                // Initialize both pointers to buffer start to avoid null pointers
                let buf_start = pool.alloc(0).buffer_slice_ptr();
                self.inner_buf_incomplete_start_ptr = buf_start;
                self.inner_buf_truncated_end_ptr = buf_start;
                self.inner_buf_slice = unsafe { std::slice::from_raw_parts(buf_start, 0) };
                pool
            },
            Err(e) => {
                log_critical!("Failed to create PageBufferPool: {e}");
            }
        };
        self
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        loop {
            let builder = T::builder();
            if let Some(cell) = self.try_parse_record(builder)? {
                return Ok(Some(cell));
            }
            unsafe {
                if self.load_next_buf()?.is_none() {
                    return Ok(None); // EOF reached
                }
            }
        }
    }
}
