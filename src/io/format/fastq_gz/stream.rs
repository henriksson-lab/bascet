use rust_htslib::htslib;

use std::fs::File;

use crate::common::{PageBuffer, PageBufferPool, UnsafePtr};
use crate::{common, log_critical, log_warning};

use crate::io;
use crate::io::format::fastq_gz;
use crate::io::traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream};

pub struct Stream<T> {
    inner_htsfile_ptr: common::UnsafePtr<htslib::htsFile>,
    inner_pool: common::PageBufferPool<u8, { common::PAGE_BUFFER_MAX_PAGES }>,

    inner_buf: &'static mut [u8],
    inner_cursor: usize,

    inner_page_ptr: common::UnsafePtr<PageBuffer<u8>>,
    inner_incomplete_start_ptr: common::UnsafePtr<u8>,
    inner_truncated_end_ptr: common::UnsafePtr<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T>
where
    T: BascetCell + 'static,
    T::Builder: BascetCellBuilder<Token = T>,
{
    pub fn new(file: &io::format::fastq_gz::Input) -> Result<Self, crate::runtime::Error> {
        let path = file.path();

        let _file_handle = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Err(crate::runtime::Error::file_not_found(path)),
        };

        unsafe {
            let path_str = match path.to_str() {
                Some(s) => s,
                None => {
                    return Err(crate::runtime::Error::file_not_valid(
                        path,
                        Some("Invalid UTF-8 in path"),
                    ))
                }
            };

            let c_path = match std::ffi::CString::new(path_str.as_bytes()) {
                Ok(p) => p,
                Err(_) => {
                    return Err(crate::runtime::Error::file_not_valid(
                        path,
                        Some("Path contains null bytes"),
                    ))
                }
            };

            let mode = match std::ffi::CString::new("r") {
                Ok(m) => m,
                Err(_) => {
                    return Err(crate::runtime::Error::file_not_valid(
                        path,
                        Some("Failed to create mode string"),
                    ))
                }
            };

            let inner_hts_file = htslib::hts_open(c_path.as_ptr(), mode.as_ptr());
            if inner_hts_file.is_null() {
                return Err(crate::runtime::Error::file_not_valid(
                    path,
                    Some("hts_open returned null"),
                ));
            }

            Ok(Stream::<T> {
                inner_htsfile_ptr: UnsafePtr::new(inner_hts_file),
                inner_pool: PageBufferPool::new(0, 0)?,

                inner_buf: &mut [],
                inner_cursor: 0,

                inner_page_ptr: UnsafePtr::null(),
                inner_incomplete_start_ptr: UnsafePtr::null(),
                inner_truncated_end_ptr: UnsafePtr::null(),
                _marker: std::marker::PhantomData,
            })
        }
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult<u8>>, crate::runtime::Error> {
        // println!("DEBUG: load_next_buf entry");
        let alloc_res = self.inner_pool.alloc(common::HUGE_PAGE_SIZE);
        // println!("DEBUG: alloc_res page_ptr: {:p}, buf_ptr: {:p}", *alloc_res.page_ptr, *alloc_res.buf_ptr);
        let carry_offset = self
            .inner_truncated_end_ptr
            .offset_from(self.inner_incomplete_start_ptr) as usize;

        let (buf_ptr, carry_copy_len) = match *alloc_res.page_ptr == *self.inner_page_ptr {
            true => (self.inner_incomplete_start_ptr, 0),
            false => (alloc_res.buf_ptr, carry_offset),
        };

        self.inner_pool.alloc(carry_copy_len);
        std::ptr::copy_nonoverlapping(*self.inner_incomplete_start_ptr, *buf_ptr, carry_copy_len);

        let buf_write_ptr = buf_ptr.add(carry_offset);
        let fileptr = htslib::hts_get_bgzfp(*self.inner_htsfile_ptr);
        // println!("DEBUG: about to call bgzf_read, fileptr: {:p}, buf_write_ptr: {:p}", fileptr, buf_write_ptr);
        match htslib::bgzf_read(
            fileptr,
            *buf_write_ptr as *mut std::os::raw::c_void,
            common::HUGE_PAGE_SIZE,
        ) {
            buf_bytes_written if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + carry_offset;
                let buf_slice = std::slice::from_raw_parts_mut(*buf_ptr, buf_slice_len);

                // NOTE: Find last \n@ sequence (very likely FASTQ record boundary). Technically not guaranteed to be last though.
                let mut found_pos_char_maybe_last_record = None;
                for pos_char_maybe_last_record in
                    memchr::memrchr_iter(common::U8_CHAR_FASTQ_RECORD, buf_slice)
                {
                    if pos_char_maybe_last_record > 0
                        && buf_slice[pos_char_maybe_last_record - 1] == common::U8_CHAR_NEWLINE
                    {
                        // Found \n@ at position (at_pos - 1, at_pos)
                        found_pos_char_maybe_last_record = Some(pos_char_maybe_last_record - 1);
                        break;
                    }
                }

                if let Some(found_pos_char_maybe_last_record) = found_pos_char_maybe_last_record {
                    let (buf_slice, buf_slice_truncated) =
                        buf_slice.split_at_mut(found_pos_char_maybe_last_record + 1);

                    self.inner_page_ptr = alloc_res.page_ptr;
                    self.inner_incomplete_start_ptr =
                        UnsafePtr::new(buf_slice_truncated.as_mut_ptr_range().start);
                    self.inner_truncated_end_ptr =
                        UnsafePtr::new(buf_slice_truncated.as_mut_ptr_range().end);

                    self.inner_buf = buf_slice;
                    self.inner_cursor = 0;

                    // println!("DEBUG: load_next_buf returning success");
                    return Ok(Some(alloc_res));
                } else {
                    // println!("DEBUG: no record boundary found in buffer");
                    return Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some(
                            "No FASTQ record boundary found in buffer. Is this a valid FASTQ file?",
                        ),
                    ));
                }
            }
            0 => {
                // println!("DEBUG: bgzf_read EOF, carry_offset={}", carry_offset);
                let buf_slice_len = carry_offset;
                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts_mut(*buf_ptr, buf_slice_len);

                    self.inner_page_ptr = alloc_res.page_ptr;
                    self.inner_incomplete_start_ptr =
                        UnsafePtr::new(eofslice.as_mut_ptr_range().start);
                    self.inner_truncated_end_ptr = UnsafePtr::new(eofslice.as_mut_ptr_range().end);

                    self.inner_buf = eofslice;
                    self.inner_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    return Ok(None);
                }
            }
            err => Err(crate::runtime::Error::parse_error(
                "bgzf_read",
                Some(format!("Read error code: {}", err)),
            )),
        }
    }

    fn try_parse_record(
        &mut self,
        builder: T::Builder,
    ) -> Result<Option<T>, crate::runtime::Error> {
        let buf_remaining = &mut self.inner_buf[self.inner_cursor..];
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
                self.inner_incomplete_start_ptr =
                    UnsafePtr::new(buf_remaining.as_mut_ptr_range().start);
                return Ok(None);
            }
        };

        let line_ends: [usize; 4] = line_positions.map(|pos| self.inner_cursor + pos);
        let hdr = &self.inner_buf[self.inner_cursor..line_ends[0]];
        let seq = &self.inner_buf[line_ends[0] + 1..line_ends[1]];
        let sep = &self.inner_buf[line_ends[1] + 1..line_ends[2]];
        let qal = &self.inner_buf[line_ends[2] + 1..line_ends[3]];
        self.inner_cursor = line_ends[3] + 1;

        // Parse record
        let (cell_id, cell_rp) = match fastq_gz::parse_record(hdr, seq, sep, qal) {
            Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
            Err(e) => {
                log_warning!(
                    "{e}";
                    "hdr" => ?String::from_utf8_lossy(hdr),
                    "seq" => ?String::from_utf8_lossy(seq),
                    "sep" => ?String::from_utf8_lossy(sep),
                    "qal" => ?String::from_utf8_lossy(qal)
                );
                return Ok(None);
            }
        };

        // SAFETY: transmute slices to static lifetime kept alive by ref counter
        let static_cell_id: &'static [u8] = unsafe { std::mem::transmute(cell_id) };
        let static_r: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r1) };
        let static_q: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q1) };

        let cell = builder
            .add_page_ref(self.inner_page_ptr)
            .add_cell_id_slice(static_cell_id)
            .add_sequence_slice(static_r)
            .add_quality_slice(static_q)
            .build();

        Ok(Some(cell))
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        unsafe {
            if !self.inner_htsfile_ptr.is_null() {
                htslib::hts_close(*self.inner_htsfile_ptr);
            }
        }
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetCell + 'static,
    T::Builder: BascetCellBuilder<Token = T>,
{
    fn set_reader_threads(self, n_threads: usize) -> Self {
        unsafe {
            htslib::hts_set_threads(*self.inner_htsfile_ptr, n_threads as i32);
        }
        self
    }

    fn set_pagebuffer_config(mut self, num_pages: usize, page_size: usize) -> Self {
        self.inner_pool = match common::PageBufferPool::new(num_pages, page_size) {
            Ok(mut pool) => {
                let buf_start = pool.alloc(0).buf_ptr;
                self.inner_incomplete_start_ptr = buf_start;
                self.inner_truncated_end_ptr = buf_start;
                self.inner_buf = unsafe { std::slice::from_raw_parts_mut(*buf_start, 0) };
                pool
            }
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
