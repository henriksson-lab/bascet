use rust_htslib::htslib;

use likely_stable::{likely, unlikely};
use std::fs::File;
use std::io::Read;

use crate::common::{self, PageBuffer};
use crate::io::{
    self,
    format::tirp_bgzf,
    traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream},
};
use crate::threading::{self, UnsafePtr};
use crate::{log_critical, log_warning};

pub struct Stream<T> {
    inner_htsfile_ptr: threading::UnsafePtr<htslib::htsFile>,
    inner_hts_tpool: htslib::htsThreadPool,
    inner_hts_block_size: usize,

    inner_pool: common::PageBufferPool<u8, { common::PAGE_BUFFER_MAX_PAGES }>,
    inner_buf: &'static [u8],

    inner_cursor_ptr: threading::UnsafePtr<u8>,
    inner_page_ptr: threading::UnsafePtr<PageBuffer<u8>>,
    inner_incomplete_start_ptr: threading::UnsafePtr<u8>,
    inner_truncated_end_ptr: threading::UnsafePtr<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::format::tirp_bgzf::Input) -> Result<Self, crate::runtime::Error> {
        let path = file.path();

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Err(crate::runtime::Error::file_not_found(path)),
        };

        let mut hdr = [0u8; 18];
        file.read_exact(&mut hdr)?;
        let block_size = u16::from_le_bytes([hdr[16], hdr[17]]) + 1;

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
                inner_hts_tpool: htslib::htsThreadPool {
                    pool: std::ptr::null::<htslib::hts_tpool>() as *mut htslib::hts_tpool,
                    qsize: 0,
                },
                inner_hts_block_size: block_size as usize,
                // HACK: [JD] n pools must be > 1! Otherwise inner_pool.alloc() WILL stall!
                // the problem here is a cell getting allocated near the end of the buffer
                // will keep the buffer marked as "in use" and as such the buffer cannot be
                // reset to fit the new data
                // because the buffer cannot be reset this stalls get_next()
                // => cell is kept alive and never used, keeping the buffer alive.
                // this could be fixed at the cost of speed in some way, though i am unaware of an elegant solution
                inner_pool: common::PageBufferPool::new(0, 0)?,
                inner_buf: &[],

                inner_cursor_ptr: UnsafePtr::null(),
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
        unsafe {
            std::hint::assert_unchecked(!self.inner_page_ptr.is_null());
            std::hint::assert_unchecked(!self.inner_truncated_end_ptr.is_null());
            std::hint::assert_unchecked(!self.inner_incomplete_start_ptr.is_null());
        }

        // incr ref count to last page, data may need to be copied
        let last_page = self.inner_page_ptr;
        (**last_page).inc_ref();

        let carry_offset = self
            .inner_truncated_end_ptr
            .offset_from(self.inner_incomplete_start_ptr) as usize;

        let alloc_res = self.inner_pool.alloc(self.inner_hts_block_size);
        let (buf_ptr, carry_copy_len) = match *alloc_res.page_ptr == *self.inner_page_ptr {
            true => (self.inner_incomplete_start_ptr, 0),
            false => (alloc_res.buf_ptr, carry_offset),
        };

        let buf_write_ptr = buf_ptr.add(carry_offset);
        let fileptr = htslib::hts_get_bgzfp(*self.inner_htsfile_ptr);
        unsafe {
            std::hint::assert_unchecked(!buf_ptr.is_null());
            std::hint::assert_unchecked(!fileptr.is_null());
            std::hint::assert_unchecked(!buf_write_ptr.is_null());
        }

        self.inner_pool.alloc(carry_copy_len);
        std::ptr::copy_nonoverlapping(*self.inner_incomplete_start_ptr, *buf_ptr, carry_copy_len);
        // free ref count to last page, data has been copied already
        (**last_page).dec_ref();
        // let start = std::time::Instant::now();
        let result = htslib::bgzf_read(
            fileptr,
            *buf_write_ptr as *mut std::os::raw::c_void,
            self.inner_hts_block_size,
        );
        // println!("bgzf_read took: {:?}", start.elapsed());
        match result {
            buf_bytes_written if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + carry_offset;
                let buf_slice = std::slice::from_raw_parts_mut(*buf_ptr, buf_slice_len);

                // Find last complete line (simplifies parsing) (mem**r**chr)
                let found_pos_char_maybe_last_record =
                    memchr::memrchr(common::U8_CHAR_NEWLINE, buf_slice);

                if let Some(found_pos_char_maybe_last_record) = found_pos_char_maybe_last_record {
                    unsafe {
                        std::hint::assert_unchecked(
                            found_pos_char_maybe_last_record < buf_slice.len(),
                        );
                    }
                    let (buf_slice, buf_slice_truncated) =
                        buf_slice.split_at_mut(found_pos_char_maybe_last_record + 1);

                    self.inner_page_ptr = alloc_res.page_ptr;
                    self.inner_cursor_ptr = UnsafePtr::new(buf_slice.as_mut_ptr_range().start);
                    self.inner_incomplete_start_ptr =
                        UnsafePtr::new(buf_slice_truncated.as_mut_ptr_range().start);
                    self.inner_truncated_end_ptr =
                        UnsafePtr::new(buf_slice_truncated.as_mut_ptr_range().end);

                    self.inner_buf = buf_slice;
                    return Ok(Some(alloc_res));
                } else {
                    return Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some("No tirp record boundary found in buffer. Is this a valid tirp file?"),
                    ));
                }
            }
            0 => {
                let buf_slice_len = carry_offset;
                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts_mut(*buf_ptr, buf_slice_len);
                    self.inner_page_ptr = alloc_res.page_ptr;
                    self.inner_cursor_ptr = UnsafePtr::new(eofslice.as_mut_ptr_range().start);
                    self.inner_incomplete_start_ptr =
                        UnsafePtr::new(eofslice.as_mut_ptr_range().start);
                    self.inner_truncated_end_ptr = UnsafePtr::new(eofslice.as_mut_ptr_range().end);

                    self.inner_buf = eofslice;
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
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        unsafe {
            std::hint::assert_unchecked(!self.inner_htsfile_ptr.is_null());
            std::hint::assert_unchecked(!self.inner_hts_tpool.pool.is_null());

            htslib::hts_close(*self.inner_htsfile_ptr);
            htslib::hts_tpool_destroy(self.inner_hts_tpool.pool);
        }
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetCell + 'static,
    T::Builder: BascetCellBuilder<Token = T>,
{
    fn set_reader_threads(&mut self, n_threads: usize) {
        unsafe {
            let inner_tpool = htslib::hts_tpool_init(n_threads as i32);
            let mut tpool = htslib::htsThreadPool {
                pool: inner_tpool,
                qsize: 0,
            };

            self.inner_hts_tpool = tpool;
            if htslib::hts_set_thread_pool(
                *self.inner_htsfile_ptr,
                &mut tpool as *mut htslib::htsThreadPool,
            ) < 0
            {
                log_critical!("Failed to create threadpool"; "n_threads" => n_threads);
            }
            self.inner_hts_block_size *= n_threads;
        }
    }

    fn set_pagebuffer_config(&mut self, num_pages: usize, page_size: usize) {
        self.inner_pool = match common::PageBufferPool::new(num_pages, page_size) {
            Ok(mut pool) => {
                // Initialize pointer and slice to buffer start to avoid null ptr branches
                let alloc = pool.alloc(0);
                self.inner_page_ptr = alloc.page_ptr;

                let buf_start = alloc.buf_ptr;
                self.inner_cursor_ptr = buf_start;
                self.inner_incomplete_start_ptr = buf_start;
                self.inner_truncated_end_ptr = buf_start;
                self.inner_buf = unsafe { std::slice::from_raw_parts(*buf_start, 0) };
                pool
            }
            Err(e) => {
                log_critical!("Failed to create PageBufferPool: {e}");
            }
        };
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        let mut next_id: &[u8] = &[];
        let mut builder = T::builder();

        loop {
            let remaining_len = unsafe {
                let remaining_len =
                    self.inner_buf
                        .as_ptr_range()
                        .end
                        .offset_from(*self.inner_cursor_ptr) as usize;
                remaining_len
            };
            let remaining_slice = unsafe {
                let remaining_slice =
                    std::slice::from_raw_parts(*self.inner_cursor_ptr, remaining_len);
                remaining_slice
            };

            let mut pos_char_last_newline = 0;
            for pos_char_next_newline in
                memchr::memchr_iter(common::U8_CHAR_NEWLINE, remaining_slice)
            {
                let line = unsafe {
                    std::hint::assert_unchecked(pos_char_next_newline < remaining_len);
                    std::slice::from_raw_parts(
                        remaining_slice.as_ptr().add(pos_char_last_newline),
                        pos_char_next_newline - pos_char_last_newline,
                    )
                };
                pos_char_last_newline = pos_char_next_newline + 1;
                let (cell_id, cell_rp) = match tirp_bgzf::parse_record(line) {
                    Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
                    Err(e) => {
                        log_warning!("{e}"; "line" => ?String::from_utf8_lossy(line));
                        continue;
                    }
                };

                // SAFETY: transmute slice to static lifetime kept alive by ref counter
                let static_cell_id: &'static [u8] = unsafe { std::mem::transmute(cell_id) };
                if unlikely(next_id.is_empty()) {
                    // NOTE: Add page ref only when starting a cell to avoid leaking refs on empty streams
                    unsafe {
                        std::hint::assert_unchecked(!self.inner_page_ptr.is_null());
                    }

                    builder = builder
                        .add_cell_id_slice(static_cell_id)
                        .add_page_ref(self.inner_page_ptr);
                    next_id = cell_id;
                }
                if unlikely(next_id != cell_id) {
                    unsafe {
                        self.inner_cursor_ptr = self.inner_cursor_ptr.add(pos_char_last_newline)
                    }
                    return Ok(Some(builder.build()));
                };

                // SAFETY: transmute slices to static static kept alive by ref counter
                let static_r1: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r1) };
                let static_r2: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r2) };
                let static_q1: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q1) };
                let static_q2: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q2) };
                let static_umi: &'static [u8] = unsafe { std::mem::transmute(cell_rp.umi) };

                builder = builder
                    .add_rp_slice(static_r1, static_r2)
                    .add_qp_slice(static_q1, static_q2)
                    .add_umi_slice(static_umi);
            }
            let previous_buf_ptr = self.inner_page_ptr;
            match unsafe { self.load_next_buf()? } {
                Some(_) => {
                    if likely(*self.inner_page_ptr == *previous_buf_ptr) {
                        continue;
                    };

                    unsafe {
                        std::hint::assert_unchecked(!self.inner_page_ptr.is_null());
                    }
                    builder = builder.add_page_ref(self.inner_page_ptr);
                }
                None => {
                    unsafe {
                        self.inner_cursor_ptr = self.inner_cursor_ptr.add(pos_char_last_newline)
                    }
                    return Ok(match next_id.is_empty() {
                        true => None,
                        false => Some(builder.build()),
                    });
                }
            }
        }
    }
}
