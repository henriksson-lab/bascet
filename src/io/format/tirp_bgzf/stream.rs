use rust_htslib::htslib;

use std::fs::File;
use std::sync::atomic::Ordering;

use crate::common::{PageBuffer, UnsafeMutPtr};
use crate::io::{
    self,
    format::tirp_bgzf,
    traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream},
};
use crate::{common, log_critical, log_warning};

pub struct Stream<T> {
    inner_htsfileptr: common::UnsafeMutPtr<htslib::htsFile>,

    inner_buf_pool: common::PageBufferPool<u8, 512>,
    inner_buf_ptr: common::UnsafeMutPtr<PageBuffer<u8>>,
    inner_buf_slice: &'static [u8],
    inner_buf_cursor: usize,

    inner_buf_truncated_end_ptr: *const u8,
    buffer_num_pages: usize,
    buffer_page_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::format::tirp_bgzf::Input) -> Result<Self, crate::runtime::Error> {
        let path = file.path();

        let _file = match File::open(&path) {
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
                inner_htsfileptr: UnsafeMutPtr::new(inner_hts_file),
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

                inner_buf_truncated_end_ptr: std::ptr::null(),
                buffer_num_pages: 512,
                buffer_page_size: 1024 * 1024 * 8,
                _marker: std::marker::PhantomData,
            })
        }
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult<u8>>, crate::runtime::Error> {
        let fileptr = htslib::hts_get_bgzfp(self.inner_htsfileptr.mut_ptr());
        // Allocates space for new read but does NOT write anything
        let alloc_res = self.inner_buf_pool.alloc(common::HUGE_PAGE_SIZE);
        // let (incramt, partptr, copylen, ptroffset) =
        let (alloc_ptr_offset, partial_slice_ptr, partial_copy_len) =
            match alloc_res.buffer_page_ptr() == self.inner_buf_ptr.mut_ptr() {
                // Continue case: move buffer pointer back to include truncated data
                true => {
                    let truncated_len = self.inner_buf_truncated_end_ptr.offset_from(
                        self.inner_buf_slice.as_ptr_range().end
                    ) as usize;
                    (
                        truncated_len,
                        alloc_res.buffer_slice_ptr(),
                        0,
                    )
                }
                // New page case: copy truncated data from end of previous buffer
                false => {
                    let buf_previous = &self.inner_buf_slice;
                    let truncated_start_ptr = buf_previous.as_ptr_range().end;
                    let truncated_len = self.inner_buf_truncated_end_ptr.offset_from(truncated_start_ptr) as usize;
                    (0, truncated_start_ptr, truncated_len)
                }
            };

        // copy partial data
        // copylen = 0 makes this compile down to noop => useful for when we dont want to copy
        // SAFETY: [JD] in _theory_ this CAN point to stale memory. I have verified this for correctness on
        // a ~400GiB dataset and compared the resulting slice with a cloned approach and found
        // no stale memory hits. It is _likely_ fine, but cannot promise.
        let buf_slice_ptr = alloc_res.buffer_slice_mut_ptr().sub(alloc_ptr_offset);
        std::ptr::copy_nonoverlapping(partial_slice_ptr, buf_slice_ptr, partial_copy_len);
        // SAFETY: as long as pages are of reasonable size (largest cell possible fits in one with some extra room) this is safe.
        assert_eq!(
            self.inner_buf_pool
                .alloc(partial_copy_len)
                .buffer_page_ptr(),
            alloc_res.buffer_page_ptr()
        );

        // Read new data after truncated data
        let truncated_len = self.inner_buf_truncated_end_ptr.offset_from(
            self.inner_buf_slice.as_ptr_range().end
        ) as usize;
        let buf_write_ptr = buf_slice_ptr.add(truncated_len);

        match htslib::bgzf_read(
            fileptr,
            buf_write_ptr as *mut std::os::raw::c_void,
            common::HUGE_PAGE_SIZE,
        ) {
            buf_bytes_written if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + truncated_len;
                let bufslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);

                // Find last complete line (simplifies parsing) (mem**r**chr)
                if let Some(pos_char_last_newline) =
                    memchr::memrchr(common::U8_CHAR_NEWLINE, bufslice)
                {
                    let (buf_slice_truncated_use, buf_slice_truncated_line) = (
                        &bufslice[..=pos_char_last_newline],
                        &bufslice[pos_char_last_newline + 1..],
                    );

                    self.inner_buf_slice = buf_slice_truncated_use;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_end_ptr = buf_slice_truncated_line.as_ptr_range().end;
                    self.inner_buf_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    // No complete lines. Likely a malformed file
                    return Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some("No complete lines found in buffer. Is this a valid file?"),
                    ));
                }
            }
            0 => {
                // EOF
                let buf_slice_len = self.inner_buf_truncated_end_ptr.offset_from(
                    self.inner_buf_slice.as_ptr_range().end
                ) as usize;
                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);
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
            if !self.inner_htsfileptr.is_null() {
                htslib::hts_close(self.inner_htsfileptr.mut_ptr());
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
            htslib::hts_set_threads(self.inner_htsfileptr.mut_ptr(), n_threads as i32);
        }
        self
    }

    fn set_pagebuffer_config(mut self, num_pages: usize, page_size: usize) -> Self {
        self.buffer_num_pages = num_pages;
        self.buffer_page_size = page_size;
        let inner_buf_pool_res = common::PageBufferPool::new(num_pages, page_size);
        self.inner_buf_pool = match inner_buf_pool_res {
            Ok(mut pool) => {
                // Initialize pointer and slice to buffer start to avoid null pointer arithmetic
                let buf_start = pool.alloc(0).buffer_slice_ptr();
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
        let mut next_id: &[u8] = &[];
        let mut builder = T::builder();

        loop {
            while let Some(pos_char_next_newline) = memchr::memchr(
                common::U8_CHAR_NEWLINE,
                &self.inner_buf_slice[self.inner_buf_cursor..],
            ) {
                let line_start = self.inner_buf_cursor;
                let line_end = self.inner_buf_cursor + pos_char_next_newline;
                let line = &self.inner_buf_slice[line_start..line_end];

                let (cell_id, cell_rp) = match tirp_bgzf::parse_record(line) {
                    Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
                    Err(e) => {
                        log_warning!("{e}"; "line" => ?String::from_utf8_lossy(line));
                        continue;
                    }
                };

                // SAFETY: transmute slice to static lifetime kept alive by ref counter
                let static_cell_id: &'static [u8] = unsafe { std::mem::transmute(cell_id) };
                if next_id.is_empty() {
                    // NOTE: Add page ref only when starting a cell to avoid leaking refs on empty streams
                    builder = builder
                        .add_cell_id_slice(static_cell_id)
                        .add_page_ref(self.inner_buf_ptr);
                    next_id = cell_id;
                } else if next_id != cell_id {
                    return Ok(Some(builder.build()));
                }

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

                self.inner_buf_cursor = line_end + 1;
            }

            // No more complete lines in current buffer
            let previous_buf_ptr = self.inner_buf_ptr;
            match unsafe { self.load_next_buf()? } {
                Some(_) => {
                    if self.inner_buf_ptr.mut_ptr() == previous_buf_ptr.mut_ptr() {
                        continue;
                    }

                    builder = builder.add_page_ref(self.inner_buf_ptr);
                }
                None => {
                    // EOF
                    return Ok(match next_id.is_empty() {
                        true => None,
                        false => Some(builder.build()),
                    });
                }
            }
        }
    }
}
