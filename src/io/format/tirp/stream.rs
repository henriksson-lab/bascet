use rust_htslib::htslib;

use std::fs::File;
use std::sync::atomic::Ordering;

use crate::common::{PageBuffer, UnsafeMutPtr};
use crate::io::{
    self,
    format::tirp,
    traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream},
};
use crate::{common, log_info, log_warning};

pub struct Stream<T> {
    inner_htsfileptr: common::UnsafeMutPtr<htslib::htsFile>,

    inner_buf_pool: common::PageBufferPool,
    inner_buf_ptr: common::UnsafeMutPtr<PageBuffer>,
    inner_buf_slice: &'static [u8],
    inner_buf_cursor: usize,

    inner_buf_truncated_line_len: usize,
    inner_buf_partial_data: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::format::tirp::Input) -> Result<Self, crate::runtime::Error> {
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
                inner_buf_pool: common::PageBufferPool::new(64, 1024 * 1024 * 8),
                inner_buf_cursor: 0,
                inner_buf_slice: &[],
                inner_buf_ptr: UnsafeMutPtr::null(),

                inner_buf_truncated_line_len: 0,
                inner_buf_partial_data: Vec::new(),
                _marker: std::marker::PhantomData,
            })
        }
    }

    unsafe fn load_next_buf(
        &mut self,
    ) -> Result<Option<common::PageBufferAllocResult>, crate::runtime::Error> {
        let fileptr = htslib::hts_get_bgzfp(self.inner_htsfileptr.mut_ptr());
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
                    // the partial is contained after the end of old_buf
                    let buf_previous = &self.inner_buf_slice;
                    let old_approach_ptr = buf_previous.as_ptr().add(buf_previous.len());
                    
                    // For debugging: compare old pointer approach with vec approach
                    if self.inner_buf_truncated_line_len > 0 {
                        let old_approach_slice = std::slice::from_raw_parts(old_approach_ptr, self.inner_buf_truncated_line_len);
                        assert_eq!(old_approach_slice, &self.inner_buf_partial_data, 
                            "Vec and pointer approaches should give same partial data");
                    }
                    
                    // Use vec approach but return pointer for compatibility
                    (
                        0,
                        self.inner_buf_partial_data.as_ptr(),
                        self.inner_buf_truncated_line_len,
                    )
                }
            };

        // copy partial data
        // copylen = 0 makes this compile down to noop => useful for when we dont want to copy
        let buf_slice_ptr = alloc_res.buffer_slice_mut_ptr().sub(alloc_ptr_offset);
        std::ptr::copy_nonoverlapping(partial_slice_ptr, buf_slice_ptr, partial_copy_len);

        // Read new data after partial
        let buf_write_ptr = buf_slice_ptr.add(self.inner_buf_truncated_line_len);
        let buf_bytes_written = htslib::bgzf_read(
            fileptr,
            buf_write_ptr as *mut std::os::raw::c_void,
            common::HUGE_PAGE_SIZE,
        );

        match buf_bytes_written {
            buf_bytes_written if buf_bytes_written > 0 => {
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + self.inner_buf_truncated_line_len;
                let bufslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);

                // Find last complete line (simplifies parsing) (mem**r**chr)
                if let Some(pos_char_last_newline) =
                    memchr::memrchr(common::U8_CHAR_NEWLINE, bufslice)
                {
                    let (buf_slice_truncated_use, buf_slice_truncated_line) = (
                        &bufslice[..=pos_char_last_newline],
                        &bufslice[pos_char_last_newline + 1..]
                    );

                    self.inner_buf_slice = buf_slice_truncated_use;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_line_len = buf_slice_truncated_line.len();
                    // Store partial data in vec for comparison/debugging
                    self.inner_buf_partial_data.clear();
                    self.inner_buf_partial_data.extend_from_slice(buf_slice_truncated_line);
                    self.inner_buf_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    // No complete lines. Likely a malformed file
                    Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some("No complete lines found in buffer. Is this a valid file?"),
                    ))
                }
            }
            0 => {
                // EOF
                let buf_bytes_written = buf_bytes_written as usize;
                let buf_slice_len = buf_bytes_written + self.inner_buf_truncated_line_len;
                if buf_slice_len > 0 {
                    let eofslice = std::slice::from_raw_parts(buf_slice_ptr, buf_slice_len);
                    self.inner_buf_slice = eofslice;
                    // SAFETY: wrap buf ptr in Send + Sync able struct. Safety is guaranteed by page buffer ref counts
                    self.inner_buf_ptr = UnsafeMutPtr::new(alloc_res.buffer_page_mut_ptr());

                    self.inner_buf_truncated_line_len = 0;
                    self.inner_buf_partial_data.clear();
                    self.inner_buf_cursor = 0;

                    return Ok(Some(alloc_res));
                } else {
                    return Ok(None);
                }
            }
            _ => Err(crate::runtime::Error::parse_error(
                "bgzf_read",
                Some(format!("Read error code: {}", buf_bytes_written)),
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
    fn set_reader_threads(self, n_threads: usize) -> Self {
        unsafe {
            htslib::hts_set_threads(self.inner_htsfileptr.mut_ptr(), n_threads as i32);
        }
        self
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        let mut next_id: &[u8] = &[];
        let mut builder = T::builder();

        if !self.inner_buf_ptr.is_null() {
            builder = builder.add_page_ref(self.inner_buf_ptr);
        }

        loop {
            while let Some(pos_char_next_newline) = memchr::memchr(
                common::U8_CHAR_NEWLINE,
                &self.inner_buf_slice[self.inner_buf_cursor..],
            ) {
                let line_start = self.inner_buf_cursor;
                let line_end = self.inner_buf_cursor + pos_char_next_newline;
                let line = &self.inner_buf_slice[line_start..line_end];
                self.inner_buf_cursor = line_end + 1;

                let (cell_id, cell_rp) = match tirp::parse_record(line) {
                    Ok((cell_id, cell_rp)) => (cell_id, cell_rp),
                    Err(e) => {
                        log_warning!("{e}"; "line" => ?String::from_utf8_lossy(line), "partial" => ?String::from_utf8_lossy(&self.inner_buf_partial_data),);
                        continue;
                    }
                };

                // SAFETY: Transmute slice to static lifetime; kept alive by buffer expiration tracking
                let static_cell_id: &'static [u8] = unsafe { std::mem::transmute(cell_id) };
                if next_id.is_empty() {
                    builder = builder.add_cell_id_slice(static_cell_id);
                    log_info!("New Cell"; "cell" => ?String::from_utf8_lossy(cell_id));
                    next_id = cell_id;
                } else if next_id != cell_id {
                    self.inner_buf_cursor = line_start;
                return Ok(Some(builder.build()));
                }

                // SAFETY: Transmute slices to static static - kept alive by ref counter
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

            // No more complete lines in current buffer
            let previous_buf_ptr = self.inner_buf_ptr;
            match unsafe { self.load_next_buf()? } {
                Some(_) => {
                    if self.inner_buf_ptr.mut_ptr() == previous_buf_ptr.mut_ptr() {
                        log_info!("Continue Buffer Page");
                        continue;
                    }

                    log_info!("New Buffer Page");
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