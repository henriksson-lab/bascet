use rust_htslib::htslib;

use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::log_info;
use crate::{
    common::{self},
    io::{
        self,
        format::tirp::{self, alloc, SENTINEL_BYTE},
        traits::{BascetCell, BascetCellBuilder, BascetFile, BascetStream},
    },
};

pub struct Stream<T> {
    inner_htsfileptr: *mut htslib::htsFile,

    inner_pool: alloc::PageBufferPool,
    inner_buf: &'static [u8],
    inner_cursor: usize,
    // Raw pointer to the ref counter for the current inner_buf
    inner_buffer_ptr: *const AtomicUsize,

    partial_len: usize,
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
                inner_htsfileptr: inner_hts_file,
                // HACK: [JD] n pools must be > 1! Otherwise inner_pool.alloc() WILL stall!
                // the problem here is a cell getting allocated near the end of the buffer
                // will keep the buffer marked as "in use" and as such the buffer cannot be
                // reset to fit the new data
                // because the buffer cannot be reset this stalls get_next()
                // => cell is kept alive and never used, keeping the buffer alive.
                // this could be fixed at the cost of speed in some way, though i am unaware of an elegant solution
                inner_pool: alloc::PageBufferPool::new(32, 1024 * 1024 * 64),
                inner_cursor: 0,
                inner_buf: &[],
                inner_buffer_ptr: std::ptr::null(),
                partial_len: 0,
                _marker: std::marker::PhantomData,
            })
        }
    }

    fn load_next_buf(
        &mut self,
    ) -> Result<Option<alloc::PageBufferAllocResult>, crate::runtime::Error> {
        unsafe {
            let fileptr = htslib::hts_get_bgzfp(self.inner_htsfileptr);

            // Allocate space for new read
            let allocres = match self.inner_pool.alloc(common::HUGE_PAGE_SIZE) {
                alloc::PageBufferAllocResult::Continue {
                    ptr,
                    len,
                    buffer_page_ptr,
                    buffer_start,
                    buffer_end,
                } => {
                    // we can move the ptr back partial.len() since partial is guaranteed to lie before the newly appended page in the buffer
                    let adjptr = ptr.sub(self.partial_len);
                    let adjlen = len + self.partial_len;
                    alloc::PageBufferAllocResult::Continue {
                        ptr: adjptr,
                        len: adjlen,
                        buffer_page_ptr,
                        buffer_start,
                        buffer_end,
                    }
                }
                alloc::PageBufferAllocResult::NewPage {
                    ptr,
                    len,
                    buffer_page_ptr,
                    buffer_start,
                    buffer_end,
                } => {
                    // log_info!("NewPage");
                    // SAFETY: new page should always be large enough. Unless using tiny pages (why do that) this will always be fine :)
                    self.inner_pool
                        .active_mut()
                        .incr_ptr_unchecked(self.partial_len);

                    // partial line guaranteed to be located at the end of the buffer
                    let oldbuf = &self.inner_buf;
                    // ok so this is kind of stupid but because the used buffer is truncated to last newline,
                    // the partial is contained after the end of old_buf
                    let partptr = oldbuf.as_ptr().add(oldbuf.len());
                    let adjlen = len + self.partial_len;

                    std::ptr::copy_nonoverlapping(partptr, ptr, self.partial_len);

                    alloc::PageBufferAllocResult::Continue {
                        ptr: ptr,
                        len: adjlen,
                        buffer_page_ptr,
                        buffer_start,
                        buffer_end,
                    }
                }
            };

            // Read new data after partial
            let bufptr = allocres.ptr_mut();
            let writeptr = bufptr.add(self.partial_len);
            let writebytes = htslib::bgzf_read(
                fileptr,
                writeptr as *mut std::os::raw::c_void,
                common::HUGE_PAGE_SIZE,
            );

            match writebytes {
                n if n > 0 => {
                    let totalbytes = writebytes as usize + self.partial_len;

                    // Write sentinel byte after the read data if there's space
                    let (buffer_start, buffer_end) = allocres.buffer_bounds();
                    let sentinel_pos = bufptr.add(totalbytes);
                    if (sentinel_pos as *const u8) < buffer_end {
                        *sentinel_pos = SENTINEL_BYTE;
                    }

                    let bufslice = std::slice::from_raw_parts(bufptr, totalbytes);
                    // Find last complete line
                    if let Some(last_newline) = memchr::memrchr(b'\n', bufslice) {
                        let (partslc, bufslc) =
                            (&bufslice[last_newline + 1..], &bufslice[..=last_newline]);
                        // println!("{:?}", String::from_utf8_lossy(partslc));
                        self.partial_len = partslc.len();
                        self.inner_buf = bufslc;
                        // Store buffer page pointer for this buffer
                        self.inner_buffer_ptr = allocres.buffer_page_ptr() as *const AtomicUsize;

                        self.inner_cursor = 0;

                        return Ok(Some(allocres));
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
                    let totalbytes = writebytes as usize + self.partial_len;
                    if totalbytes > 0 {
                        // Write sentinel byte after the read data if there's space
                        let (buffer_start, buffer_end) = allocres.buffer_bounds();
                        let sentinel_pos = bufptr.add(totalbytes);
                        if (sentinel_pos as *const u8) < buffer_end {
                            *sentinel_pos = SENTINEL_BYTE;
                        }

                        let eofslice = std::slice::from_raw_parts(bufptr, totalbytes);
                        self.inner_buf = eofslice;
                        self.inner_cursor = 0;
                        // Store buffer page pointer for this buffer
                        self.inner_buffer_ptr = allocres.buffer_page_ptr() as *const AtomicUsize;

                        self.partial_len = 0;
                        return Ok(Some(allocres));
                    } else {
                        return Ok(None);
                    }
                }
                _ => Err(crate::runtime::Error::parse_error(
                    "bgzf_read",
                    Some(format!("Read error: {}", writebytes)),
                )),
            }
        }
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        // println!("Stream being dropped at {:?}", std::time::SystemTime::now());
        unsafe {
            if !self.inner_htsfileptr.is_null() {
                htslib::hts_close(self.inner_htsfileptr);
            }
        }
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetCell + 'static,
    for<'page> T::Builder<'page>: BascetCellBuilder<'page, Token = T>,
{
    fn set_reader_threads(self, n_threads: usize) -> Self {
        unsafe {
            htslib::hts_set_threads(self.inner_htsfileptr, n_threads as i32);
        }
        self
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        let mut next_id: &[u8] = &[];
        let mut builder = T::builder();

        loop {
            let mut buf = &self.inner_buf;
            if self.inner_cursor >= buf.len() {
                match self.load_next_buf()? {
                    None => {
                        // EOF
                        if next_id.is_empty() {
                            return Ok(None);
                        } else {
                            return Ok(Some(builder.build()));
                        }
                    }
                    Some(alloc_result) => {
                        // Got new buffer data
                        match &alloc_result {
                            alloc::PageBufferAllocResult::NewPage {
                                buffer_page_ptr,
                                buffer_start,
                                buffer_end,
                                ..
                            } => {
                                // New page: add buffer information to builder
                                builder = builder.add_sentinel_tracking(
                                    *buffer_page_ptr,
                                    (*buffer_start, *buffer_end),
                                );
                            }
                            alloc::PageBufferAllocResult::Continue { .. } => {
                                // Continue in same page: no new buffer info needed
                            }
                        }
                    }
                }

                buf = &self.inner_buf;
            }

            if let Some(next_pos) =
                memchr::memchr(common::U8_CHAR_NEWLINE, &buf[self.inner_cursor..])
            {
                let line_start = self.inner_cursor;
                let line_end = self.inner_cursor + next_pos;
                let line = &buf[line_start..line_end];

                if let Ok((cell_id, cell_rp)) = tirp::parse_readpair(line) {
                    if next_id.is_empty() {
                        // SAFETY: Transmute slice to static lifetime - kept alive by buffer expiration tracking
                        let lifetime_cell_id: &'static [u8] =
                            unsafe { std::mem::transmute(cell_id) };
                        builder = builder
                            .add_sentinel_tracking(
                                self.inner_buffer_ptr as *mut alloc::PageBuffer,
                                (self.inner_buf.as_ptr(), unsafe {
                                    self.inner_buf.as_ptr().add(self.inner_buf.len())
                                }),
                            )
                            .add_cell_id_slice(lifetime_cell_id);
                        next_id = cell_id;
                    }
                    if next_id != cell_id {
                        // Different cell, return the current one
                        return Ok(Some(builder.build()));
                    }

                    // SAFETY: Transmute slices to static lifetime - kept alive by ref counter
                    let lifetime_r1: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r1) };
                    let lifetime_r2: &'static [u8] = unsafe { std::mem::transmute(cell_rp.r2) };
                    let lifetime_q1: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q1) };
                    let lifetime_q2: &'static [u8] = unsafe { std::mem::transmute(cell_rp.q2) };
                    let lifetime_umi: &'static [u8] = unsafe { std::mem::transmute(cell_rp.umi) };

                    builder = builder
                        .add_rp_slice(lifetime_r1, lifetime_r2)
                        .add_qp_slice(lifetime_q1, lifetime_q2)
                        .add_umi_slice(lifetime_umi);
                }
                self.inner_cursor = line_end + 1;
            } else {
                self.inner_cursor = buf.len();
            }
        }
    }
}
