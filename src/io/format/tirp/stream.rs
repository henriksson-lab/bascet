use memmap2::Mmap;
use rust_htslib::htslib;

use std::fs::File;
use std::sync::Arc;

use crate::{
    common::{self},
    io::{
        self,
        format::{self, tirp},
        BascetFile, BascetStream, BascetStreamToken, BascetStreamTokenBuilder,
    },
};

pub struct Stream<T> {
    inner_hts_file: *mut htslib::htsFile,

    inner_buf: Option<Arc<Vec<u8>>>,
    inner_partial: Vec<u8>,
    inner_cursor: usize,
    inner_read_buf: Vec<u8>,

    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::tirp::File) -> Result<Self, crate::runtime::Error> {
        let path = file.file_path();

        let file = match File::open(&path) {
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
                inner_hts_file,
                inner_buf: None,
                inner_cursor: 0,
                inner_partial: Vec::new(),
                inner_read_buf: vec![0; common::HUGE_PAGE_SIZE],
                _marker: std::marker::PhantomData,
            })
        }
    }

    fn load_next_buf(&mut self) -> Result<bool, crate::runtime::Error> {
        unsafe {
            let fp = htslib::hts_get_bgzfp(self.inner_hts_file);

            self.inner_read_buf.clear();
            self.inner_read_buf.resize(common::HUGE_PAGE_SIZE, 0);

            let bytes_read = htslib::bgzf_read(
                fp,
                self.inner_read_buf.as_mut_ptr() as *mut std::os::raw::c_void,
                common::HUGE_PAGE_SIZE,
            );

            match bytes_read {
                n if n > 0 => {
                    self.inner_read_buf.truncate(n as usize);

                    if self.inner_partial.is_empty() {
                        std::mem::swap(&mut self.inner_partial, &mut self.inner_read_buf);
                    } else {
                        self.inner_partial.append(&mut self.inner_read_buf);
                    }

                    if let Some(last_newline) = memchr::memrchr(b'\n', &self.inner_partial) {
                        let mut complete = std::mem::take(&mut self.inner_partial);
                        self.inner_partial = complete.split_off(last_newline + 1);
                        complete.truncate(last_newline + 1);

                        self.inner_buf = Some(Arc::new(complete));
                        self.inner_cursor = 0;
                        return Ok(true);
                    }

                    return Err(crate::runtime::Error::parse_error(
                        "load_next_buf",
                        Some("No complete lines found in buffer - file may be corrupt or have extremely long reads")
                    ));
                }
                0 => {
                    if !self.inner_partial.is_empty() {
                        self.inner_buf = Some(Arc::new(std::mem::take(&mut self.inner_partial)));
                        self.inner_cursor = 0;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                _ => {
                    return Err(crate::runtime::Error::parse_error(
                        "bgzf_read",
                        Some(format!("Read error: {}", bytes_read)),
                    ))
                }
            }
        }
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        unsafe {
            if !self.inner_hts_file.is_null() {
                htslib::hts_close(self.inner_hts_file);
            }
        }
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetStreamToken + 'static,
    T::Builder: BascetStreamTokenBuilder<Token = T>,
{
    fn set_reader_threads(self, n_threads: usize) -> Self {
        unsafe {
            htslib::hts_set_threads(self.inner_hts_file, n_threads as i32);
        }
        self
    }

    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error> {
        let mut cell_id: Option<Vec<u8>> = None;
        let mut builder: Option<T::Builder> = None;

        loop {
            if self.inner_buf.is_none() {
                if !self.load_next_buf()? {
                    if let Some(b) = builder.take() {
                        return Ok(Some(b.build()));
                    } else {
                        return Ok(None);
                    }
                }

                if let Some(buf) = &self.inner_buf {
                    if let Some(b) = builder.take() {
                        builder = Some(b.add_underlying(Arc::clone(buf)));
                    }
                }
            }

            let buf = self.inner_buf.as_ref().unwrap();

            if let Some(next_pos) =
                memchr::memchr(common::U8_CHAR_NEWLINE, &buf[self.inner_cursor..])
            {
                let line_start = self.inner_cursor;
                let line_end = self.inner_cursor + next_pos;
                let line = &buf[line_start..line_end];

                if let Ok((id, rp)) = tirp::parse_readpair(line) {
                    match &cell_id {
                        Some(existing_id) if existing_id == id => {
                            if let Some(b) = builder.take() {
                                builder = Some(b.add_seq_slice(rp.r1).add_seq_slice(rp.r2));
                            }
                        }
                        Some(_) => {
                            if let Some(b) = builder.take() {
                                let token = b.build();
                                return Ok(Some(token));
                            }
                        }
                        None => {
                            cell_id = Some(id.to_vec());

                            let new_builder = T::builder()
                                .add_underlying(Arc::clone(buf))
                                .add_cell_id_slice(id)
                                .add_seq_slice(rp.r1)
                                .add_seq_slice(rp.r2);
                            builder = Some(new_builder);
                        }
                    }
                }
                self.inner_cursor = line_end + 1;
            } else {
                self.inner_partial
                    .extend_from_slice(&buf[self.inner_cursor..]);
                self.inner_buf = None;
                self.inner_cursor = 0;
            }
        }
    }
}
