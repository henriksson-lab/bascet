use std::io::{Seek, Write};

use bascet_core::ArenaSlice;

use crate::{
    codec::bbgz::consts::{MAX_SIZEOF_BLOCKusize, SIZEOF_MARKER_DEFLATE_ALIGN_BYTESusize},
    BBGZCompressionJob, BBGZHeader, BBGZTrailer, BBGZWriter,
};

pub struct BBGZWriteBlock<'a> {
    inner_compressor: &'a mut BBGZWriter,
    inner_header: BBGZHeader,
    inner_raw: ArenaSlice<u8>,
    inner_raw_bytes_written: usize,
}

impl<'a> BBGZWriteBlock<'a> {
    pub fn new(compressor: &'a mut BBGZWriter, header: BBGZHeader) -> Self {
        let raw = compressor.alloc_raw();

        BBGZWriteBlock::<'a> {
            inner_compressor: compressor,
            inner_header: header,
            inner_raw: raw,
            inner_raw_bytes_written: 0,
        }
    }

    pub fn reserve(&mut self, size: usize) {
        let hsize = self.inner_raw_bytes_written
            + self.inner_header.size()
            + SIZEOF_MARKER_DEFLATE_ALIGN_BYTESusize
            + BBGZTrailer::SSIZE;

        if size + hsize > MAX_SIZEOF_BLOCKusize && self.inner_raw_bytes_written > 0 {
            let new_raw = self.inner_compressor.alloc_raw();
            let mut send_raw = std::mem::replace(&mut self.inner_raw, new_raw);
            unsafe {
                send_raw = send_raw.truncate(self.inner_raw_bytes_written);
                let send_job = BBGZCompressionJob {
                    header: self.inner_header.clone(),
                    raw: send_raw,
                };
                self.inner_compressor.submit_compress(send_job);
            }
            self.inner_raw_bytes_written = 0;
        }
    }
}

impl<'a> std::io::Write for BBGZWriteBlock<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // NOTE hsize assumes the entire data is written as one incompressible block
        let hsize = self.inner_raw_bytes_written
            + self.inner_header.size()
            + SIZEOF_MARKER_DEFLATE_ALIGN_BYTESusize
            + BBGZTrailer::SSIZE;

        if buf.len() + hsize > MAX_SIZEOF_BLOCKusize {
            let new_raw = self.inner_compressor.alloc_raw();
            let mut send_raw = std::mem::replace(&mut self.inner_raw, new_raw);
            unsafe {
                send_raw = send_raw.truncate(self.inner_raw_bytes_written);
                let send_job = BBGZCompressionJob {
                    header: self.inner_header.clone(),
                    raw: send_raw,
                };
                self.inner_compressor.submit_compress(send_job);
            }
            self.inner_raw_bytes_written = 0;
        }

        let raw_buf = self.inner_raw.as_mut_slice();
        unsafe {
            let raw_buf_ptr = raw_buf.as_mut_ptr().add(self.inner_raw_bytes_written);
            std::ptr::copy_nonoverlapping(buf.as_ptr(), raw_buf_ptr, buf.len());
        }
        self.inner_raw_bytes_written += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.inner_raw_bytes_written > 0 {
            let new_raw = self.inner_compressor.alloc_raw();
            let mut send_raw = std::mem::replace(&mut self.inner_raw, new_raw);
            unsafe {
                send_raw = send_raw.truncate(self.inner_raw_bytes_written);
                let send_job = BBGZCompressionJob {
                    header: self.inner_header.clone(),
                    raw: send_raw,
                };
                self.inner_compressor.submit_compress(send_job);
            }
            self.inner_raw_bytes_written = 0;
        }

        Ok(())
    }
}
