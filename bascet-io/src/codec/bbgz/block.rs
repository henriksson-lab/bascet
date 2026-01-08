use bascet_core::ArenaSlice;

use crate::{BBGZHeader, BBGZWriter};

pub struct BBGZBlock<'a, W> {
    inner_writer: &'a mut BBGZWriter<W>,
    inner_header: BBGZHeader,
    inner_raw: ArenaSlice<u8>,
    inner_raw_offset: usize,
    inner_compressed: ArenaSlice<u8>,
    inner_compressed_offset: usize,
}

impl<'a, W> std::io::Write for BBGZBlock<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let raw_buf = self.inner_raw.as_mut_slice();
        let raw_offset = self.inner_raw_offset;
        let raw_len = raw_buf.len();
        let remaining = raw_len - raw_offset;

        if buf.len() > remaining {
            // TODO: Send current arena slice to compressor and request a new one
            todo!();
        }

        unsafe {
            let dest_ptr = raw_buf.as_mut_ptr().add(raw_offset);
            std::ptr::copy_nonoverlapping(buf.as_ptr(), dest_ptr, buf.len());
        }
        self.inner_raw_offset += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}
