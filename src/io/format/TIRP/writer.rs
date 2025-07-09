use std::{
    io::{BufWriter, Write},
    sync::Arc,
};

use bgzip::{write::BGZFMultiThreadWriter, Compression};

use crate::{
    common::{self, ReadPair},
    io::{BascetFile, BascetWrite},
    log_critical, log_error, log_trace,
};

pub type DefaultWriter =
    Writer<bgzip::write::BGZFMultiThreadWriter<std::io::BufWriter<std::fs::File>>>;

pub struct Writer<W>
where
    W: std::io::Write,
{
    inner: W,
}

impl<W> Writer<W>
where
    W: std::io::Write,
{
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl DefaultWriter {
    pub fn from_file(file: &crate::io::File) -> Self {
        let file = log_critical!(
            file.file_open(),
            "[TIRP Writer] Could not open destination file"
        );

        let buf_writer = BufWriter::new(file);
        let bgzf_writer = BGZFMultiThreadWriter::new(buf_writer, Compression::default());

        Self::new(bgzf_writer)
    }
}

impl BascetWrite for DefaultWriter {
    fn write_cell(&mut self, cell_id: &str, reads: &Arc<Vec<ReadPair>>) {
        log_trace!("[TIRP Writer] Writing"; "cell" => ?cell_id);

        for rp in reads.iter() {
            _ = self.inner.write_all(cell_id.as_bytes());
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);

            _ = self.inner.write_all(&[common::U8_CHAR_1]);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);

            _ = self.inner.write_all(&[common::U8_CHAR_1]);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);

            _ = self.inner.write_all(&rp.r1);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);
            _ = self.inner.write_all(&rp.r2);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);
            _ = self.inner.write_all(&rp.q1);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);
            _ = self.inner.write_all(&rp.q2);
            _ = self.inner.write_all(&[common::U8_CHAR_TAB]);
            _ = self.inner.write_all(&rp.umi);
            _ = self.inner.write_all(&[common::U8_CHAR_NEWLINE]);
        }
    }
}
