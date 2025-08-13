use std::io::{BufWriter, Write};

use bgzip::{write::BGZFMultiThreadWriter, Compression};
<<<<<<< HEAD
use itertools::izip;

use crate::io::traits::{BascetCell, BascetWrite};
=======

use crate::{
    common::{self, ReadPair},
    io::format::tirp,
    io::{BascetFile, BascetWrite},
    log_critical, log_trace,
};

pub type DefaultWriter =
    Writer<bgzip::write::BGZFMultiThreadWriter<std::io::BufWriter<std::fs::File>>>;
>>>>>>> main

pub struct Writer<W>
where
    W: std::io::Write,
{
<<<<<<< HEAD
    inner: Option<W>,
=======
    inner: W,
>>>>>>> main
}

impl<W> Writer<W>
where
    W: std::io::Write,
{
<<<<<<< HEAD
    pub fn new() -> Result<Self, crate::runtime::Error> {
        Ok(Self { inner: None })
    }
}

impl<W> BascetWrite<W> for Writer<W>
where
    W: std::io::Write,
{
    fn get_writer(self) -> Option<W> {
        self.inner
    }
    fn set_writer(mut self, writer: W) -> Self {
        self.inner = Some(writer);
        self
    }

    #[inline(always)]
    fn write_cell<C>(&mut self, cell: &C) -> Result<(), crate::runtime::Error>
    where
        C: BascetCell,
    {
        let id = cell.get_cell().unwrap();
        if let Some(ref mut writer) = self.inner {
            let reads = cell.get_reads().unwrap_or(&[]);
            let quals = cell.get_qualities().unwrap_or(&[]);
            let umis = cell.get_umis().unwrap_or(&[]);

            for ((r1, r2), (q1, q2), umi) in izip!(reads, quals, umis) {
                _ = writer.write_all(id);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);

                _ = writer.write_all(&[crate::common::U8_CHAR_1]);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);

                _ = writer.write_all(&[crate::common::U8_CHAR_1]);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);

                _ = writer.write_all(r1);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);
                _ = writer.write_all(r2);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);
                _ = writer.write_all(q1);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);
                _ = writer.write_all(q2);
                _ = writer.write_all(&[crate::common::U8_CHAR_TAB]);
                _ = writer.write_all(umi);
                _ = writer.write_all(&[crate::common::U8_CHAR_NEWLINE]);
            }
        }

        Ok(())
=======
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl DefaultWriter {
    pub fn from_tirp(file: &tirp::File) -> Self {
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
    fn write_cell(&mut self, cell_id: &str, reads: &Vec<ReadPair>) {
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
>>>>>>> main
    }
}
