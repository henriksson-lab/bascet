use std::io::{BufWriter, Write};

use bgzip::{write::BGZFMultiThreadWriter, Compression};

use crate::{
    common::{self, ReadPair},
    io::{format::tirp, BascetFile, BascetStreamToken, BascetWriter},
    log_critical, log_trace,
};

pub struct Writer<W>
where
    W: std::io::Write,
{
    inner: Option<W>,
}

impl<W> Writer<W>
where
    W: std::io::Write,
{
    pub fn new() -> Result<Self, crate::runtime::Error> {
        Ok(Self { inner: None })
    }
}

impl<W> BascetWriter<W> for Writer<W>
where
    W: std::io::Write,
{
    fn set_writer(mut self, writer: W) -> Self {
        self.inner = Some(writer);
        self
    }

    fn write_cell<T>(&mut self, token: T)
    where
        T: BascetStreamToken,
    {
        // log_trace!("[TIRP Writer] Writing"; "cell" => ?cell_id);

        if let Some(ref mut writer) = self.inner {
            // for rp in reads.iter() {
            //     _ = writer.write_all(cell_id.as_bytes());
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);

            //     _ = writer.write_all(&[common::U8_CHAR_1]);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);

            //     _ = writer.write_all(&[common::U8_CHAR_1]);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);

            //     _ = writer.write_all(&rp.r1);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);
            //     _ = writer.write_all(&rp.r2);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);
            //     _ = writer.write_all(&rp.q1);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);
            //     _ = writer.write_all(&rp.q2);
            //     _ = writer.write_all(&[common::U8_CHAR_TAB]);
            //     _ = writer.write_all(&rp.umi);
            //     _ = writer.write_all(&[common::U8_CHAR_NEWLINE]);
            // }
        }
    }
}
