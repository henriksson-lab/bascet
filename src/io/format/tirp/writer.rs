use std::io::{BufWriter, Write};

use bgzip::{write::BGZFMultiThreadWriter, Compression};
use itertools::izip;

use crate::io::traits::{BascetCell, BascetWrite};

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

impl<W> BascetWrite<W> for Writer<W>
where
    W: std::io::Write,
{
    fn set_writer(mut self, writer: W) -> Self {
        self.inner = Some(writer);
        self
    }

    fn write_cell<T>(&mut self, token: T)
    where
        T: BascetCell,
    {
        let cell = token.get_cell().unwrap();
        if let Some(ref mut writer) = self.inner {
            let reads = token.get_reads().unwrap_or(&[]);
            let quals = token.get_qualities().unwrap_or(&[]);
            let umis = token.get_umis().unwrap_or(&[]);

            for ((r1, r2), (q1, q2), umi) in izip!(reads, quals, umis) {
                _ = writer.write_all(cell);
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
    }
}
