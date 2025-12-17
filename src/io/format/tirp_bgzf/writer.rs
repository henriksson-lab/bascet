use crate::io::traits::{BascetCell, BascetWrite};
use bascet_core::{Collection, GetBytes, Id, QualityPair, SequencePair, Umi};
use itertools::izip;

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
            let metadata = cell.get_metadata().unwrap_or(&[]);

            for ((r1, r2), (q1, q2), umi) in izip!(reads, quals, umis) {
                // Write each component directly to avoid vector allocation
                writer.write_all(id)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(&[crate::common::U8_CHAR_1])?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(&[crate::common::U8_CHAR_1])?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(r1)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(r2)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(q1)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(q2)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(umi)?;
                writer.write_all(&[crate::common::U8_CHAR_TAB])?;
                writer.write_all(metadata)?;
                writer.write_all(&[crate::common::U8_CHAR_NEWLINE])?;
            }
        }

        Ok(())
    }
}
