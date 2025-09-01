use std::io::Write;

use itertools::izip;

use crate::io::traits::{
    BascetCellWrite, CellIdAccessor, CellPairedQualitiesAccessor, CellPairedReadsAccessor,
    CellUmisAccessor,
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

impl<
        W,
        C: CellIdAccessor + CellPairedReadsAccessor + CellPairedQualitiesAccessor + CellUmisAccessor,
    > BascetCellWrite<W, C> for Writer<W>
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
    fn write_cell(&mut self, cell: &C) -> Result<(), crate::runtime::Error> {
        let id = cell.get_id();
        if let Some(ref mut writer) = self.inner {
            let reads = cell.get_vec_paired_reads();
            let quals = cell.get_vec_paired_qualities();
            let umis = cell.get_vec_umis();

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
                writer.write_all(&[crate::common::U8_CHAR_NEWLINE])?;
            }
        }

        Ok(())
    }
}
