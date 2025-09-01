use crate::{
    common,
    io::traits::{BascetCellWrite, CellIdAccessor},
};

pub struct Writer<W>
where
    W: std::io::Write,
{
    pub inner: Option<W>,
}

impl<W> Writer<W>
where
    W: std::io::Write,
{
    pub fn new() -> Result<Self, crate::runtime::Error> {
        Ok(Self { inner: None })
    }
}

impl<W, C: CellIdAccessor> BascetCellWrite<W, C> for Writer<W>
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

    fn write_countsketch(
        &mut self,
        cell: &C,
        countsketch: &crate::kmer::kmc_counter::CountSketch,
    ) -> Result<(), crate::runtime::Error> {
        if let Some(ref mut writer) = self.inner {
            writer.write_all(cell.get_id())?;
            writer.write_all(&[common::U8_CHAR_TAB])?;
            writer.write_all(&countsketch.total.to_string().as_bytes())?;

            for value in countsketch.sketch.iter() {
                writer.write_all(&[common::U8_CHAR_TAB])?;
                writer.write_all(value.to_string().as_bytes())?;
            }
            writer.write_all(&[common::U8_CHAR_NEWLINE])?;
        }

        Ok(())
    }
}
