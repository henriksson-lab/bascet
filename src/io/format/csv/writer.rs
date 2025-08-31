use crate::{common, io::traits::{BascetWrite, CellIdAccessor, CellPairedReadAccessor}};

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

    fn write_countsketch<C>(
        &mut self,
        cell: &C,
        countsketch: &crate::kmer::kmc_counter::CountSketch,
    ) -> Result<(), crate::runtime::Error>
    where
        C: CellIdAccessor,
    {
        if let Some(ref mut writer) = self.inner {
            let id = match cell.get_id() {
                Some(id) => id,
                None => {
                    return Err(crate::runtime::Error::parse_error(
                        "countsketch writer",
                        Some("Missing cell ID"),
                    ))
                }
            };
            // TODO: continue
            let n = match countsketch.() {
                Some(n) => n.to_string().into_bytes(),
                None => vec![b"0"; 1],
            };

            // NOTE: in theory these can fail writing, however, for performance reasons, this is unchecked
            let _ = writer.write_all(id);
            let _ = writer.write_all(&[common::U8_CHAR_TAB]);
            let _ = writer.write_all(&n);

            for value in countsketch.sketch.iter() {
                let _ = writer.write_all(&[common::U8_CHAR_TAB]);
                let _ = writer.write_all(value.to_string().as_bytes());
            }
            let _ = writer.write_all(&[common::U8_CHAR_NEWLINE]);
        }

        Ok(())
    }
}
