use bascet_core::{Collection, Get, GetBytes, Id, SequencePair};
use itertools::Itertools;

use crate::{common, io::traits::BascetWrite};

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

    fn write_hist<H, K, V>(&mut self, counts: H) -> Result<(), crate::runtime::Error>
    where
        H: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: std::fmt::Display,
    {
        if let Some(ref mut writer) = self.inner {
            let output = counts
                .into_iter()
                .map(|(key, value)| format!("{}\t{}", String::from_utf8_lossy(key.as_ref()), value))
                .join("\n");

            writer.write_all(output.as_bytes())?;
        }

        Ok(())
    }

    fn write_countsketch<C>(
        &mut self,
        cell: &C,
        countsketch: &crate::kmer::kmc_counter::CountSketch,
    ) -> Result<(), crate::runtime::Error>
    where
        C: crate::io::traits::BascetCell,
    {
        if let Some(ref mut writer) = self.inner {
            let id = match cell.get_cell() {
                Some(id) => id,
                None => {
                    return Err(crate::runtime::Error::parse_error(
                        "countsketch writer",
                        Some("Missing cell ID"),
                    ))
                }
            };
            let n = match cell.get_reads() {
                Some(reads) => reads.len().to_string().into_bytes(),
                None => b"0".to_vec(),
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

    fn write_comp_countsketch<C>(&mut self, comp: &C, countsketch: &crate::kmer::kmc_counter::CountSketch) -> Result<(), crate::runtime::Error>
    where
        C: bascet_core::Composite
            + Get<Id>
            + Get<SequencePair>,
        <C as Get<Id>>::Value: AsRef<[u8]>,
        <C as Get<SequencePair>>::Value: Collection,
    {
        if let Some(ref mut writer) = self.inner {
            let n = comp
                .get_ref::<SequencePair>()
                .len()
                .to_string()
                .into_bytes();

            // NOTE: in theory these can fail writing, however, for performance reasons, this is unchecked
            let _ = writer.write_all(comp.get_bytes::<Id>());
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
