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
}
