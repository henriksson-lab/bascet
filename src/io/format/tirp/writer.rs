use std::io::Write;

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
                // Build the entire line first to check for double tabs
                let mut line = Vec::new();
                line.extend_from_slice(id);
                line.push(crate::common::U8_CHAR_TAB);
                line.push(crate::common::U8_CHAR_1);
                line.push(crate::common::U8_CHAR_TAB);
                line.push(crate::common::U8_CHAR_1);
                line.push(crate::common::U8_CHAR_TAB);
                line.extend_from_slice(r1);
                line.push(crate::common::U8_CHAR_TAB);
                line.extend_from_slice(r2);
                line.push(crate::common::U8_CHAR_TAB);
                line.extend_from_slice(q1);
                line.push(crate::common::U8_CHAR_TAB);
                line.extend_from_slice(q2);
                line.push(crate::common::U8_CHAR_TAB);
                line.extend_from_slice(umi);
                line.push(crate::common::U8_CHAR_NEWLINE);

                // Check for double tabs before writing
                let line_str = String::from_utf8_lossy(&line);
                if line_str.contains("\t\t") {
                    let error_msg = format!(
                        "ERROR: About to write line with double tabs!\n\
                         Cell ID: {:?}\n\
                         r1 length: {}, r2 length: {}, q1 length: {}, q2 length: {}, umi length: {}\n\
                         r1 empty: {}, r2 empty: {}, q1 empty: {}, q2 empty: {}, umi empty: {}\n\
                         Raw field contents:\n\
                         r1: {:?}\n\
                         r2: {:?}\n\
                         q1: {:?}\n\
                         q2: {:?}\n\
                         umi: {:?}\n\
                         Full line: {:?}",
                        String::from_utf8_lossy(id),
                        r1.len(), r2.len(), q1.len(), q2.len(), umi.len(),
                        r1.is_empty(), r2.is_empty(), q1.is_empty(), q2.is_empty(), umi.is_empty(),
                        String::from_utf8_lossy(r1),
                        String::from_utf8_lossy(r2), 
                        String::from_utf8_lossy(q1),
                        String::from_utf8_lossy(q2),
                        String::from_utf8_lossy(umi),
                        line_str
                    );
                    panic!("{}", error_msg);
                }

                // Write the validated line
                _ = writer.write_all(&line);
            }
        }

        Ok(())
    }
}
