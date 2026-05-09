use std::fs::File;
use std::io;
use std::path::Path;

use noodles::{
    core::Position,
    csi::binning_index::index::{
        header::Builder as IndexHeaderBuilder, reference_sequence::bin::Chunk,
    },
    tabix,
};

pub struct BedTabixIndexer {
    inner: tabix::index::Indexer,
}

impl BedTabixIndexer {
    pub fn new() -> Self {
        let mut inner = tabix::index::Indexer::default();
        inner.set_header(IndexHeaderBuilder::bed().build());

        Self { inner }
    }

    pub fn add_record(
        &mut self,
        reference_sequence_name: &str,
        bed_start: usize,
        bed_end: usize,
        chunk: Chunk,
    ) -> io::Result<()> {
        let start = Position::try_from(bed_start + 1)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let end = Position::try_from(bed_end)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        self.inner
            .add_record(reference_sequence_name, start, end, chunk)
    }

    pub fn write_to_path(self, path: impl AsRef<Path>) -> io::Result<()> {
        let index = self.inner.build();
        let file = File::create(path)?;
        let mut writer = tabix::io::Writer::new(file);
        writer.write_index(&index)
    }
}
