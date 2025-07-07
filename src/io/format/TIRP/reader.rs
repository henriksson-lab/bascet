use std::sync::Arc;

use rust_htslib::tbx;

use crate::{
    common::ReadPair,
    io::{BascetFile, BascetRead},
};

pub type TirpDefaultReader = TirpReader<rust_htslib::tbx::Reader>;

pub struct TirpReader<R> {
    inner: R,
}

impl<R> TirpReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl TirpDefaultReader {
    pub fn from_file(file: &super::File) -> anyhow::Result<Self> {
        let tabix_reader = tbx::Reader::from_path(file.file_path())?;

        Ok(Self::new(tabix_reader))
    }
}

impl BascetRead for TirpDefaultReader {
    fn has_cell(&self, cell: &str) -> bool {
        self.inner.seqnames().contains(&String::from(cell))
    }

    fn list_cells(&self) -> Vec<String> {
        self.inner.seqnames()
    }

    fn read_cell(&mut self, cell_id: &str) -> anyhow::Result<Arc<Vec<ReadPair>>> {
        //Get tabix id for the cell
        let tid = self
            .inner
            .tid(&cell_id)
            .expect("Could not tabix ID for cell");

        // Seek to the reads (all of them)
        // NOTE: using simulated data 1, 1 is the only range. This might NOT be true for real world data!
        self.inner
            .fetch(tid, 1, 1) //hopefully ok!
            .expect("could not find reads");

        //Get all reads
        let mut reads: Vec<ReadPair> = Vec::new();
        for line in tbx::Read::records(&mut self.inner) {
            let line = line.expect("Failed to get one TIRP line");
            let parts: Vec<&[u8]> = line.split(|&b| b == b'\t').collect();

            let rp = ReadPair {
                r1: parts[3].to_vec(),
                r2: parts[4].to_vec(),
                q1: Some(parts[5].to_vec()),
                q2: Some(parts[6].to_vec()),
                umi: parts[7].to_vec(),
            };

            reads.push(rp);
        }
        Ok(Arc::new(reads))
    }
}
