use std::sync::Arc;

use rust_htslib::tbx;

use crate::{
    common::{self, ReadPair},
    io::{BascetFile, BascetRead},
    log_critical, log_error, log_warning,
};

pub type DefaultReader = Reader<rust_htslib::tbx::Reader>;

pub struct Reader<R> {
    inner: R,
}

impl<R> Reader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl DefaultReader {
    pub fn from_file(file: &crate::io::File) -> Self {
        let tabix_reader = log_critical!(
            tbx::Reader::from_path(file.file_path()),
            "[TIRP Reader] Failed to initialise reader"
        );

        Self::new(tabix_reader)
    }

    pub fn set_threads(&mut self, n_threads: usize) {
        let _ = log_error!(
            self.inner.set_threads(n_threads),
            "[TIRP Reader] Failed to set n_threads"
        );
    }
}

impl BascetRead for DefaultReader {
    fn has_cell(&self, cell: &str) -> bool {
        self.inner.seqnames().contains(&String::from(cell))
    }

    fn get_cells(&self) -> Vec<String> {
        self.inner.seqnames()
    }

    fn read_cell(&mut self, cell_id: &str) -> Arc<Vec<ReadPair>> {
        //Get tabix id for the cell
        let tid = match log_error!(
            self.inner.tid(&cell_id),
            "[TIRP Reader] Failed to find tabix ID"; "cell" => ?cell_id
        ) {
            Ok(tid) => tid,
            Err(_) => return Arc::new(Vec::<ReadPair>::new()),
        };

        // Seek to reads
        // NOTE: using simulated data [0, 2) is the only range. This might NOT be true for real world data!
        let _ = match log_error!(
            self.inner.fetch(tid, 0, 2),
            "[TIRP Reader] Failed to fetch data"; "cell" => ?cell_id, "tid" => ?tid
        ) {
            Ok(_) => {}
            Err(_) => return Arc::new(Vec::<ReadPair>::new()),
        };

        //Get all reads
        let mut reads: Vec<ReadPair> = Vec::new();
        for record in tbx::Read::records(&mut self.inner) {
            let record = match log_error!(record, "Failed to get line") {
                Ok(record) => record,
                Err(_) => continue,
            };

            let parts: Vec<&[u8]> = record.split(|&b| b == common::U8_CHAR_TAB).collect();
            let r1 = parts[3];
            let r2 = parts[4];
            let q1 = parts[5];
            let q2 = parts[6];
            let umi = parts[7];

            if r1.len() != q1.len() {
                log_warning!(
                    "[TIRP Reader] Parsing: r1 and q1 have different lengths";
                    "cell" => ?cell_id,
                    "r1" => ?parts[3],
                    "q1" => ?parts[5]
                );
                continue;
            }
            if r2.len() != q2.len() {
                log_warning!(
                    "[TIRP Reader] Parsing: r2 and q2 have different lengths";
                    "cell" => ?cell_id,
                    "r2" => ?parts[4],
                    "q2" => ?parts[6]
                );
                continue;
            }

            let rp = ReadPair {
                r1: r1.to_vec(),
                r2: r2.to_vec(),
                q1: q1.to_vec(),
                q2: q2.to_vec(),
                umi: umi.to_vec(),
            };

            reads.push(rp);
        }

        Arc::new(reads)
    }
}
