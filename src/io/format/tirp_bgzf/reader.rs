use rust_htslib::tbx::{self, Read};

use crate::{
    common::ReadPair,
    io::format::tirp,
    io::{BascetFile, BascetRead},
    log_critical, log_error,
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
    pub fn from_tirp(file: &tirp::File) -> Self {
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

    fn read_cell(&mut self, cell_id: &str) -> Vec<ReadPair> {
        todo!();

        //Get tabix id for the cell
        let tid = match log_error!(
            self.inner.tid(&cell_id),
            "[TIRP Reader] Failed to find tabix ID"; "cell" => ?cell_id
        ) {
            Ok(tid) => tid,
            Err(_) => return Vec::<ReadPair>::new(),
        };

        // Seek to reads
        // NOTE: using simulated data [0, 2) is the only range. This might NOT be true for real world data!
        let _ = match log_error!(
            self.inner.fetch(tid, 0, 2),
            "[TIRP Reader] Failed to fetch data"; "cell" => ?cell_id, "tid" => ?tid
        ) {
            Ok(_) => {}
            Err(_) => return Vec::<ReadPair>::new(),
        };

        //Get all reads
        let mut reads: Vec<ReadPair> = Vec::new();
        let mut record = Vec::new();

        loop {
            match self.inner.read(&mut record) {
                Ok(true) => {
                    match tirp::parse_readpair(&record) {
                        Ok(rp) => reads.push(rp),
                        Err(e) => {
                            log::error!("[TIRP Reader] Failed to parse readpair: {:?}", e);
                        }
                    }
                    record.clear();
                }
                Ok(false) => break, // EOF
                Err(e) => {
                    log::error!("[TIRP Reader] Error reading record: {:?}", e);
                    break; // or return Err(e) if you want to propagate the error
                }
            }
        }

        reads
    }
}
