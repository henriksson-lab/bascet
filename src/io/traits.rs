pub trait BascetFile {
    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> Result<(), impl std::error::Error>;
    fn file_path(&self) -> &std::path::Path;
    fn file_open(&self) -> anyhow::Result<std::fs::File>;
}
pub trait BascetRead {
    // Check if a cell exists.
    fn has_cell(&self, cell: &str) -> bool;

    // List all cell IDs.
    fn get_cells(&self) -> Vec<String>;

    // Retrieve all records for a cell.
    fn read_cell(&mut self, cell: &str) -> Vec<crate::common::ReadPair>;
}
pub trait BascetWrite {
    fn write_cell(&mut self, cell_id: &str, reads: &Vec<crate::common::ReadPair>);
}

pub trait BascetStream: Sized {
    type Token: BascetStreamToken;

    fn set_reader_threads(&mut self, n_threads: usize);
    fn set_worker_threads(&mut self, n_threads: usize);

    fn next(&mut self) -> anyhow::Result<Option<Self::Token>>;

    fn par_map<F, R, S>(&mut self, state: S, f: F) -> Vec<R>
    where
        F: Fn(Self::Token, &mut S) -> R + Send + Sync + 'static,
        R: Send + 'static,
        S: Clone + Send + 'static;
}

pub trait BascetStreamToken {}
pub trait BascetExtract {}
