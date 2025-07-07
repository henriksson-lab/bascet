pub trait BascetFile {
    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<()>;
    fn file_path(&self) -> &std::path::Path;
    fn file_open(&self) -> anyhow::Result<std::fs::File>;
}
pub trait BascetRead {
    // Check if a cell exists.
    fn has_cell(&self, cell: &str) -> bool;

    // List all cell IDs.
    fn list_cells(&self) -> Vec<String>;

    // Retrieve all records for a cell.
    fn read_cell(&mut self, cell: &str)
        -> anyhow::Result<std::sync::Arc<Vec<crate::common::ReadPair>>>;
}
pub trait BascetWrite {}

pub trait BascetStream {
    fn next(&mut self) -> anyhow::Result<Option<crate::io::stream::Cell>>;
}
pub trait BascetExtract {}
