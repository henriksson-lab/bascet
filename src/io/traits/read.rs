pub trait BascetCellRead {
    // Check if a cell exists.
    fn has_cell(&self, cell: &str) -> bool;

    // List all cell IDs.
    fn get_cells(&self) -> Vec<String>;

    // Retrieve all records for a cell.
    fn read_cell(&mut self, cell: &str) -> Vec<crate::common::ReadPair>;
}
