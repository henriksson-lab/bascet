use std::fs::read_to_string;
use std::path::PathBuf;

pub fn read_cell_list_file(filename: &PathBuf) -> Vec<String> {
    read_to_string(filename)
        .expect("Failed to read file with list of cells")
        .lines()
        .map(String::from)
        .collect()
}
