pub enum Cell {
    Memory { data: Vec<u8> },
    Disk { path: std::path::PathBuf },
}
