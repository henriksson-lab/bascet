use std::sync::Arc;

pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
}

pub struct Runtime {
    pub min_reads_per_cell: usize,
}

pub struct Threading {
    pub threads_work: usize,
    pub threads_read: u32,
}
