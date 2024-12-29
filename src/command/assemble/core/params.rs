pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
}

pub struct Runtime {}

pub struct Threading {
    pub threads_read: usize,
    pub threads_write: usize,
    pub threads_work: usize,
}
