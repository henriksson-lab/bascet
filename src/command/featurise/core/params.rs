pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_idx: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
}

pub struct Runtime {
    pub kmer_size: usize,
}

pub struct Threading<'a> {
    pub threads_read: usize,
    pub threads_write: usize,
    pub thread_pool: &'a threadpool::ThreadPool,
}
