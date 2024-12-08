pub struct IO<'a> {
    pub path_in: &'a std::path::PathBuf,
    pub path_temp: &'a std::path::PathBuf,
    pub path_out: &'a mut clio::Output,
}

pub struct Runtime {
    pub min_reads: usize,
}

pub struct Threading<'a> {
    pub threads_read: u32,
    pub threads_write: usize,

    pub thread_pool_read: &'a rust_htslib::tpool::ThreadPool,
    pub thread_pool_write: &'a threadpool::ThreadPool,
}
