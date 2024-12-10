use super::threading::ThreadState;

pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
}

pub struct Runtime {
    pub assemble: bool,
    pub cleanup: bool,
    pub min_reads: usize,
}

pub struct Threading<'a, W>
where
    W: std::io::Seek + std::io::Write,
{
    pub threads_write: usize,
    pub threads_read: u32,
    pub thread_pool_write: &'a threadpool::ThreadPool,
    pub thread_pool_read: &'a rust_htslib::tpool::ThreadPool,
    pub thread_states: &'a Vec<std::sync::Arc<ThreadState<W>>>,
}
