pub struct IO {
    pub path_tmp: std::path::PathBuf,

    pub path_forward: std::path::PathBuf,
    pub path_reverse: std::path::PathBuf,
    pub path_output_complete: std::path::PathBuf,
    pub path_output_incomplete: std::path::PathBuf,

    pub barcode_file: Option<std::path::PathBuf>,
    pub sort: bool,
}

pub struct Runtime {
    //pub kmer_size: usize,
}
pub struct Threading {
    pub threads_work: usize,   
    //pub threads_read: usize,
    //pub threads_write: usize,
    //pub thread_pool: &'a threadpool::ThreadPool,
}
