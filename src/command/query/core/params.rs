pub struct IO<'a> {
    pub file_in: &'a std::fs::File,
    pub path_out: &'a mut clio::Output,
}

pub struct Runtime {
    pub kmer_size: usize,
    pub ovlp_size: usize,
    pub features_nmin: usize,
    pub features_nmax: usize,
    pub codec: crate::utils::KMERCodec,
    pub seed: u64,
}

pub struct Threading<'a> {
    pub threads_io: usize,
    pub threads_work: usize,
    pub thread_buffer_size: usize,
    pub thread_pool: &'a threadpool::ThreadPool,
    pub thread_states: &'a Vec<std::sync::Arc<super::threading::DefaultThreadState>>,
}
