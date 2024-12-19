#[derive(Clone, Copy)]
pub struct IO<'a> {
    pub path_in: &'a std::path::Path,
}

#[derive(Clone, Copy)]
pub struct Runtime {
    pub kmer_size: usize,
    pub features_nmin: usize,
    pub features_nmax: usize,
    pub codec: crate::utils::KMERCodec,
    pub seed: u64,
}

#[derive(Clone, Copy)]
pub struct Threading<'a> {
    pub threads_io: usize,
    pub threads_work: usize,
    pub thread_pool: &'a threadpool::ThreadPool,
    pub thread_buffer_size: usize,
    pub thread_states: &'a Vec<std::sync::Arc<super::threading::DefaultThreadState>>,
}
