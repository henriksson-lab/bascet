pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
}
pub struct Runtime {
    pub kmer_size: usize,
    pub features_nmin: usize,
    pub features_nmax: usize,
    pub codec: crate::utils::KMERCodec,
    pub seed: u64,
}

pub struct Threading {
    pub threads_read: usize,
    pub threads_work: usize,
    pub threads_buffer_size: usize,
}
