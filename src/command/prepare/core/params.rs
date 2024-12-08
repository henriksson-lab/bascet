pub struct IO<'a> {
    pub file_in: &'a std::path::PathBuf,
    pub path_out: &'a mut clio::Output,
}

pub struct Runtime {}

pub struct Threading {
    pub threads_read: u32,
    pub threads_write: usize,
}
