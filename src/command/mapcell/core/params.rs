#[derive(Clone,Debug)]
pub struct IO {
    pub path_in: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
    pub path_script: std::path::PathBuf,

    //How many threads are reading the input zip file?
    pub threads_read: usize,

    //How many runners are there? each runner writes it's own zip file output, to be merged later
    pub threads_write: usize,

    //How many threads should the invoked script use? Passed on as a parameter. Not all commands will support this
    pub threads_work: usize,


    pub keep_files: bool
}
