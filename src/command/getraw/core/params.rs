pub struct IO {
    pub path_tmp: std::path::PathBuf,

    pub path_forward: std::path::PathBuf,
    pub path_reverse: std::path::PathBuf,
    pub path_output_complete: std::path::PathBuf,
    pub path_output_incomplete: std::path::PathBuf,

    pub barcode_file: Option<std::path::PathBuf>,
    pub sort: bool,

    pub threads_work: usize,   

}

