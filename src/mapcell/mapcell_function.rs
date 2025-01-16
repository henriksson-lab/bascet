use core::fmt;
use std::{fmt::Debug, path::PathBuf};


#[derive(Clone,Debug,Eq,PartialEq,Copy)]
pub enum MissingFileMode {
    Ignore,
    Skip,
    Fail
}
impl fmt::Display for MissingFileMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}




#[derive(Clone,Debug,Eq,PartialEq,Copy )]
pub enum CompressionMode {
    Default,
    Uncompressed
}
impl fmt::Display for CompressionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}



pub trait MapCellFunction where Self: Sync+Send+Debug { 

    fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        num_threads: usize
    ) -> anyhow::Result<(bool, String)>;

    fn get_missing_file_mode(&self) -> MissingFileMode;

    fn get_compression_mode(&self, fname: &str) -> CompressionMode;

    fn get_expect_files(&self) -> Vec<String>;

}




pub fn parse_compression_mode(s: &str) -> anyhow::Result<CompressionMode> {
    match s {
        "default" => Ok(CompressionMode::Default),
        "uncompressed" => Ok(CompressionMode::Uncompressed),
        _ => anyhow::bail!("Cannot parse compression mode")
    }
}

pub fn parse_missing_file_mode(s: &str) -> anyhow::Result<MissingFileMode> {
    match s {
        "ignore" => Ok(MissingFileMode::Ignore),
        "skip" => Ok(MissingFileMode::Skip),
        "fail" => Ok(MissingFileMode::Fail),
        _ => anyhow::bail!("Cannot parse missing file mode")
    }
}



