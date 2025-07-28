use std::sync::{Arc, RwLock};

use crate::command::countsketch::AutoStream;
use crate::io::support::*;
use enum_dispatch::enum_dispatch;

pub trait BascetFile {
    const VALID_EXT: Option<&'static str>;

    fn file_path(&self) -> &std::path::Path;
    fn file_open(&self) -> anyhow::Result<std::fs::File>;

    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> Result<(), crate::io::format::Error> {
        let fpath = path.as_ref();

        // 1. File exists and is a regular file
        if !fpath.exists() {
            return Err(crate::io::format::Error::FileNotFound {
                path: fpath.to_path_buf(),
            });
        } else if !fpath.is_file() {
            return Err(crate::io::format::Error::FileNotValid {
                path: fpath.to_path_buf(),
                msg: Some("directory found instead".into()),
            });
        }

        // 2. File has the correct extension
        let fext = fpath.extension().and_then(|e| e.to_str());
        if fext != Self::VALID_EXT {
            return Err(crate::io::format::Error::FileNotValid {
                path: fpath.to_path_buf(),
                msg: Some(
                    format!(
                        "file extension is not {}",
                        Self::VALID_EXT.unwrap_or("None")
                    )
                    .into(),
                ),
            });
        }

        // 3. File is not empty
        let meta = match std::fs::metadata(&fpath) {
            Ok(m) => m,
            Err(_) => {
                return Err(crate::io::format::Error::FileNotValid {
                    path: fpath.to_path_buf(),
                    msg: Some("metadata could not be fetched".into()),
                })
            }
        };
        if meta.len() == 0 {
            return Err(crate::io::format::Error::FileNotValid {
                path: fpath.to_path_buf(),
                msg: Some("file is 0 bytes".into()),
            });
        }

        // NOTE: Could/should try to attempt to read a record/magic bytes, skipping this for now though

        Ok(())
    }
}
pub trait BascetRead {
    // Check if a cell exists.
    fn has_cell(&self, cell: &str) -> bool;

    // List all cell IDs.
    fn get_cells(&self) -> Vec<String>;

    // Retrieve all records for a cell.
    fn read_cell(&mut self, cell: &str) -> Vec<crate::common::ReadPair>;
}
pub trait BascetWrite {
    fn write_cell(&mut self, cell_id: &str, reads: &Vec<crate::common::ReadPair>);
}

#[enum_dispatch]
pub trait BascetStream<T>: Sized
where
    T: BascetStreamToken + 'static,
    T::Builder: BascetStreamTokenBuilder<Token = T>,
{
    fn next_cell(&mut self) -> Result<Option<T>, crate::io::format::Error>;
    fn set_reader_threads(self, _: usize) -> Self {
        self
    }
}

pub trait BascetStreamToken: Send {
    type Builder: BascetStreamTokenBuilder<Token = Self>;
    fn builder() -> Self::Builder;
}
pub trait BascetStreamTokenBuilder: Sized {
    type Token: BascetStreamToken;

    // Core methods all builders must support
    fn add_cell_id_owned(self, id: Vec<u8>) -> Self;
    fn build(self) -> Self::Token;

    // Optional methods with default implementations
    fn add_cell_id_slice(self, slice: &[u8]) -> Self {
        self
    }
    fn add_seq_slice(self, slice: &[u8]) -> Self {
        self
    }
    fn add_underlying(self, other: Arc<Vec<u8>>) -> Self {
        self
    }

    fn add_sequence_owned(self, seq: Vec<u8>) -> Self {
        self
    }
    fn add_quality_owned(self, scores: Vec<u8>) -> Self {
        self
    }

    fn add_metadata(self, meta: &str) -> Self {
        self
    }
}
pub trait BascetExtract {}
