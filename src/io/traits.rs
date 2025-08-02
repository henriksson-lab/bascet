use std::sync::Arc;

use enum_dispatch::enum_dispatch;

use crate::command::countsketch::CountsketchStream;

use crate::command::shardify::ShardifyStream;
use crate::command::shardify::ShardifyWriter;
use crate::common::ReadPair;

pub trait BascetFile {
    const VALID_EXT: Option<&'static str>;

    fn file_path(&self) -> &std::path::Path;
    fn file_open(&self) -> anyhow::Result<std::fs::File>;

    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> Result<(), crate::runtime::Error> {
        let fpath = path.as_ref();

        // 1. File exists and is a regular file
        if !fpath.exists() {
            return Err(crate::runtime::Error::FileNotFound {
                path: fpath.to_path_buf(),
            });
        } else if !fpath.is_file() {
            return Err(crate::runtime::Error::FileNotValid {
                path: fpath.to_path_buf(),
                msg: Some("directory found instead".into()),
            });
        }

        // 2. File has the correct extension
        let fext = fpath.extension().and_then(|e| e.to_str());
        if fext != Self::VALID_EXT {
            return Err(crate::runtime::Error::FileNotValid {
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
                return Err(crate::runtime::Error::FileNotValid {
                    path: fpath.to_path_buf(),
                    msg: Some("metadata could not be fetched".into()),
                })
            }
        };
        if meta.len() == 0 {
            return Err(crate::runtime::Error::FileNotValid {
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

#[enum_dispatch]
pub trait BascetWriter<W>: Sized
where
    W: std::io::Write,
{
    fn set_writer(self, _: W) -> Self {
        self
    }
    fn write_cell<T>(&mut self, token: T)
    where
        T: BascetStreamToken;
}

#[enum_dispatch]
pub trait BascetStream<T>: Sized
where
    T: BascetStreamToken + 'static,
    T::Builder: BascetStreamTokenBuilder<Token = T>,
{
    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error>;
    fn set_reader_threads(self, _: usize) -> Self {
        self
    }
}

pub trait BascetStreamToken: Send + Sized {
    type Builder: BascetStreamTokenBuilder<Token = Self>;
    fn builder() -> Self::Builder;

    fn get_cell(&self) -> Option<&[u8]> {
        None
    }
    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        None
    }
    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        None
    }
    fn get_umis(&self) -> Option<&[&[u8]]> {
        None
    }
}
pub trait BascetStreamTokenBuilder: Sized {
    type Token: BascetStreamToken;

    // Core methods all builders must support
    fn build(self) -> Self::Token;

    // Optional methods with default implementations
    fn add_cell_id_slice(self, id: &[u8]) -> Self {
        self
    }
    fn add_rp_slice(self, r1: &[u8], r2: &[u8]) -> Self {
        self
    }
    fn add_qp_slice(self, q1: &[u8], q2: &[u8]) -> Self {
        self
    }
    fn add_sequence_slice(self, sequence: &[u8]) -> Self {
        self
    }
    fn add_quality_slice(self, qualities: &[u8]) -> Self {
        self
    }
    fn add_umi_slice(self, umi: &[u8]) -> Self {
        self
    }
    fn add_underlying(self, other: Arc<Vec<u8>>) -> Self {
        self
    }

    fn add_cell_id_owned(self, id: Vec<u8>) -> Self {
        self
    }
    fn add_sequence_owned(self, sequence: Vec<u8>) -> Self {
        self
    }
    fn add_rp_owned(self, rp: (Vec<u8>, Vec<u8>)) -> Self {
        self
    }
    fn add_quality_owned(self, scores: Vec<u8>) -> Self {
        self
    }
    fn add_umi_owned(self, umi: Vec<u8>) -> Self {
        self
    }

    fn add_metadata_owned(self, meta: Vec<u8>) -> Self {
        self
    }
    fn add_metadata_slice(self, meta: &[u8]) -> Self {
        self
    }
}
pub trait BascetExtract {}
