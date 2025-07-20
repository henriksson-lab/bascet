use crate::io::detect::{AutoStream, AutoToken};
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
pub trait BascetStream {
    fn set_reader_threads(&mut self, n_threads: usize);
    fn set_worker_threads(&mut self, n_threads: usize);

    fn next(&mut self) -> anyhow::Result<Option<AutoToken>>;

    fn par_map<C, R, G, L>(
        &mut self,
        global_state: G,
        local_states: Vec<L>,
        f: C,
    ) -> (Vec<R>, G, Vec<L>)
    where
        C: Fn(AutoToken, &G, &mut L) -> R + Send + Sync + 'static,
        R: Send + 'static,
        G: std::fmt::Debug + Send + Sync + 'static,
        L: Send + Sync + 'static;
}

#[enum_dispatch]
pub trait BascetStreamToken {}
pub trait BascetExtract {}
