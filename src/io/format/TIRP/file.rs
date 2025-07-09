use std::process::Command;

use thiserror::Error;

use crate::{io::BascetFile, utils::expand_and_resolve};

pub struct File {
    path: std::path::PathBuf,
}

#[derive(Error, Debug)]
enum Error {
    #[error(
        "[TIRP File] File at \"{:?}\' not found.",
         fpath
    )]
    FileNotFound { 
        fpath: std::path::PathBuf 
    },

    #[error(
        "[TIRP File] File at \"{:?}\' is invalid{}.",
        fpath,
        match msg { Some(m) => format!(" [{}]", m), None => String::new() }
    )]
    FileNotValid {
        fpath: std::path::PathBuf,
        msg: Option<String>,
    },

    #[error(
        "[TIRP Index File] File index at \"{:?}\' for file \"{:?}\' not found.",
        fpath,
        ipath
    )]
    IndexNotFound {
        fpath: std::path::PathBuf,
        ipath: std::path::PathBuf,
    },

    #[error(
        "[TIRP Index File] File index at \"{:?}\' for file \"{:?}\' is invalid{}.",
        fpath,
        ipath,
        match msg { Some(m) => format!(" [{}]", m), None => String::new() }
    )]
    IndexNotValid {
        fpath: std::path::PathBuf,
        ipath: std::path::PathBuf,
        msg: Option<String>,
    },

    #[error(
        "[TIRP Index File] Index file for file at \"{:?}\' is could not be built{}.",
        fpath,
         match msg { Some(m) => format!(" [{}]", m), None => String::new() }
    )]
    IndexNotBuilt {
        fpath: std::path::PathBuf,
        msg: Option<String>,
    },
}

impl File {
    pub fn new<'this, P: AsRef<std::path::Path>>(path: P) -> Result<Self, impl std::error::Error> {
        let path = path.as_ref().to_path_buf();

        let path = match expand_and_resolve(&path) {
            Ok(p) => p,
            Err(e) => panic!(),
        };

        let _ = match Self::file_validate(&path) {
            Ok(_) => {}
            Err(e) => return Err(e),
        };

        Ok(Self { path: path })
    }

    /// TABIX-index TIRP file
    pub fn index(&self) -> Result<(), impl std::error::Error> {
        let mut process = Command::new("tabix");
        let process = process.arg("-p").arg("bed").arg(&self.path);

        return match process.output() {
            Ok(out) => match out.status.success() {
                true => Ok(()),
                false => {
                    let msg = format!("error code: {:?}", out.status.code());
                    Err(Error::IndexNotBuilt {
                        fpath: self.path.to_path_buf(),
                        msg: Some(msg),
                    })
                }
            },
            Err(_) => Err(Error::IndexNotBuilt {
                fpath: self.path.to_path_buf(),
                msg: Some("tabix command could not be run".into()),
            }),
        };
    }
}

impl BascetFile for File {
    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> Result<(), impl std::error::Error> {
        let fpath = path.as_ref();

        // 1. File exists and is a regular file
        if !fpath.exists() || !fpath.is_file() {
            return Err(Error::FileNotFound {
                fpath: fpath.to_path_buf(),
            });
        }

        // 2. File has the correct extension
        let fext = fpath.extension().and_then(|e| e.to_str());
        let sext_ok = fpath
            .file_stem()
            .and_then(|s| s.to_str())
            .map_or(false, |s| s.ends_with("tirp"));

        let ext_ok = (fext == Some("gz") && sext_ok) || fext == Some("tirp");
        if !ext_ok {
            return Err(Error::FileNotValid {
                fpath: fpath.to_path_buf(),
                msg: Some("File extension is not tirp or tirp.gz".into()),
            });
        }

        // 3. File is not empty
        let meta = match std::fs::metadata(&fpath) {
            Ok(m) => m,
            Err(_) => {
                return Err(Error::FileNotValid {
                    fpath: fpath.to_path_buf(),
                    msg: Some("Metadata could not be fetched".into()),
                })
            }
        };
        if meta.len() == 0 {
            return Err(Error::FileNotValid {
                fpath: fpath.to_path_buf(),
                msg: Some("File is 0 bytes".into()),
            });
        }

        // Tabix index file checks
        let ipath = fpath.with_file_name(format!(
            "{}.tbi",
            fpath.file_name().unwrap().to_string_lossy()
        ));

        // 1. File exists and is a regular file
        if !ipath.exists() || !ipath.is_file() {
            return Err(Error::IndexNotFound {
                fpath: ipath.to_path_buf(),
                ipath: fpath.to_path_buf(),
            });
        }

        // 2. File has the correct extension
        let fext = fpath.extension().and_then(|e| e.to_str());
        let ext_ok = fext == Some("tbi");
        if !ext_ok {
            return Err(Error::IndexNotValid {
                fpath: ipath.to_path_buf(),
                ipath: fpath.to_path_buf(),
                msg: Some("File extension is not tbi".into()),
            });
        }

        // 3. File is not empty
        let meta = match std::fs::metadata(&fpath) {
            Ok(m) => m,
            Err(_) => {
                return Err(Error::IndexNotValid {
                    fpath: ipath.to_path_buf(),
                    ipath: fpath.to_path_buf(),
                    msg: Some("Metadata could not be fetched".into()),
                })
            }
        };
        if meta.len() == 0 {
            return Err(Error::IndexNotValid {
                fpath: ipath.to_path_buf(),
                ipath: fpath.to_path_buf(),
                msg: Some("File is 0 bytes".into()),
            });
        }

        // NOTE: Could/should try to attempt to read a record/magic bytes, skipping this for now though

        Ok(())
    }

    fn file_path(&self) -> &std::path::Path {
        &self.path
    }

    fn file_open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(&self.path)?)
    }
}
