<<<<<<< HEAD
use crate::io::traits::{BascetFile, BascetTempFile};
use crate::utils::expand_and_resolve;

const VALID_EXTENSIONS: &[&str] = &["tirp", "tirp.gz"];

/// Tirp input file - must exist, have content, and match extensions
#[derive(Debug)]
pub struct Input {
    path: std::path::PathBuf,
}

impl Input {
=======
use crate::{
    io::BascetFile,
    utils::{command_to_string, expand_and_resolve},
};
use std::process::Command;

#[derive(Debug)]
pub struct File {
    path: std::path::PathBuf,
}

impl File {
>>>>>>> main
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
        let path = match expand_and_resolve(&path) {
            Ok(p) => p,
            Err(_) => path.as_ref().to_path_buf(),
        };

<<<<<<< HEAD
        let file = Self { path };
        file.validate()?;
        Ok(file)
    }
}

impl BascetFile for Input {
    fn valid_extensions(&self) -> &[&str] {
        VALID_EXTENSIONS
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(&self.path)?)
    }
}

/// Tirp output file - parent directory must exist, file may or may not exist
#[derive(Debug)]
pub struct Output {
    path: std::path::PathBuf,
}

impl Output {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
        let path = match expand_and_resolve(&path) {
            Ok(p) => p,
            Err(_) => path.as_ref().to_path_buf(),
        };

        let file = Self { path };
        file.validate()?;
        Ok(file)
    }
}

impl BascetFile for Output {
    fn valid_extensions(&self) -> &[&str] {
        VALID_EXTENSIONS
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(&self.path)?)
    }

    // Override validation for output files
    fn validate(&self) -> Result<(), crate::runtime::Error> {
        self.validate_parent_dir()?;
        self.validate_extension()?;
        Ok(())
    }
}

/// Tirp temporary file - automatically deleted on drop, minimal validation
#[derive(Debug)]
pub struct Temp {
    inner: tempfile::NamedTempFile,
}

impl BascetFile for Temp {
    fn valid_extensions(&self) -> &[&str] {
        VALID_EXTENSIONS
    }

    fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    fn open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(self.path())?)
    }

    fn validate(&self) -> Result<(), crate::runtime::Error> {
        self.validate_parent_dir()?;
        self.validate_extension()?;
        Ok(())
    }
}

impl BascetTempFile for Temp {
    fn from_tempfile(temp_file: tempfile::NamedTempFile) -> Result<Self, crate::runtime::Error> {
        Ok(Self { inner: temp_file })
    }

    fn preserve(self) -> std::path::PathBuf {
        let temp_path = self.inner.into_temp_path();
        let path = temp_path.to_path_buf();

        // prevent drop from cleaning the file
        std::mem::forget(temp_path);
        path
    }
=======
        let _ = match Self::file_validate(&path) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        Ok(Self { path: path })
    }

    // /// TABIX-index tirp file
    // NOTE: probably better to generate the index when generating shards?
    pub fn index(&self) -> Result<(), impl std::error::Error> {
        let mut process = Command::new("tabix");
        let process = process.arg("-p").arg("bed").arg(&self.path);

        return match process.output() {
            Ok(out) => match out.status.success() {
                true => Ok(()),
                false => {
                    let msg = format!("error code: {:?}", out.status.code());
                    Err(crate::runtime::Error::UtilityExecutionError {
                        utility: format!("{:?}", process.get_program()),
                        cmd: command_to_string(&process),
                        msg: Some(msg),
                    })
                }
            },
            Err(_) => Err(crate::runtime::Error::UtilityNotExecutable {
                utility: format!("{:?}", process.get_program()),
            }),
        };
    }
}

impl BascetFile for File {
    const VALID_EXT: Option<&'static str> = Some("gz");

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
        let sext_ok = fpath
            .file_stem()
            .and_then(|s| s.to_str())
            .map_or(false, |s| s.ends_with("tirp"));

        let ext_ok = (fext == Self::VALID_EXT && sext_ok) || fext == Self::VALID_EXT;
        if !ext_ok {
            return Err(crate::runtime::Error::FileNotValid {
                path: fpath.to_path_buf(),
                msg: Some("file extension is not tirp or tirp.gz".into()),
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

        // Tabix index file checks
        let ipath = fpath.with_file_name(format!(
            "{}.tbi",
            fpath.file_name().unwrap().to_string_lossy()
        ));

        // 1. File exists and is a regular file
        if !ipath.exists() {
            return Err(crate::runtime::Error::FileNotFound {
                path: ipath.to_path_buf(),
            });
        } else if !ipath.is_file() {
            return Err(crate::runtime::Error::FileNotValid {
                path: ipath.to_path_buf(),
                msg: Some("directory found instead".into()),
            });
        }

        // 2. File has the correct extension
        let fext = ipath.extension().and_then(|e| e.to_str());
        let ext_ok = fext == Some("tbi");
        if !ext_ok {
            return Err(crate::runtime::Error::FileNotValid {
                path: ipath.to_path_buf(),
                msg: Some("file extension is not tbi".into()),
            });
        }

        // 3. File is not empty
        let meta = match std::fs::metadata(&ipath) {
            Ok(m) => m,
            Err(_) => {
                return Err(crate::runtime::Error::FileNotValid {
                    path: ipath.to_path_buf(),
                    msg: Some("metadata could not be fetched".into()),
                })
            }
        };
        if meta.len() == 0 {
            return Err(crate::runtime::Error::FileNotValid {
                path: ipath.to_path_buf(),
                msg: Some("file is 0 bytes".into()),
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
>>>>>>> main
}
