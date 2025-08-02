use crate::{
    io::{format, BascetFile},
    log_info, runtime,
    utils::{command_to_string, expand_and_resolve},
};
use std::process::Command;

#[derive(Debug)]
pub struct File {
    path: std::path::PathBuf,
}

impl File {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
        let path = match expand_and_resolve(&path) {
            Ok(p) => p,
            Err(_) => path.as_ref().to_path_buf(),
        };

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
            return Err(runtime::Error::file_not_valid(
                fpath,
                Some(format!(
                    "file extension is not tirp or tirp.gz (is {fext:?})"
                )),
            ));
        }

        // // 3. File is not empty
        // let meta = match std::fs::metadata(&fpath) {
        //     Ok(m) => m,
        //     Err(_) => {
        //         return Err(crate::runtime::Error::FileNotValid {
        //             path: fpath.to_path_buf(),
        //             msg: Some("metadata could not be fetched".into()),
        //         })
        //     }
        // };
        // if meta.len() == 0 {
        //     return Err(crate::runtime::Error::FileNotValid {
        //         path: fpath.to_path_buf(),
        //         msg: Some("file is 0 bytes".into()),
        //     });
        // }

        // // Tabix index file checks
        // let ipath = fpath.with_file_name(format!(
        //     "{}.tbi",
        //     fpath.file_name().unwrap().to_string_lossy()
        // ));

        // // 1. File exists and is a regular file
        // if !ipath.exists() {
        //     return Err(crate::runtime::Error::FileNotFound {
        //         path: ipath.to_path_buf(),
        //     });
        // } else if !ipath.is_file() {
        //     return Err(crate::runtime::Error::FileNotValid {
        //         path: ipath.to_path_buf(),
        //         msg: Some("directory found instead".into()),
        //     });
        // }

        // // 2. File has the correct extension
        // let fext = ipath.extension().and_then(|e| e.to_str());
        // let ext_ok = fext == Some("tbi");
        // if !ext_ok {
        //     return Err(crate::runtime::Error::FileNotValid {
        //         path: ipath.to_path_buf(),
        //         msg: Some("file extension is not tbi".into()),
        //     });
        // }

        // // 3. File is not empty
        // let meta = match std::fs::metadata(&ipath) {
        //     Ok(m) => m,
        //     Err(_) => {
        //         return Err(crate::runtime::Error::FileNotValid {
        //             path: ipath.to_path_buf(),
        //             msg: Some("metadata could not be fetched".into()),
        //         })
        //     }
        // };
        // if meta.len() == 0 {
        //     return Err(crate::runtime::Error::FileNotValid {
        //         path: ipath.to_path_buf(),
        //         msg: Some("file is 0 bytes".into()),
        //     });
        // }

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
