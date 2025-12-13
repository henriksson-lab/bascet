// use crate::command::countsketch::CountsketchInput;
use crate::command::countsketch::CountsketchOutput;

use crate::command::shardify::ShardifyInput;
use crate::command::shardify::ShardifyOutput;

use crate::command::getraw::DebarcodeHistOutput;
use crate::command::getraw::DebarcodeMergeInput;
use crate::command::getraw::DebarcodeMergeOutput;
use crate::command::getraw::DebarcodeReadsInput;
// use crate::command::trim_experimental::TrimExperimentalOutput;

#[enum_dispatch::enum_dispatch]
pub trait BascetFile: Sized {
    /// Valid file extensions for this file type (e.g., &["tirp", "tirp.gz"])
    fn path(&self) -> &std::path::Path;
    fn open(&self) -> anyhow::Result<std::fs::File>;

    /// Validate that file exists and is a regular file
    fn validate_exists(&self) -> Result<(), crate::runtime::Error> {
        let fpath = self.path();

        if !fpath.exists() {
            return Err(crate::runtime::Error::file_not_found(fpath));
        }

        if !fpath.is_file() {
            return Err(crate::runtime::Error::file_not_valid(
                fpath,
                Some("directory found instead"),
            ));
        }

        Ok(())
    }

    /// Validate file extension to match any self.valid_extensions()
    fn valid_extensions(&self) -> &[&str];
    fn validate_extension(&self) -> Result<(), crate::runtime::Error> {
        let fpath = self.path();
        let valid_extensions = self.valid_extensions();

        let filename = fpath.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            crate::runtime::Error::file_not_valid(fpath, Some("invalid filename"))
        })?;

        let matches = valid_extensions.iter().any(|&ext| filename.ends_with(ext));

        if !matches {
            return Err(crate::runtime::Error::file_not_valid(
                fpath,
                Some(format!(
                    "file must end with one of: {:?} (found: {})",
                    valid_extensions, filename
                )),
            ));
        }

        Ok(())
    }

    /// Validate file is not empty
    fn validate_not_empty(&self) -> Result<(), crate::runtime::Error> {
        let fpath = self.path();

        let meta = std::fs::metadata(fpath).map_err(|_| {
            crate::runtime::Error::file_not_valid(fpath, Some("metadata could not be fetched"))
        })?;

        if meta.len() == 0 {
            return Err(crate::runtime::Error::file_not_valid(
                fpath,
                Some("file is empty"),
            ));
        }

        Ok(())
    }

    /// Validate parent directory exists
    fn validate_parent_dir(&self) -> Result<(), crate::runtime::Error> {
        let fpath = self.path();

        if let Some(parent) = fpath.parent() {
            if !parent.exists() {
                return Err(crate::runtime::Error::file_not_valid(
                    fpath,
                    Some("parent directory does not exist"),
                ));
            }
        }
        Ok(())
    }

    /// Full file validation - can be overridden by implementors
    fn validate(&self) -> Result<(), crate::runtime::Error> {
        self.validate_exists()?;
        self.validate_extension()?;
        self.validate_not_empty()?;
        Ok(())
    }
}

// // #[enum_dispatch::enum_dispatch]
// pub trait BascetTempFile: BascetFile {
//     /// Create temp file instance from NamedTempFile (implementors must provide this)
//     fn from_tempfile(temp_file: tempfile::NamedTempFile) -> Result<Self, crate::runtime::Error>
//     where
//         Self: Sized;

//     /// Prevent automatic deletion and return the path (consumes temp file)
//     fn preserve(self) -> std::path::PathBuf;

//     /// Manually trigger cleanup now (consumes self to prevent double-cleanup)
//     fn cleanup(self) -> Result<(), crate::runtime::Error> {
//         if self.path().exists() {
//             std::fs::remove_file(self.path()).map_err(|e| {
//                 crate::runtime::Error::file_not_valid(
//                     self.path(),
//                     Some(format!("failed to delete temp file: {}", e)),
//                 )
//             })?;
//         }
//         std::mem::forget(self);
//         Ok(())
//     }

//     /// Create temp file in system temp directory with auto-generated name
//     fn with_unique_name(extension: &str) -> Result<Self, crate::runtime::Error>
//     where
//         Self: Sized,
//     {
//         let suffix = if extension.starts_with('.') {
//             extension.to_string()
//         } else {
//             format!(".{}", extension)
//         };

//         let temp_file = tempfile::NamedTempFile::with_suffix(&suffix).map_err(|e| {
//             crate::runtime::Error::file_not_valid(
//                 "temp_file",
//                 Some(format!("failed to create temp file: {}", e)),
//             )
//         })?;

//         let temp = Self::from_tempfile(temp_file)?;
//         temp.validate_extension()?;
//         Ok(temp)
//     }

//     /// Create temp file in specified directory with auto-generated name
//     fn with_unique_name_in<P: AsRef<std::path::Path>>(
//         dir: P,
//         extension: &str,
//     ) -> Result<Self, crate::runtime::Error>
//     where
//         Self: Sized,
//     {
//         let suffix = if extension.starts_with('.') {
//             extension.to_string()
//         } else {
//             format!(".{}", extension)
//         };

//         let temp_file = tempfile::NamedTempFile::with_suffix_in(&suffix, dir).map_err(|e| {
//             crate::runtime::Error::file_not_valid(
//                 "temp_file",
//                 Some(format!("failed to create temp file in directory: {}", e)),
//             )
//         })?;

//         let temp = Self::from_tempfile(temp_file)?;
//         temp.validate_extension()?;
//         Ok(temp)
//     }
// }
