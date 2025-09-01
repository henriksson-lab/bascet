use crate::io::traits::BascetFile;
use crate::utils::expand_and_resolve;

const VALID_EXTENSIONS: &[&str] = &["zip"];

/// Zip input file - must exist, have content, and match extensions
#[derive(Debug)]
pub struct Input {
    path: std::path::PathBuf,
}

impl Input {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
        let path = match expand_and_resolve(&path) {
            Ok(p) => p,
            Err(_) => path.as_ref().to_path_buf(),
        };

        let file = Self { path };
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

/// Zip output file - parent directory must exist, file may or may not exist
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

// Zip temporary file - automatically deleted on drop, minimal validation
// #[derive(Debug)]
// pub struct Temp {
//     inner: tempfile::NamedTempFile,
// }

// impl BascetFile for Temp {
//     fn valid_extensions(&self) -> &[&str] {
//         VALID_EXTENSIONS
//     }

//     fn path(&self) -> &std::path::Path {
//         self.inner.path()
//     }

//     fn open(&self) -> anyhow::Result<std::fs::File> {
//         Ok(std::fs::File::open(self.path())?)
//     }

//     fn validate(&self) -> Result<(), crate::runtime::Error> {
//         self.validate_parent_dir()?;
//         self.validate_extension()?;
//         Ok(())
//     }
// }

// impl BascetTempFile for Temp {
//     fn from_tempfile(temp_file: tempfile::NamedTempFile) -> Result<Self, crate::runtime::Error> {
//         Ok(Self { inner: temp_file })
//     }

//     fn preserve(self) -> std::path::PathBuf {
//         let temp_path = self.inner.into_temp_path();
//         let path = temp_path.to_path_buf();

//         // prevent drop from cleaning the file
//         std::mem::forget(temp_path);
//         path
//     }
// }
