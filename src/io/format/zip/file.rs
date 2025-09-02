use crate::{
    io::BascetFile,
    utils::{expand_and_resolve},
};

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
}

impl BascetFile for File {
    const VALID_EXT: Option<&'static str> = Some("zip");

    fn file_path(&self) -> &std::path::Path {
        &self.path
    }

    fn file_open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(&self.path)?)
    }
}
