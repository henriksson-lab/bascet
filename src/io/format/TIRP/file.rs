use crate::{io::BascetFile, utils::expand_and_resolve_path};

pub struct File {
    path: std::path::PathBuf,
}

impl File {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let path = expand_and_resolve_path(&path)?;
        File::file_validate(&path)?;

        Ok(Self { path: path })
    }
}

impl BascetFile for File {
    fn file_validate<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<()> {
        let path = path.as_ref().to_path_buf();

        // 1. File exists and is a regular file
        if !path.exists() || !path.is_file() {
            anyhow::bail!("File not found or is not a regular file.");
        }

        // 2. File is not empty
        let meta = std::fs::metadata(&path)?;
        if meta.len() == 0 {
            anyhow::bail!("File is empty.");
        }

        // 3. File has the correct extension
        let ext_ok = path.extension().and_then(|e| e.to_str()) == Some("gz")
            && path
                .file_stem()
                .and_then(|s| s.to_str())
                .map_or(false, |s| s.ends_with("tirp"));
        if !ext_ok {
            anyhow::bail!("File extension does not match .tirp.gz.");
        }

        // 4. Tabix index file exists
        let tbi_path = path.with_file_name(format!(
            "{}.tbi",
            path.file_name().unwrap().to_string_lossy()
        ));
        if !tbi_path.exists() {
            anyhow::bail!(format!("Index file (.tbi) is missing. {:?}", tbi_path));
        }

        // 5. Could try to attempt to read a record

        Ok(())
    }

    fn file_path(&self) -> &std::path::Path {
        &self.path
    }

    fn file_open(&self) -> anyhow::Result<std::fs::File> {
        Ok(std::fs::File::open(&self.path)?)
    }
}
