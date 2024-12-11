use std::fs;
use std::io::Write;
use std::{fs::File, io::BufWriter, path::PathBuf};

use anyhow::Result;
use clap::Args;
use zip::ZipArchive;

use super::constants::{INDEX_DEFAULT_PATH_OUT, INDEX_DEFAULT_PATH_SPLIT};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = INDEX_DEFAULT_PATH_SPLIT)]
    path_in: PathBuf,
    #[arg(short = 'o', value_parser, default_value = INDEX_DEFAULT_PATH_OUT)]
    path_out: PathBuf,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        self.verify_input_file()?;

        let zip_file = File::open(&self.path_in)?;
        let mut archive = ZipArchive::new(zip_file)?;
        fs::create_dir_all(&self.path_out)?;

        let reads_index_file = File::create(&self.path_out.join("reads"))?;
        let mut reads_index_writer = BufWriter::new(&reads_index_file);
        let kmc_index_file = File::create(&self.path_out.join("kmc"))?;
        let mut kmc_index_writer = BufWriter::new(&kmc_index_file);

        for i in 0..archive.len() {
            let file = archive
                .by_index(i)
                .map_err(|e| anyhow::anyhow!("Failed to read ZIP entry {}: {}", i, e))?;

            match file.name() {
                name if name.contains("reads.fastq") => {
                    writeln!(reads_index_writer, "{},{}", i, file.name())
                        .map_err(|e| anyhow::anyhow!("Failed to write index entry {}: {}", i, e))?
                }
                name if name.contains("dump.txt") => {
                    writeln!(kmc_index_writer, "{},{}", i, file.name())
                        .map_err(|e| anyhow::anyhow!("Failed to write index entry {}: {}", i, e))?;
                }
                _ => {}
            }
        }

        reads_index_writer.flush()?;
        kmc_index_writer.flush()?;
        Ok(())
    }

    fn verify_input_file(&mut self) -> anyhow::Result<()> {
        if let Ok(file) = File::open(&self.path_in) {
            if file.metadata()?.len() == 0 {
                anyhow::bail!("Empty input file");
            }
        }
        match self.path_in.extension().and_then(|ext| ext.to_str()) {
            Some("zip") => return Ok(()),
            _ => anyhow::bail!("Input file must be a zip archive"),
        };
    }
}
