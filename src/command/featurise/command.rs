use super::{
    constants::{
        COUNT_DEFAULT_PATH_IN, COUNT_DEFAULT_PATH_INDEX, COUNT_DEFAULT_PATH_OUT,
        COUNT_DEFAULT_PATH_TEMP,
    },
    core::{core::RDBCounter, params, threading::DefaultThreadState},
};
use anyhow::Result;
use clap::Args;
use itertools::Itertools;
use linya::Progress;
use rev_buf_reader::RevBufReader;
use std::io::Write;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    thread,
};
use walkdir::WalkDir;
use zip::{write::FileOptions, HasZipMetadata, ZipArchive, ZipWriter};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = COUNT_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 'j', value_parser, default_value = COUNT_DEFAULT_PATH_INDEX)]
    pub path_index: PathBuf,
    #[arg(short = 't', value_parser, default_value = COUNT_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = COUNT_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(u32))]
    threads_read: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_write: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub seed: Option<u64>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        self.verify_input_file()?;
        let kmer_size = self.verify_kmer_size()?;
        let (threads_read, threads_write) = self.resolve_thread_config()?;

        let rdb_file = File::open(&self.path_in).expect("Failed to open RDB file");
        let index_file = File::open(&self.path_index).expect("Failed to open index file");
        let index_reader = BufReader::new(index_file);
        let mut archive_rdb = ZipArchive::new(rdb_file).expect("Unable to create zip archive");

        for line in index_reader.lines() {
            if let Ok(line) = line {
                let index = line
                    .split(',')
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .expect("Error parsing index file");

                let mut barcode_kmc = archive_rdb
                    .by_index(index)
                    .expect(&format!("No file at index {}", &index));

                let barcode_path = barcode_kmc.mangled_name();
                let barcode_kmc_ext = barcode_path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap();
                match barcode_kmc_ext {
                    "kmc_pre" | "kmc_suf" => {}
                    _ => continue,
                }

                let barcode = barcode_path.parent().unwrap();

                let path_dir_barcode = self.path_tmp.join(barcode);
                let _ = fs::create_dir_all(&path_dir_barcode);

                let path_temp_barcode_kmc = path_dir_barcode.join(format!("kmc.{barcode_kmc_ext}"));
                let mut file_temp_barcode_kmc = File::create(&path_temp_barcode_kmc).unwrap();
                std::io::copy(&mut barcode_kmc, &mut file_temp_barcode_kmc).unwrap();
            }
        }

        let dbs: Vec<PathBuf> = WalkDir::new(&self.path_tmp)
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() || path == self.path_tmp {
                    return None;
                }
                Some(path.to_path_buf())
            })
            .collect();

        let path_kmc_union_script = self.path_tmp.join("kmc_union");
        let file_kmc_union_script = File::create(&path_kmc_union_script).unwrap();
        let mut writer_kmc_union_script = BufWriter::new(&file_kmc_union_script);
        writeln!(writer_kmc_union_script, "INPUT:")?;
        for db in &dbs {
            let barcode = db.file_stem().unwrap();
            let barcode_sanitised = barcode.to_str().unwrap().replace("-", "");
            writeln!(
                writer_kmc_union_script,
                "{} = {}",
                barcode_sanitised,
                db.join("kmc").to_str().unwrap()
            )?;
        }
        writeln!(writer_kmc_union_script, "OUTPUT:")?;

        let dbs_union_kmc = dbs
            .iter()
            .map(|db| {
                let barcode = db.file_stem().unwrap();
                let barcode_sanitised = barcode.to_str().unwrap().replace("-", "");

                barcode_sanitised
            })
            .join(" + sum ");
        writeln!(writer_kmc_union_script, "{} = {dbs_union_kmc}", self.path_out.to_str().unwrap())?;
        
        let _ = std::process::Command::new("kmc_tools")
                .arg("complex")
                .arg(&path_kmc_union_script)
                .output()?;
        
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

    fn verify_kmer_size(&self) -> Result<usize> {
        if self.kmer_size < 48 {
            return Ok(self.kmer_size);
        }

        anyhow::bail!("Invalid kmer size k:{}", self.kmer_size);
    }

    fn resolve_thread_config(&self) -> Result<(usize, usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 2 {
            anyhow::bail!("At least two threads must be available");
        }

        let (threads_read, threads_write) = match (self.threads_read, self.threads_write) {
            (Some(i), Some(w)) => (i, w),
            (Some(i), None) => (i, available_threads.saturating_sub(i).max(1)),
            (None, Some(w)) => (1, w),
            (None, None) => (1, available_threads.saturating_sub(1).max(1)),
        };

        if threads_read == 0 {
            anyhow::bail!("At least one IO thread required");
        }
        if threads_write == 0 {
            anyhow::bail!("At least one work thread required");
        }

        Ok((threads_read, threads_write))
    }
}
