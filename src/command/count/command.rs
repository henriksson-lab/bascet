use anyhow::Result;
use clap::Args;
use linya::Progress;
use rev_buf_reader::RevBufReader;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    thread,
};

use super::{
    constants::{
        COUNT_DEFAULT_PATH_IN, COUNT_DEFAULT_PATH_INDEX, COUNT_DEFAULT_PATH_OUT,
        COUNT_DEFAULT_PATH_TEMP,
    },
    core::{core::RDBCounter, params, threading::DefaultThreadState},
};

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

        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_idx: self.path_index.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            kmer_size: kmer_size,
        };
        let params_threading = params::Threading {
            threads_read: threads_read,
            threads_write: threads_write,
            thread_pool: &threadpool::ThreadPool::new(threads_read + threads_write),
        };
        fs::create_dir_all(&self.path_out).unwrap();
        let thread_paths_out: Vec<(PathBuf, PathBuf)> = (0..threads_write)
            .map(|index| {
                (
                    self.path_out
                        .join(format!("rdb-count-{}", index))
                        .with_extension("zip"),
                    self.path_tmp.join(format!("temp_path-{}", index)),
                )
            })
            .collect();

        let thread_states: Vec<Arc<DefaultThreadState>> = thread_paths_out
            .iter()
            .map(|path| {
                let _ = fs::create_dir_all(path.1.clone());
                println!("{:?}", path.0);
                let zipfile = File::create(path.0.clone()).unwrap();
                Arc::new(DefaultThreadState::new(zipfile, path.1.clone()))
            })
            .collect();

        let _ = RDBCounter::extract(
            Arc::new(params_io),
            Arc::new(params_runtime),
            Arc::new(params_threading),
            thread_states,
        );

        let zip_paths: Vec<PathBuf> = thread_paths_out.iter().map(|e| e.0.clone()).collect();
        let _ = match std::process::Command::new("zipmerge")
            .arg("-isk")
            .arg(self.path_out.join("rdb-count").with_extension("zip"))
            .args(&zip_paths)
            .output()
        {
            Ok(_) => {}
            Err(e) => panic!("Failed to execute zipmerge command: {}", e),
        };

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
