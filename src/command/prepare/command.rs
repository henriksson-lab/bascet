use crate::command::constants::RDB_PATH_INDEX_READS;

use super::{
    constants::{
        PREPARE_DEFAULT_CLEANUP, PREPARE_DEFAULT_MIN_READS_PER_CELL, PREPARE_DEFAULT_PATH_OUT,
        PREPARE_DEFAULT_PATH_TMP,
    },
    core::{core::BAMProcessor, params, state},
};
use anyhow::Result;
use clap::Args;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    thread,
};
use zip::{write::FileOptions, ZipWriter};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', long, value_parser)]
    path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = PREPARE_DEFAULT_PATH_TMP)]
    path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = PREPARE_DEFAULT_PATH_OUT)]
    path_out: PathBuf,
    #[arg(value_parser, default_value_t = PREPARE_DEFAULT_MIN_READS_PER_CELL)]
    min_reads_per_cell: usize,
    #[arg(long, default_value_t = PREPARE_DEFAULT_CLEANUP)]
    cleanup: bool,
    #[arg(long, value_parser = clap::value_parser!(u32))]
    threads_read: Option<u32>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_write: Option<usize>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let (threads_read, threads_write) = self.resolve_thread_config()?;
 
        let path_rdb_out = self.path_out.join("rdb");
        let file_rdb_out = File::create(&path_rdb_out).unwrap();
        let bufwriter_rdb_out = BufWriter::new(&file_rdb_out);
        let mut zipwriter_rdb_out = ZipWriter::new(bufwriter_rdb_out);
        let zipwriter_opts: FileOptions<'_, ()> =
            FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        if let Ok(_) = &zipwriter_rdb_out.start_file(&RDB_PATH_INDEX_READS, zipwriter_opts) {
            writeln!(&mut zipwriter_rdb_out, "").expect("Could not write to rdb archive");
        }

        let (thread_pool_write, thread_pool_read) = (
            threadpool::ThreadPool::new(threads_write),
            rust_htslib::tpool::ThreadPool::new(threads_read)?,
        );
        let thread_paths_out: Vec<PathBuf> = (0..threads_write)
        .map(|index| {
            self.path_tmp
            .join(format!("rdb-{}", index))
        })
        .collect();
    
    let thread_states: Vec<state::Threading> = thread_paths_out
    .iter()
    .map(|path| {
        let file = File::create(path).unwrap();
        return state::Threading::new(file);
    })
    .collect();


        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            min_reads_per_cell: self.min_reads_per_cell,
        };
        let params_threading = params::Threading {
            threads_write,
            threads_read,
        };

        let _ = BAMProcessor::extract_cells(
            &Arc::new(params_io),
            &Arc::new(params_runtime),
            &Arc::new(params_threading),
            &Arc::new(thread_states),
            &thread_pool_read,
            &thread_pool_write,
        );
    
        let _ = match std::process::Command::new("zipmerge")
            .arg("-i")
            .arg(&path_rdb_out)
            .args(&thread_paths_out)
            .output()
        {
            Ok(_) => {}
            Err(e) => panic!("Failed to execute zipmerge command: {}", e),
        };

        Ok(())
    }

    // fn ensure_paths(&self) -> Result<()> {
    //     if let Ok(file) = File::open(&self.path_in) {
    //         if file.metadata()?.len() == 0 {
    //             anyhow::bail!("Empty input file");
    //         }
    //         match self.path_in.extension().and_then(|ext| ext.to_str()) {
    //             Some("cram" | "bam") => return Ok(()),
    //             _ => anyhow::bail!("Input file must be a cram or bam file"),
    //         };
    //     } else {
    //         anyhow::bail!("Input file does not exist or cannot be opened");
    //     }
    // }

    fn resolve_thread_config(&self) -> Result<(u32, usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get() as u32;

        if available_threads < 2 {
            anyhow::bail!("At least two threads must be available");
        }

        let (threads_read, threads_write) = match (self.threads_read, self.threads_write) {
            (Some(r), Some(w)) => (r, w),
            (Some(r), None) => (r, (available_threads.saturating_sub(r) as usize).max(1)),
            (None, Some(w)) => (1, w),
            (None, None) => (1, (available_threads.saturating_sub(1) as usize).max(1)),
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
