use crate::command::prepare::constants::{
    PREPARE_DEFAULT_THREADS_READ, PREPARE_DEFAULT_THREADS_WRITE,
};

use super::{
    constants::{
        PREPARE_DEFAULT_ASSEMBLE, PREPARE_DEFAULT_CLEANUP, PREPARE_DEFAULT_MIN_READS_PER_CELL,
        PREPARE_DEFAULT_PATH_OUT,
    },
    core::{core::BAMProcessor, params},
};
use anyhow::Result;
use clap::Args;
use clio::{Input, Output};
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    thread,
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser)]
    path_in: Input,

    #[arg(short = 't', value_parser, default_value = PREPARE_DEFAULT_PATH_OUT)]
    path_tmp: PathBuf,

    #[arg(short = 'o', value_parser, default_value = PREPARE_DEFAULT_PATH_OUT)]
    path_out: Output,

    #[arg(value_parser, default_value_t = PREPARE_DEFAULT_MIN_READS_PER_CELL)]
    min_reads_per_cell: usize,

    #[arg(long, default_value_t = PREPARE_DEFAULT_ASSEMBLE)]
    assemble: bool,

    #[arg(long, default_value_t = PREPARE_DEFAULT_CLEANUP)]
    cleanup: bool,

    #[arg(long, value_parser = clap::value_parser!(u32))]
    threads_read: Option<u32>,

    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_write: Option<usize>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        // self.verify_input_file()?;

        let (threads_read, threads_write) = self.resolve_thread_config()?;

        let (thread_pool_write, thread_pool_read) = (
            threadpool::ThreadPool::new(threads_write),
            rust_htslib::tpool::ThreadPool::new(threads_read)?,
        );
        let processor = BAMProcessor::new(
            params::IO {
                path_in: self.path_in.path().to_path_buf(),
                path_tmp: self.path_tmp.clone(),
                path_out: Arc::new(RwLock::new(self.path_out.clone())),
            },
            params::Runtime {
                assemble: self.assemble,
                cleanup: self.cleanup,
                min_reads: self.min_reads_per_cell,
            },
            params::Threading {
                threads_write,
                threads_read,
                thread_pool_write: &thread_pool_write,
                thread_pool_read: &thread_pool_read,
            },
        );
        let _ = processor.process_bam();
        Ok(())
    }

    // fn verify_input_file(&self) -> Result<()> {
    //     if self.path_in.is_std() {
    //         anyhow::bail!("stdin not supported for now");
    //     }

    //     if self.path_in.get_file()?.metadata()?.len() == 0 {
    //         anyhow::bail!("Empty input file");
    //     }

    //     match self.path_in.path().extension().and_then(|ext| ext.to_str()) {
    //         Some("cram" | "bam") => Ok(()),
    //         _ => anyhow::bail!("Input file must be a CRAM or BAM file"),
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
