use anyhow::Result;
use clap::Args;
use std::{fs::File, path::PathBuf, sync::Arc};

use super::{
    constants::{
        COUNT_DEFAULT_PATH_IN, COUNT_DEFAULT_PATH_OUT, COUNT_DEFAULT_PATH_TEMP,
        COUNT_DEFAULT_THREADS_READ, COUNT_DEFAULT_THREADS_WORK,
    },
    core::{core::RDBCounter, params, state},
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = COUNT_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = COUNT_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = COUNT_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = COUNT_DEFAULT_THREADS_READ)]
    pub threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = COUNT_DEFAULT_THREADS_WORK)]
    pub threads_work: usize,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let thread_pool = threadpool::ThreadPool::new(self.threads_read + self.threads_work);
        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            kmer_size: self.kmer_size,
        };
        let params_threading = params::Threading {
            threads_read: self.threads_read,
            threads_work: self.threads_read,
        };

        let paths_threading_temp_out: Vec<PathBuf> = (0..self.threads_work)
            .map(|index| self.path_tmp.join(format!("temp-{}.rdb", index)))
            .collect();

        let thread_states: Vec<state::Threading> = paths_threading_temp_out
            .iter()
            .map(|path| {
                let file = File::create(path).unwrap();
                return state::Threading::new(file);
            })
            .collect();

        let _ = RDBCounter::extract(
            &Arc::new(params_io),
            &Arc::new(params_runtime),
            &Arc::new(params_threading),
            &Arc::new(thread_states),
            &thread_pool,
        );

        Ok(())
    }
}
