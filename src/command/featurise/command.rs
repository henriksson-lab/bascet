use crate::{
    command::constants::DEFAULT_SEED_RANDOM,
    core::constants::{HUGE_PAGE_SIZE, KMC_COUNTER_MAX_DIGITS},
    utils::KMERCodec,
};

use super::{
    constants::{
        FEATURISE_DEFAULT_FEATURES_MAX, FEATURISE_DEFAULT_FEATURES_MIN, FEATURISE_DEFAULT_PATH_IN,
        FEATURISE_DEFAULT_PATH_OUT, FEATURISE_DEFAULT_PATH_TEMP, FEATURISE_DEFAULT_THREADS_READ,
        FEATURISE_DEFAULT_THREADS_WORK,
    },
    core::{core, params},
};
use anyhow::Result;
use clap::Args;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};
#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = FEATURISE_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = FEATURISE_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = FEATURISE_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_FEATURES_MIN)]
    pub features_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_FEATURES_MAX)]
    pub features_nmax: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_THREADS_READ)]
    pub threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_THREADS_WORK)]
    pub threads_work: usize,
    #[arg(long, value_parser = clap::value_parser!(u64), default_value_t = *DEFAULT_SEED_RANDOM)]
    pub seed: u64,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
        };
        let params_threading = params::Threading {
            threads_work: self.threads_work,
        };

        if let Ok(path_dump) = core::KMCProcessor::merge(&params_io, &params_threading) {
            let thread_buffer_size =
                (HUGE_PAGE_SIZE / self.threads_work) - (self.kmer_size + KMC_COUNTER_MAX_DIGITS);
            let thread_states: Vec<crate::state::Threading> = (0..self.threads_work)
                .map(|_| {
                    crate::state::Threading::from_seed(
                        self.seed,
                        thread_buffer_size,
                        self.features_nmin,
                        self.features_nmax,
                    )
                })
                .collect();

            let thread_pool = threadpool::ThreadPool::new(self.threads_work);

            let params_io = crate::core::params::IO { path_in: path_dump };

            let codec = KMERCodec::new(self.kmer_size);
            let params_runtime = crate::core::params::Runtime {
                kmer_size: self.kmer_size,
                features_nmin: self.features_nmin,
                features_nmax: self.features_nmax,
                seed: self.seed,
                codec: codec,
            };
            let params_threading = crate::core::params::Threading {
                threads_read: self.threads_read,
                threads_work: self.threads_work,
                threads_buffer_size: thread_buffer_size,
            };

            let file_out = File::create(&self.path_out).unwrap();
            let mut bufwriter_out = BufWriter::new(&file_out);
            if let Ok((min_features, max_features)) = crate::KMCProcessor::extract(
                &Arc::new(params_io),
                &Arc::new(params_runtime),
                &Arc::new(params_threading),
                &Arc::new(thread_states),
                &thread_pool,
            ) {
                for feature in min_features.iter().chain(max_features.iter()) {
                    let _ = writeln!(bufwriter_out, "{}, {}", (feature << 64) >> 64, unsafe {
                        codec.decode(*feature)
                    });
                }
            }
            bufwriter_out.flush().unwrap();
        }

        Ok(())
    }
}
