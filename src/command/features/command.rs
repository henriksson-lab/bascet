use crate::constants::{HUGE_PAGE_SIZE, OVLP_DIGITS};
use anyhow::Result;
use clap::Args;
use clio::{Input, Output};
use fs2::FileExt;
use std::{fs::File, sync::Arc, thread};

use super::{
    constants::{
        MARKERS_DEFAULT_FEATURES_MAX, MARKERS_DEFAULT_FEATURES_MIN, MARKERS_DEFAULT_PATH_IN,
        MARKERS_DEFAULT_PATH_OUT,
    },
    core::{extract_features, DefaultThreadState, ParamsIO, ParamsRuntime, ParamsThreading},
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = MARKERS_DEFAULT_PATH_IN)]
    pub path_in: Input,

    #[arg(short = 'o', value_parser, default_value = MARKERS_DEFAULT_PATH_OUT)]
    pub path_out: Output,

    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value = MARKERS_DEFAULT_FEATURES_MIN)]
    pub features_nmin: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value = MARKERS_DEFAULT_FEATURES_MAX)]
    pub features_nmax: usize,

    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_io: Option<usize>,

    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_work: Option<usize>,

    #[arg(long, value_parser = clap::value_parser!(u64))]
    pub seed: Option<u64>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        self.verify_input_file()?;
        let kmer_size = self.verify_kmer_size()?;
        let (features_nmin, features_nmax) = self.verify_features()?;
        let (threads_io, threads_work) = self.resolve_thread_config()?;

        let seed = self.seed.unwrap_or_else(rand::random);
        let file_in = self.path_in.get_file().unwrap();
        let lock = file_in.lock_shared();

        let thread_pool = threadpool::ThreadPool::new(threads_io + threads_work);
        // HACK: Did not implement a good way to get huge page size.
        //       buffer_size takes into account the thread count and the overlap window.
        // NOTE: Maximum amount of characters in counts column, including seperator
        let buffer_size = (HUGE_PAGE_SIZE / threads_work) - ((kmer_size as usize) + OVLP_DIGITS);
        let thread_states: Vec<Arc<DefaultThreadState>> = (0..threads_work)
            .map(|_| {
                Arc::new(DefaultThreadState::from_seed(
                    seed,
                    buffer_size,
                    features_nmin,
                    features_nmax,
                ))
            })
            .collect();

        let result = extract_features(
            ParamsIO {
                file_in,
                path_out: &mut self.path_out,
            },
            ParamsRuntime {
                kmer_size: kmer_size,
                ovlp_size: kmer_size + OVLP_DIGITS,
                features_nmin: features_nmin,
                features_nmax: features_nmax,
                seed: seed,
            },
            ParamsThreading {
                threads_io: threads_io,
                threads_work: threads_work,
                thread_buffer_size: buffer_size,
                thread_pool: &thread_pool,
                thread_states: &thread_states
            },
        );

        drop(lock);

        return result;
    }

    fn verify_input_file(&mut self) -> Result<()> {
        if self.path_in.is_std() {
            anyhow::bail!("stdin not supported for now");
        }
        if self.path_in.get_file().unwrap().metadata()?.len() == 0 {
            anyhow::bail!("Empty input file");
        }
        match self.path_in.path().extension().and_then(|ext| ext.to_str()) {
            Some("cram" | "bam") => Ok(()),
            _ => anyhow::bail!("Input file must be a CRAM or BAM file"),
        }
    }

    fn verify_features(&self) -> Result<(usize, usize)> {
        if self.features_nmin == 0 && self.features_nmax == 0 {
            anyhow::bail!("Both features_nmin and features_nmax cannot be 0");
        }

        Ok((self.features_nmin, self.features_nmax))
    }

    fn verify_kmer_size(&self) -> Result<usize> {
        if 0 > self.kmer_size &&  self.kmer_size > 48 {
            return Ok(self.kmer_size);
        } 

        anyhow::bail!("Invalid kmer size");
    }

    fn resolve_thread_config(&self) -> Result<(usize, usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 2 {
            anyhow::bail!("At least two threads must be available");
        }

        let (threads_io, threads_work) = match (self.threads_io, self.threads_work) {
            (Some(i), Some(w)) => (i, w),
            (Some(i), None) => (i, available_threads.saturating_sub(i).max(1)),
            (None, Some(w)) => (1, w),
            (None, None) => (1, available_threads.saturating_sub(1).max(1)),
        };

        if threads_io == 0 {
            anyhow::bail!("At least one IO thread required");
        }
        if threads_work == 0 {
            anyhow::bail!("At least one work thread required");
        }

        Ok((threads_io, threads_work))
    }
}
