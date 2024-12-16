use crate::{constants::HUGE_PAGE_SIZE, utils::KMERCodec};
use anyhow::Result;
use clap::Args;
use clio::{Input, Output};
use fs2::FileExt;
use linya::Progress;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    thread,
};
use zip::ZipArchive;

use super::{
    constants::{
        OVLP_DIGITS, QUERY_DEFAULT_FEATURES_QUERY_MAX, QUERY_DEFAULT_FEATURES_QUERY_MIN,
        QUERY_DEFAULT_FEATURES_REF_MAX, QUERY_DEFAULT_FEATURES_REF_MIN, QUERY_DEFAULT_PATH_IN,
        QUERY_DEFAULT_PATH_OUT,
    },
    core::{core::KMCProcessor, params, threading::DefaultThreadState},
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = QUERY_DEFAULT_PATH_IN)]
    pub path_in: Input,
    #[arg(short = 'j', value_parser, default_value = QUERY_DEFAULT_PATH_IN)]
    pub path_index: Input,
    #[arg(short = 't', value_parser, default_value = QUERY_DEFAULT_PATH_IN)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = QUERY_DEFAULT_PATH_OUT)]
    pub path_out: Output,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_REF_MIN)]
    pub features_ref_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_REF_MAX)]
    pub features_ref_nmax: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_QUERY_MIN)]
    pub features_query_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_QUERY_MAX)]
    pub features_query_nmax: usize,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_io: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_work: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(u64))]
    pub seed: Option<u64>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        // TODO: actually implement the CRAM->BAM/fastq.gz
        self.verify_input_file()?;
        let kmer_size = self.verify_kmer_size()?;
        let (query_features_nmin, query_features_nmax, ref_features_nmin, ref_features_nmax) =
            self.verify_features()?;
        let (threads_io, threads_work) = self.resolve_thread_config()?;
        let seed = self.seed.unwrap_or_else(rand::random);
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
                    query_features_nmin,
                    query_features_nmax,
                ))
            })
            .collect();
        let zip_file = File::open(self.path_in.path().to_path_buf())?;
        let mut archive = ZipArchive::new(zip_file)?;
        let index_reader = BufReader::new(File::open(self.path_index.path().to_path_buf())?);

        let mut index_file = File::open(self.path_index.path().to_path_buf())?;
        index_file.seek(SeekFrom::End(-128))?; // Seek back just enough for last few lines
        let reader = BufReader::new(index_file);
        let lines: Vec<_> = reader.lines().collect();
        let total = lines[lines.len() - 2]
            .as_ref()
            .unwrap()
            .split(',')
            .next()
            .unwrap()
            .parse::<usize>()?;

        // create an empty db to merge with
        let path_empty = self
            .path_tmp
            .join("empty")
            .join("reads")
            .with_extension("fastq");
        let path_empty_kmc = self.path_tmp.join("empty").join("kmc");
        fs::create_dir_all(&path_empty_kmc)?;
        let kmc_empty = File::create(&path_empty)?;
        let kmc = std::process::Command::new("kmc")
            .arg(format!("-cs{}", u32::MAX - 1))
            .arg(format!("-k{}", &self.kmer_size))
            .arg(&path_empty)
            .arg(&path_empty_kmc)
            .arg(&self.path_tmp)
            .output()?;

        let params_runtime = Arc::new(params::Runtime {
            kmer_size: kmer_size,
            ovlp_size: kmer_size + OVLP_DIGITS,
            features_nmin: query_features_nmin,
            features_nmax: query_features_nmax,
            codec: KMERCodec::new(kmer_size),
            seed: seed,
        });
        let params_threading = Arc::new(params::Threading {
            threads_io: threads_io,
            threads_work: threads_work,
            thread_buffer_size: buffer_size,
            thread_pool: &thread_pool,
            thread_states: &thread_states,
        });

        let mut progress = Progress::new();
        let bar = progress.bar(total, "Extracting files");

        for line in index_reader.lines() {
            let line = line?;
            let index: usize = line
                .split(',')
                .next()
                .ok_or_else(|| anyhow::anyhow!("Error parsing index file"))?
                .parse()?;

            if index == 0 {
                progress.inc_and_draw(&bar, 1);
                continue;
            }

            let mut file = archive.by_index(index)?;
            let out_path = self.path_tmp.join(file.name());
            let dir_path = out_path.parent().unwrap();
            let _ = fs::create_dir_all(&dir_path);
            let mut out_file = File::create(&out_path)?;
            std::io::copy(&mut file, &mut out_file)?;

            let kmc_path_db = dir_path.join("kmc");
            let kmc = std::process::Command::new("kmc")
                .arg(format!("-cs{}", u32::MAX - 1))
                .arg(format!("-k{}", &self.kmer_size))
                .arg(&out_path)
                .arg(&kmc_path_db)
                .arg(&self.path_tmp)
                .output()?;

            if !kmc.status.success() {
                anyhow::bail!("KMC failed: {}", String::from_utf8_lossy(&kmc.stderr));
            }

            let kmc_union = std::process::Command::new("kmc_tools")
                .arg("simple")
                .arg(&path_empty_kmc)
                .arg(&out_path)
                .arg(&path_empty_kmc)
                .arg("union")
                .arg(&self.path_tmp.join("union"))
                .output()?;

            if !kmc_union.status.success() {
                anyhow::bail!(
                    "KMC merge failed: {}",
                    String::from_utf8_lossy(&kmc_union.stderr)
                );
            }

            // let file_dump = File::open(&kmc_path_dump)?;
            // let lock = file_dump.lock_shared();
            // let params_io = Arc::new(params::IO {
            //     file_in: &file_dump,
            //     path_out: &mut self.path_out,
            // });
            // let _ = KMCProcessor::extract(params_io, params_runtime, params_threading);
            // drop(lock);
            let _ = fs::remove_dir_all(&dir_path);
            progress.inc_and_draw(&bar, 1);

            //TODO//NOTE: REMEMBER TO RESET THREAD STATES!!!
        }
        Ok(())
    }

    fn verify_input_file(&mut self) -> Result<()> {
        if self.path_in.is_std() {
            anyhow::bail!("stdin not supported for now");
        }
        if self.path_in.get_file().unwrap().metadata()?.len() == 0 {
            anyhow::bail!("Empty input file");
        }
        match self.path_in.path().extension().and_then(|ext| ext.to_str()) {
            Some("zip") => Ok(()),
            _ => anyhow::bail!("Input file must be a robert database (zip)"),
        }
    }

    fn verify_features(&self) -> Result<(usize, usize, usize, usize)> {
        if self.features_ref_nmin == 0 && self.features_ref_nmax == 0 {
            anyhow::bail!("Ref features_nmin and features_nmax cannot be 0");
        }
        if self.features_query_nmin == 0 && self.features_query_nmax == 0 {
            anyhow::bail!("Query features_nmin and features_nmax cannot be 0");
        }

        Ok((
            self.features_ref_nmin,
            self.features_ref_nmax,
            self.features_ref_nmin,
            self.features_ref_nmax,
        ))
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
