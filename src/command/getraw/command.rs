use anyhow::Result;
use clap::Args;
use std::{
    fs::{self, File},
    path::PathBuf,
  //  sync::Arc,
  //  thread,
};

use super::{
    constants::{
        GETRAW_DEFAULT_PATH_TEMP,
    },
    core::{core::GetRaw, params},
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = GETRAW_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser)]
    pub path_out: PathBuf,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_work: Option<usize>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {

        self.verify_input_file()?;

         
        //let kmer_size = self.verify_kmer_size()?;
        //let (threads_read, threads_write, threads_work) = self.resolve_thread_config()?;

        let params_io = params::IO {
            path_in: self.path_in.clone(),
            //path_idx: self.path_index.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            //kmer_size: kmer_size,
        };


        /*
        let params_threading = params::Threading {
            threads_read: threads_read,
            threads_write: threads_write,
            threads_work: threads_work,
        };
        fs::create_dir_all(&self.path_out).unwrap();

        let thread_pool = threadpool::ThreadPool::new(threads_read + threads_write + threads_work);
        let _ = RDBAssembler::assemble(
            Arc::new(params_io),
            Arc::new(params_runtime),
            Arc::new(params_threading),
            &thread_pool,
        );
        */

        Ok(())
    }



    fn verify_input_file(&mut self) -> anyhow::Result<()> {
        if let Ok(file) = File::open(&self.path_in) {
            if file.metadata()?.len() == 0 {
                //anyhow::bail!("Empty input file");
                print!("Warning: input file is empty");
            }
        }
        match self.path_in.extension().and_then(|ext| ext.to_str()) {
            Some("fq") => return Ok(()),
            Some("fastq") => return Ok(()),
            Some("fq.gz") => return Ok(()),
            Some("fastq.gz") => return Ok(()),
            _ => anyhow::bail!("Input file must be a fastq file"),
        };
    }


    /* 

    fn verify_kmer_size(&self) -> Result<usize> {
        if self.kmer_size < 48 {
            return Ok(self.kmer_size);
        }

        anyhow::bail!("Invalid kmer size k:{}", self.kmer_size);
    }

    fn resolve_thread_config(&self) -> Result<(usize, usize, usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 3 {
            anyhow::bail!("At least three threads must be available"); ///////// can this ever happen?? todo 
        }

        let (threads_read, threads_write, threads_work) =
            match (self.threads_read, self.threads_write, self.threads_work) {
                (Some(r), Some(w), Some(work)) => (r, w, work),
                (Some(r), Some(w), None) => {
                    let io_threads = r + w;
                    (r, w, available_threads.saturating_sub(io_threads).max(1))
                }
                (Some(r), None, Some(work)) => (r, 1, work),
                (None, Some(w), Some(work)) => (1, w, work),
                (Some(r), None, None) => {
                    let io_threads = r + 1; // 1 for write
                    (r, 1, available_threads.saturating_sub(io_threads).max(1))
                }
                (None, Some(w), None) => {
                    let io_threads = 1 + w; // 1 for read
                    (1, w, available_threads.saturating_sub(io_threads).max(1))
                }
                (None, None, Some(work)) => (1, 1, work),
                (None, None, None) => {
                    let io_threads = 2; // 1 for read, 1 for write
                    (1, 1, available_threads.saturating_sub(io_threads).max(1))
                }
            };

        if threads_read == 0 {
            anyhow::bail!("At least one read thread required");
        }
        if threads_write == 0 {
            anyhow::bail!("At least one write thread required");
        }
        if threads_work == 0 {
            anyhow::bail!("At least one work thread required");
        }

        Ok((threads_read, threads_write, threads_work))
    }


    */
}
