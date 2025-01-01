use anyhow::Result;
use clap::Args;
use std::{
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
    thread,
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
    pub path_forward: PathBuf,
    #[arg(short = 'j', value_parser)]
    pub path_reverse: PathBuf,
    #[arg(short = 'o', value_parser)]
    pub path_output_complete: PathBuf,
    #[arg(short = 'p', value_parser)]
    pub path_output_incomplete: PathBuf, /////// should we make this optional?
    #[arg(short = 'b', value_parser)]
    pub barcode_file: Option<PathBuf>, 
    #[arg(short = 't', value_parser, default_value = GETRAW_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 's', value_parser)]
    pub sort: bool,  
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_work: Option<usize>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {

        verify_input_fq_file(&self.path_forward)?;
        verify_input_fq_file(&self.path_reverse)?;
         
        //let kmer_size = self.verify_kmer_size()?;
        //let (threads_read, threads_write, threads_work) = self.resolve_thread_config()?;

        //let (threads_read, threads_write, threads_work) = self.resolve_thread_config()?;

        let threads_work = self.resolve_thread_config()?;

        let params_io = params::IO {

            path_tmp: self.path_tmp.clone(),            
            path_forward: self.path_forward.clone(),            
            path_reverse: self.path_reverse.clone(),            
            path_output_complete: self.path_output_complete.clone(),            
            path_output_incomplete: self.path_output_incomplete.clone(),            
            barcode_file: self.barcode_file.clone(),            
            sort: self.sort,            
        };
        let params_runtime = params::Runtime {
            //kmer_size: kmer_size,
        };


        let params_threading = params::Threading {
            threads_work: threads_work,
        };

        //fs::create_dir_all(&self.path_out).unwrap();

        let threads_read = 1;
        let threads_write = 1;
        let thread_pool = threadpool::ThreadPool::new(threads_read + threads_write + threads_work);


        /*

        let _ = RDBAssembler::assemble(
            Arc::new(params_io),
            Arc::new(params_runtime),
            Arc::new(params_threading),
            &thread_pool,
        );
        */

        Ok(())
    }






    fn resolve_thread_config(&self) -> Result<(usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 2 {
            println!("Warning: less than two threads reported to be available");
        }

        Ok(available_threads-1)
    }


}




fn verify_input_fq_file(path_in: &PathBuf) -> anyhow::Result<()> {
    if let Ok(file) = File::open(&path_in) {
        if file.metadata()?.len() == 0 {
            //anyhow::bail!("Empty input file");
            print!("Warning: input file is empty");
        }
    }
    match path_in.extension().and_then(|ext| ext.to_str()) {
        Some("fq") => return Ok(()),
        Some("fastq") => return Ok(()),
        Some("fq.gz") => return Ok(()),
        Some("fastq.gz") => return Ok(()),
        _ => anyhow::bail!("Input file must be a fastq file"),
    };
}
