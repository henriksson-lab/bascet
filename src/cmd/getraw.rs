use anyhow::Result;
use clap::Args;
use std::{
    fs::File,
    path::PathBuf,
    sync::Arc,
    thread,
};

pub const GETRAW_DEFAULT_PATH_TEMP: &str = "temp";


use crate::command::getraw::GetRaw;
use crate::command::getraw::GetRawParams;


#[derive(Args)]
pub struct GetRawCMD {
    // FASTQ for r1
    #[arg(long = "i1", value_parser)]
    pub path_forward: PathBuf,

    // FASTQ for r2
    #[arg(long = "i2", value_parser)]
    pub path_reverse: PathBuf,

    // Output filename for complete reads
    #[arg(short = 'o', long="out-complete", value_parser)]
    pub path_output_complete: PathBuf,

    // Output filename for incomplete reads
    #[arg(long = "out-incomplete", value_parser)]
    pub path_output_incomplete: PathBuf, 

    // Optional: file with barcodes to use
    #[arg(long = "bc", value_parser)]
    pub barcode_file: Option<PathBuf>, 


    // Temporary file directory. TODO - use system temp directory as default? TEMP variable?
    #[arg(short = 't', value_parser, default_value = GETRAW_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    //Whether to sort or not
    #[arg(long = "no-sort")]
    pub no_sort: bool,  

    // Optional: How many threads to use. Need better way of specifying? TODO
    #[arg(long, value_parser = clap::value_parser!(usize))]
    threads_work: Option<usize>,
}

impl GetRawCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        verify_input_fq_file(&self.path_forward)?;
        verify_input_fq_file(&self.path_reverse)?;

        let threads_work = self.resolve_thread_config()?;

        let params_io = GetRawParams {

            path_tmp: self.path_tmp.clone(),            
            path_forward: self.path_forward.clone(),            
            path_reverse: self.path_reverse.clone(),            
            path_output_complete: self.path_output_complete.clone(),            
            path_output_incomplete: self.path_output_incomplete.clone(),            
            barcode_file: self.barcode_file.clone(),            
            sort: !self.no_sort,            
            threads_work: threads_work,
        };

        //fs::create_dir_all(&self.path_out).unwrap();

        let _ = GetRaw::getraw(
            Arc::new(params_io)
        );

        Ok(())
    }





    ///////  todo keep this or not?
    fn resolve_thread_config(&self) -> Result<usize> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 2 {
            println!("Warning: less than two threads reported to be available");
        }

        Ok(available_threads-1)
    }


}



/////// Check that the specified file is a fastq file
fn verify_input_fq_file(path_in: &PathBuf) -> anyhow::Result<()> {
    if let Ok(file) = File::open(&path_in) {
        if file.metadata()?.len() == 0 {
            //anyhow::bail!("Empty input file");
            print!("Warning: input file is empty");
        }
    }

    let filename = path_in.file_name().unwrap().to_str().unwrap();

    if filename.ends_with("fq") | filename.ends_with("fq.gz") | filename.ends_with("fastq.gz") | filename.ends_with("fastq.gz")  {
        //ok
    } else {
        anyhow::bail!("Input file must be a fastq file")
    }

    Ok(())
}
