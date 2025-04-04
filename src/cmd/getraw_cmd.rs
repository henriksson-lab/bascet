use anyhow::Result;
use anyhow::bail;

use clap::Args;
use std::{
    path::PathBuf,
    sync::Arc,
    thread,
};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_CHEMISTRY: &str = "atrandi_wgs";


use crate::command::getraw::GetRaw;
use crate::command::getraw::GetRawParams;

use crate::barcode::PetriseqChemistry;
use crate::barcode::AtrandiWGSChemistry;
use crate::barcode::AtrandiRNAseqChemistry;
use crate::barcode::GeneralCombinatorialBarcode;


#[derive(Args)]
pub struct GetRawCMD {
    // FASTQ for r1
    #[arg(long = "r1", value_parser)]
    pub path_forward: PathBuf,

    // FASTQ for r2
    #[arg(long = "r2", value_parser)]
    pub path_reverse: PathBuf,

    // Output filename for complete reads
    #[arg(short = 'o', long="out-complete", value_parser)]
    pub path_output_complete: PathBuf,

    // Output filename for incomplete reads
    #[arg(long = "out-incomplete", value_parser)]
    pub path_output_incomplete: PathBuf, 

    // Optional: chemistry with barcodes to use
    #[arg(long = "chemistry", value_parser, default_value = DEFAULT_CHEMISTRY)]
    pub chemistry: String, 

    // Optional: file with barcodes to use
    #[arg(long = "barcodes", value_parser)]
    pub path_barcodes: Option<PathBuf>, 

    // Optional: Prepend library name to barcodes
    #[arg(long = "libname", value_parser)]
    pub libname: Option<String>,

    // Temporary file directory. TODO - use system temp directory as default? TEMP variable?
    #[arg(short = 't', value_parser, default_value = DEFAULT_PATH_TEMP)]
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

        crate::fileformat::verify_input_fq_file(&self.path_forward)?;
        crate::fileformat::verify_input_fq_file(&self.path_reverse)?;

        let threads_work = self.resolve_thread_config()?;

        //Set default libname
        let libname = if let Some(libname)=&self.libname {
            libname.clone()
        } else {
            "".to_string()
        };


        let params_io = GetRawParams {

            path_tmp: self.path_tmp.clone(),            
            path_forward: self.path_forward.clone(),            
            path_reverse: self.path_reverse.clone(),            
            path_output_complete: self.path_output_complete.clone(),            
            path_output_incomplete: self.path_output_incomplete.clone(),            
            libname: libname,
            //barcode_file: self.barcode_file.clone(),            
            sort: !self.no_sort,            
            threads_work: threads_work,
        };

        // Start the debarcoding for specified chemistry
        if self.chemistry == "atrandi_wgs" {
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut AtrandiWGSChemistry::new()
            );
        } else if self.chemistry == "atrandi_rnaseq" {
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut AtrandiRNAseqChemistry::new()
            );
        } else if self.chemistry == "petriseq" {
            let _ = GetRaw::getraw(
                Arc::new(params_io),
                &mut PetriseqChemistry::new()
            );
        } else if self.chemistry == "combinatorial" {
            if let Some(path_barcodes) = &self.path_barcodes {
                let _ = GetRaw::getraw(
                    Arc::new(params_io),
                    &mut GeneralCombinatorialBarcode::new(&path_barcodes)
                );
            } else {
                bail!("Barcode file not specified");
            }
        } else if self.chemistry == "10x" {
            panic!("not implemented");
        } else if self.chemistry == "parsebio" {
            panic!("not implemented");

        } else {
            bail!("Unidentified chemistry");
        }

        log::info!("GetRaw has finished succesfully");
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




