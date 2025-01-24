use anyhow::Result;
use clap::Args;
use std::path::PathBuf;
use crate::command::bam2fragments::Bam2Fragments;
use crate::command::bam2fragments::Bam2FragmentsParams;

pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct Bam2FragmentsCMD {
    #[arg(short = 'i', value_parser)]  /// BAM or CRAM file; sorted, indexed? unless cell_id's given, no need for sorting
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]  /// Full path to file to store in
    pub path_out: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)] //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,
    
}

impl Bam2FragmentsCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        //TODO Can check that input file is sorted via header

        Bam2Fragments::run(& Bam2FragmentsParams {
            path_input: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_output: self.path_out.clone(),

        }).unwrap();

        log::info!("Bam2Fragments has finished succesfully");
        Ok(())
    }
}
