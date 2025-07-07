use anyhow::Result;
use clap::Args;
use log::debug;
use std::fs::File;
use std::{
    io::{BufReader, BufWriter},
    path::PathBuf,
};
use zip::ZipArchive;

pub const DEFAULT_PATH_TEMP: &str = "temp";

#[derive(Args)]
pub struct ExtractCMD {
    #[arg(short = 'i', value_parser)]
    /// Zip-file name. Note that this command takes a shard, not a full bascet (can support later!) -- this is for speed
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]
    /// Full path to file to store in
    pub path_out: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    #[arg(short = 'b', value_parser)]
    /// Cell barcode
    pub cell_id: String,

    #[arg(short = 'f', value_parser)]
    /// Name of file
    pub fname: String,
}

impl ExtractCMD {
    /// Run the commandline option.
    /// This one enables the extraction of a single file from a Bascet ZIP archive
    pub fn try_execute(&mut self) -> Result<()> {
        //Just unzip list of files from zip. This way we can be sure to support fancier compression methods and be sure to be compatible with R

        let file = File::open(&self.path_in).expect("Failed to open bascet shard");
        let bufreader_shard = BufReader::new(file);
        let mut zip_shard = ZipArchive::new(bufreader_shard).unwrap();

        let zip_fname = format!("{}/{}", self.cell_id, self.fname);

        let mut entry = zip_shard.by_name(&zip_fname).expect("File is not present");

        if entry.is_file() {
            debug!("extracting {} ", self.fname);

            let file_out = File::create(&self.path_out).unwrap();
            let mut bufwriter_out = BufWriter::new(&file_out);
            let mut bufreader_found = BufReader::new(&mut entry);
            std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();
        }

        log::info!("Extract has finished succesfully");
        Ok(())
    }
}
