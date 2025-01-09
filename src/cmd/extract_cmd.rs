use anyhow::Result;
use clap::Args;
use std::fs::File;
use std::{
    io::{BufReader, BufWriter},
    path::PathBuf,
};
use zip::ZipArchive;
use log::debug;


#[derive(Args)]
pub struct ExtractCMD {
    #[arg(short = 'i', value_parser)]  /// Zip-file name. Note that this command takes a shard, not a full bascet (can support later!) -- this is for speed
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]  /// Full path to file to store in
    pub path_out: PathBuf,

    #[arg(short = 'b', value_parser)]  /// Cell barcode
    pub cell_id: String,

    #[arg(short = 'f', value_parser)]  /// Name of file
    pub fname: String,

//    #[arg(trailing_var_arg = true)]  //, allow_hyphen_values = true, hide = true
//    pub remaining_args: Vec<String>,
}

impl ExtractCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        //Just unzip list of files from zip. This way we can be sure to support fancier compression methods and be sure to be compatible with R


        let file = File::open(&self.path_in).expect("Failed to open bascet shard");
        let bufreader_shard = BufReader::new(file);
        let mut zip_shard =    ZipArchive::new(bufreader_shard).unwrap();

        let zip_fname = format!("{}/{}",self.cell_id, self.fname);

        let mut entry = zip_shard.by_name(&zip_fname).expect("File is not present");

        
        if entry.is_file() {
            debug!("extracting {} ", self.fname); 
                            
            let file_out = File::create(&self.path_out).unwrap();
            let mut bufwriter_out = BufWriter::new(&file_out);
            let mut bufreader_found = BufReader::new(&mut entry);
            std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();
        }

        //Example call in Zorn
        //        unzip(name_of_zip, files=extract_files, exdir=tname.dir) ### 666 cannot operate on our rust files. 

        
        Ok(())
    }
}
