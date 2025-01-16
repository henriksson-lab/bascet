use std::path::PathBuf;
use anyhow::Result;
use clap::Args;
use std::sync::Arc;

use crate::{command::mapcell, mapcell::MapCellFunctionShellScript};
use crate::mapcell::MapCellFunction;

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS_READ: usize = 1;
pub const DEFAULT_THREADS_WRITE: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 1;


#[derive(Args)]
pub struct MapCellCMD {
    // Input bascet, TIRP etc
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: Option<PathBuf>,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: Option<PathBuf>,


    //The script to run
    #[arg(short = 's', value_parser = clap::value_parser!(PathBuf))]
    pub path_script: PathBuf,

    //If we should show script output in terminal
    #[arg(long = "show-script-output")]
    pub show_script_output: bool,


    //Show a list of preset scripts available
    #[arg(long = "show-presets")]
    pub show_presets: bool,

    //Keep files extracted for the script. For debugging purposes
    #[arg(long = "keep-files")]
    pub keep_files: bool,

    //TODO: allow a pre-filter script
    //TODO: allow a post-filter script

    //TODO: call script to check if needed commands are present

    //Thread settings
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,

    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WRITE)]
    threads_write: usize,
    
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
}



impl MapCellCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        if self.show_presets {
            let names = crate::mapcell_scripts::get_preset_script_names();
            println!("Available preset scripts: {:?}", names);
            return Ok(());
        }

        //Figure out what script to use.
        //Check if using a new script or a preset. user scripts start with _
        //let path_script = self.path_script;
        let preset_name = self.path_script.to_str().expect("argument conversion error");
        let script: Arc<Box<dyn MapCellFunction>> = if preset_name.starts_with("_") {
            println!("using preset {:?}", self.path_script);
            let preset_name=&preset_name[1..]; //Remove the initial _  ; or capital letter? 
            crate::mapcell_scripts::get_preset_script(preset_name).expect("Unable to load preset script")            
        } else {
            println!("Using user provided script");
            let s = MapCellFunctionShellScript::new_from_file(&self.path_script).expect("Failed to load user defined script");
            Arc::new(Box::new(s))
        };


        let params = mapcell::MapCellParams {
            
            path_in: self.path_in.as_ref().expect("Input file was not provided").clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.as_ref().expect("Output file was not provided").clone(),
            script: script,

            threads_read: self.threads_read,
            threads_write: self.threads_write,
            threads_work: self.threads_work,

            show_script_output: self.show_script_output,
            keep_files: self.keep_files            
        };

        let _ = mapcell::MapCell::run(params).expect("mapcell failed");

        println!("Mapcell has finished!");
        Ok(())
    }
}

