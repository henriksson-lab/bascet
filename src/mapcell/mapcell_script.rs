use anyhow::bail;
use std::process;
use std::path::Path;
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::io::Read;
use std::fmt;
use rand::Rng;

use path_clean::PathClean;

use super::MapCellFunction;
use super::MissingFileMode;
use super::CompressionMode;

use super::parse_missing_file_mode;
use super::parse_compression_mode;

use std::{thread, time};







#[derive(Clone, Debug)]  
pub struct MapCellFunctionShellScript {
    script_file: PathBuf,
    api_version: String,
    expect_files: Vec<String>,
    missing_file_mode: MissingFileMode,
    compression_mode: CompressionMode
}
impl MapCellFunctionShellScript {


    pub fn new_from_reader(preset_script_code: &mut impl Read) -> anyhow::Result<MapCellFunctionShellScript> {


        let mut rng = rand::thread_rng();
        let n2: u16 = rng.gen();

        //Copy the reader content to a new temp file. This file will be deleted upon exit. Wrapping in {} to force operation to be done at the end
        let path_script = PathBuf::from(format!("./_temp_script.{}.sh", n2));//canonicalize().expect("Failed to get full temp script path");
        {
            let mut script_file = File::create_new(&path_script).expect("Failed to create temp script file");
            let _ = std::io::copy(preset_script_code, &mut script_file).expect("Failed to copy script to temp file");   
        }
        let path_script=path_script.canonicalize().expect("Failed to get full temp script path");

        //Make the script executable
        let path_script_s = &path_script.clone().into_os_string().into_string().unwrap();
        process::Command::new("chmod")
            .arg("u+x")
            .arg(&path_script_s)
            .output()
            .expect("Failed to get output from chmod");

        //Ugly hack: To avoid running the script before ready. another way is to keep trying to run it until OK
        thread::sleep(time::Duration::from_millis(1000));
        

        //Return script
        println!("Extracted preset script to {:?} and set chmod", &path_script_s);

        //Figure out script metadata
        let api_version = get_script_api_version(&path_script)?;
        let expect_files = get_script_expect_files(&path_script)?;
        let missing_file_mode = get_missing_file_mode(&path_script)?; 
        let compression_mode = get_compression_mode(&path_script)?;

        let script = MapCellFunctionShellScript {
            script_file: path_script,
            api_version: api_version,
            expect_files: expect_files,
            missing_file_mode: missing_file_mode,
            compression_mode: compression_mode
        };


        if !script.preflight_check() {
            anyhow::bail!("Script does not pass pre-flight check")
        } else {
            anyhow::Ok(script)
        }

    }
    

    pub fn new_from_file(f: &PathBuf) -> anyhow::Result<MapCellFunctionShellScript> {
        let mut f = File::open(f).expect("Failed to open script file for reading");
        Self::new_from_reader(&mut f)
    }


}

impl Drop for MapCellFunctionShellScript{
    fn drop(&mut self) {
        _ = std::fs::remove_file(&self.script_file);
    }
}

impl fmt::Display for MapCellFunctionShellScript {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Script API version: {}",self.api_version).unwrap();
        writeln!(f,"Script expects files: {:?}", self.expect_files).unwrap();
        writeln!(f,"Script file missing mode: {}", self.missing_file_mode).unwrap();
        Ok(())
    }
}

impl MapCellFunction for MapCellFunctionShellScript {


    fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        num_threads: usize
    ) -> anyhow::Result<(bool, String)> {

        //Run script in output folder to make life easier for end user
        let input_dir = to_absolute_path(&input_dir).expect("Could not get absolute directory for input");
        let output_dir = to_absolute_path(&output_dir).expect("Could not get absolute directory for output"); 
        let path_script = to_absolute_path(&self.script_file).expect("Could not get absolute directory for script"); 
        
        //Invoke command
        let run_output = process::Command::new(&path_script)
            .current_dir(&output_dir)
            .arg("--num-threads").arg(num_threads.to_string())
            .arg("--input-dir").arg(&input_dir)
            .arg("--output-dir").arg(&output_dir)
            .output()
            .expect(format!("Could not spawn process in mapcell script {:?}", &path_script).as_str());
        let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
        let run_output_string = run_output_string.trim();
        let run_stderr_string = String::from_utf8(run_output.stderr).expect("utf8 error");

        //Check if script ran fine
        let last_line = run_output_string.split("\n").last(); //can this ever fail?
        let success = if let Some(last_line) = last_line { last_line=="MAPCELL-OK" } else { false };

     //   println!("last line");
   //     println!("{:?}",last_line);
        //debug!("last scrip init line {:?}", last_line);

        Ok((success, format!("{}\n{}", run_output_string, run_stderr_string)))
    }  


    fn get_missing_file_mode(&self) -> MissingFileMode {
        self.missing_file_mode
    }

    fn get_compression_mode(&self, _fname: &str) -> CompressionMode {
        self.compression_mode
    }

    fn get_expect_files(&self) -> Vec<String> {
        self.expect_files.clone()
    }

    fn preflight_check(&self) -> bool {
        let run_output = process::Command::new(&self.script_file)
            .arg("--preflight-check")
            .output();

        if let Ok(run_output) = run_output {

            let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
            let run_output_string = run_output_string.trim();

            if run_output_string=="MAPCELL-CHECK" {
                true
            } else {
                println!("{}",run_output_string);
                false
            }
        } else {
            false
        }
    }

}








pub fn get_script_expect_files(path_script: &impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let run_output = process::Command::new(path_script.as_ref())
        .arg("--expect-files")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    let splitter = run_output_string.split(',');
    let list_files: Vec<String> = splitter.map(|s| s.to_string()).collect();
    Ok(list_files)
}



fn get_missing_file_mode(path_script: &impl AsRef<Path>) -> anyhow::Result<MissingFileMode> {
    let run_output = process::Command::new(path_script.as_ref())
        .arg("--missing-file-mode")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    parse_missing_file_mode(run_output_string)
}



fn get_compression_mode(path_script: &impl AsRef<Path>) -> anyhow::Result<CompressionMode> {
    let run_output = process::Command::new(path_script.as_ref())
        .arg("--compression-mode")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    parse_compression_mode(run_output_string)
}



//// Get API version. This is the first call so a lot of checks to help user debug
fn get_script_api_version(path_script: &impl AsRef<Path>) -> anyhow::Result<String> {
    let run_output = process::Command::new(path_script.as_ref())
        .arg("--bascet-api")
        .output();

    if let Ok(run_output) = run_output {
        let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
        let run_output_string = run_output_string.trim();
        let mut splitter = run_output_string.split(' ');
        let first_part = splitter.next();
        if let Some(first_part) = first_part {
            if first_part == "bascet-mapcell-api" {
                let version = splitter.next().expect("API version missing");
                Ok(version.to_string())
            } else {
                bail!("Script --bascet-api is incorrect. Are you sure this is a valid script?");
            }
        } else {
            bail!("Failed to parse --bascet-api output of script. Are you sure this is a valid script?");
        }
    } else {
        bail!("Failed to run script {:?}. Try chmod +x script.sh to make it executable", path_script.as_ref());
    }
}








pub fn to_absolute_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref();

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    }.clean();

    Ok(absolute_path)
}

