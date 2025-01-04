use anyhow::bail;
use core::fmt;
use std::process;
use std::path::PathBuf;


#[derive(Clone,Debug,Eq,PartialEq)]
pub enum MissingFileMode {
    Ignore,
    Skip,
    Fail
}
impl fmt::Display for MissingFileMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}




#[derive(Clone,Debug,Eq,PartialEq)]
pub enum CompressionMode {
    Default,
    Uncompressed
}
impl fmt::Display for CompressionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}




fn parse_compression_mode(s: &str) -> anyhow::Result<CompressionMode> {
    match s {
        "default" => Ok(CompressionMode::Default),
        "uncompressed" => Ok(CompressionMode::Uncompressed),
        _ => bail!("Cannot parse compression mode")
    }
}

fn parse_missing_file_mode(s: &str) -> anyhow::Result<MissingFileMode> {
    match s {
        "ignore" => Ok(MissingFileMode::Ignore),
        "skip" => Ok(MissingFileMode::Skip),
        "fail" => Ok(MissingFileMode::Fail),
        _ => bail!("Cannot parse missing file mode")
    }
}





//#[derive(Clone,Debug,Eq,PartialEq)]  //// not sure about all of these
pub struct MapCellScript {
    pub path_script: PathBuf,
    pub api_version: String,
    pub expect_files: Vec<String>,
    pub missing_file_mode: MissingFileMode,
    pub compression_mode: CompressionMode
}

impl MapCellScript {

    pub fn new(path_script: &PathBuf) -> anyhow::Result<MapCellScript>{

        let api_version = get_script_api_version(path_script)?;
        let expect_files = get_script_expect_files(path_script)?;
        let missing_file_mode = get_missing_file_mode(path_script)?; 
        let compression_mode = get_compression_mode(path_script)?;

        Ok(MapCellScript {
            path_script: path_script.clone(),
            api_version: api_version,
            expect_files: expect_files,
            missing_file_mode: missing_file_mode,
            compression_mode: compression_mode
        })      
    }



    pub fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        num_threads: usize
    ) -> anyhow::Result<(bool, String)> {


        let run_output = process::Command::new(&self.path_script)
            .arg("--num-threads").arg(num_threads.to_string())
            .arg("--input-dir").arg(&input_dir)
            .arg("--output-dir").arg(&output_dir)
            .output()?;
        let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
        let run_output_string = run_output_string.trim();

        let last_line = run_output_string.split("\n").last(); //can this ever fail?
        let success = if let Some(last_line) = last_line { last_line=="MAPCELL-OK" } else { false };

        //debug!("last scrip init line {:?}", last_line);

        Ok((success, String::from(run_output_string)))
    }  

}



pub fn get_script_expect_files(path_script: &PathBuf) -> anyhow::Result<Vec<String>> {
    let run_output = process::Command::new(path_script)
        .arg("--expect-files")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    let splitter = run_output_string.split(',');
    let list_files: Vec<String> = splitter.map(|s| s.to_string()).collect();
    Ok(list_files)
}



fn get_missing_file_mode(path_script: &PathBuf) -> anyhow::Result<MissingFileMode> {
    let run_output = process::Command::new(path_script)
        .arg("--missing-file-mode")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    parse_missing_file_mode(run_output_string)
}



fn get_compression_mode(path_script: &PathBuf) -> anyhow::Result<CompressionMode> {
    let run_output = process::Command::new(path_script)
        .arg("--compression-mode")
        .output()?;
    let run_output_string = String::from_utf8(run_output.stdout).expect("utf8 error");
    let run_output_string = run_output_string.trim();

    parse_compression_mode(run_output_string)
}

//// Get API version. This is the first call so a lot of checks to help user debug
fn get_script_api_version(path_script: &PathBuf) -> anyhow::Result<String> {
    let run_output = process::Command::new(path_script)
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
        bail!("Failed to run script {:?}. Try chmod +x script.sh to make it executable", path_script);
    }
}









