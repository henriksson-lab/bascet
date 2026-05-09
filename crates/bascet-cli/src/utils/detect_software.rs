use anyhow::bail;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use tracing::{debug, info};

///////////////////////////////
/// Get directory where all Bascet data files are stored. These are kept separate
pub fn get_bascet_datadir() -> PathBuf {
    let env_bascet_data = env::var("BASCET_DATA");
    if let Ok(v) = env_bascet_data {
        PathBuf::from(v)
    } else {
        //Use default directory if no path set. This is for developers primarily
        PathBuf::from("./data/")
    }
}

///////////////////////////////
/// Check if KMC is installed
pub fn check_kmc_tools() -> anyhow::Result<()> {
    debug!("Checking for kmc_tools");
    if let Ok(_output) = Command::new("kmc_tools").output() {
        info!("Found kmc_tools");
        Ok(())
    } else {
        bail!("kmc_tools is either not installed or not in PATH")
    }
}
