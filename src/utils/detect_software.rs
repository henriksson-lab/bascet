use anyhow::bail;
use log::debug;
use log::info;
use semver::{Version, VersionReq};
<<<<<<< HEAD
use std::env;
use std::path::PathBuf;
use std::process::Command;
=======
use std::process::Command;
use std::path::PathBuf;
use std::env;
>>>>>>> main

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

<<<<<<< HEAD
=======

>>>>>>> main
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

///////////////////////////////
/// Check if TABIX is installed
pub fn check_tabix() -> anyhow::Result<()> {
    debug!("Checking for tabix");
    if let Ok(_output) = Command::new("tabix").output() {
        info!("Found tabix");
        Ok(())
    } else {
        bail!("Tabix is either not installed or not in PATH")
    }
}

///////////////////////////////
/// Check if BGZIP is installed
pub fn check_bgzip() -> anyhow::Result<()> {
    debug!("Checking for bgzip");
    if let Ok(_output) = Command::new("bgzip").output() {
        info!("Found bgzip");
        Ok(())
    } else {
        bail!("Bgzip is either not installed or not in PATH")
    }
}

///////////////////////////////
/// Check if samtools is installed
pub fn check_samtools() -> anyhow::Result<()> {
    debug!("Checking for the correct samtools");
    let req_samtools_version = VersionReq::parse(">=1.18").unwrap();
    let samtools = Command::new("samtools")
        .arg("version")
        .output()
        .expect("Samtools is either not installed or not in PATH");
    let samtools_version = String::from_utf8_lossy(
        samtools
            .stdout
            .split(|c| *c == b'\n')
            .next()
            .unwrap()
            .split(|c| *c == b' ')
            .last()
            .unwrap(),
    );
    let samtools_version = samtools_version.parse::<Version>().unwrap();
    if req_samtools_version.matches(&samtools_version) {
        debug!("Samtools version is recent enough");
        Ok(())
    } else {
        bail!("This software requires Samtools >= 1.18");
    }
}
