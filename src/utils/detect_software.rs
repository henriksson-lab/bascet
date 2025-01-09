use log::debug;
use log::info;
use std::process::Command;
//use anyhow::Result;
use anyhow::bail;
use semver::{Version, VersionReq};


pub fn check_tabix() -> anyhow::Result<()> {
    debug!("Checking for tabix");
    if let Ok(_output)  = Command::new("tabix").output() {
        info!("Found tabix");
        Ok(())
    } else {
        bail!("Tabix is either not installed or not in PATH")
    }
}



pub fn check_bgzip() -> anyhow::Result<()> {
    debug!("Checking for bgzip");
    if let Ok(_output)  = Command::new("bgzip").output() {
        info!("Found bgzip");
        Ok(())
    } else {
        bail!("Bgzip is either not installed or not in PATH")
    }
}



#[allow(dead_code)]
pub fn check_samtools() -> anyhow::Result<()> {
    debug!("Checking for the correct samtools");
    let req_samtools_version = VersionReq::parse(">=1.18").unwrap();
    let samtools = Command::new("samtools").arg("version").output().expect("Samtools is either not installed or not in PATH");
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

