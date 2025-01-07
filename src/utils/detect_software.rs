use log::debug;
use log::info;
use std::process::Command;
//use anyhow::Result;
use anyhow::bail;

#[allow(dead_code)]
pub fn check_tabix() -> anyhow::Result<()> {
    debug!("Checking for tabix");
    if let Ok(_output)  = Command::new("tabix").output() {
        info!("Found tabix");
        Ok(())
    } else {
        bail!("Tabix is either not installed or not in PATH")
    }
}



#[allow(dead_code)]
pub fn check_bgzip() -> anyhow::Result<()> {
    debug!("Checking for bgzip");
    if let Ok(_output)  = Command::new("bgzip").output() {
        info!("Found bgzip");
        Ok(())
    } else {
        bail!("Bgzip is either not installed or not in PATH")
    }
}


