use sysinfo::{System, RefreshKind, CpuRefreshKind};

use anyhow::Result;
use clap::Args;


#[derive(Args)]
pub struct SysinfoCMD {
    #[arg(long = "info")]
    pub info: String,
}
impl SysinfoCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        let s = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()),
        );

        if self.info=="cpu" {
            for cpu in s.cpus() {
                print!("{}", cpu.brand());
                break;
            }
        } else {
            print!("Invalid")

        }
        Ok(())
    }
}
