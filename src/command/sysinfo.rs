use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use anyhow::Result;
use clap::Args;


#[derive(Args)]
pub struct SysinfoCMD {
    #[arg(long = "info")]
    pub info: String,
}
impl SysinfoCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        if self.info=="cpu" {
            let s = System::new_with_specifics(
                RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()),
            );
            for cpu in s.cpus() {
                print!("{}", cpu.brand());
                break;
            }
        } else if self.info=="totalmem" {

            let s = System::new_with_specifics(
                RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
            );
            print!("{}", s.total_memory()); //bytes
        } else {
            print!("Invalid")
        }
        Ok(())
    }
}
