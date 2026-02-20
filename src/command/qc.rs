pub mod aligned_coverage;

use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Args)]
pub struct QcCMD {
    #[command(subcommand)]
    pub subcommand: QcSubcommand,
}

#[derive(Subcommand)]
pub enum QcSubcommand {
    AlignedCoverage(aligned_coverage::QcAlignedCoverageCMD),
}

impl QcCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        match &mut self.subcommand {
            QcSubcommand::AlignedCoverage(cmd) => cmd.try_execute(),
        }
    }
}