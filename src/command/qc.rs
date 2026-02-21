pub mod aligned_coverage;
pub mod ref_composition;

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
    RefComposition(ref_composition::QcRefCompositionCMD),
}

impl QcCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        match &mut self.subcommand {
            QcSubcommand::AlignedCoverage(cmd) => cmd.try_execute(),
            QcSubcommand::RefComposition(cmd) => cmd.try_execute(),
        }
    }
}