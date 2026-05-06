use std::path::PathBuf;

use anyhow::Result;
use clap::Args;

use super::determine_thread_counts_1;
use crate::bigwig::{ToBigWigOptions, bam_to_bigwig};

const DEFAULT_BIN_SIZE: u32 = 50;

#[derive(Args)]
pub struct ToBigWigCMD {
    /// Input BAM file.
    #[arg(short = 'i', long = "in", alias = "bam", value_parser)]
    pub path_in: PathBuf,

    /// Output BigWig file.
    #[arg(short = 'o', long = "out", value_parser)]
    pub path_out: PathBuf,

    /// Coverage bin size in bases. Matches bamCoverage's default.
    #[arg(long = "bin-size", short = 'b', default_value_t = DEFAULT_BIN_SIZE)]
    pub bin_size: u32,

    /// Skip records with BAM flag 0x4 set.
    #[arg(long = "skip-unmapped", default_value_t = true)]
    pub skip_unmapped: bool,

    /// Skip records with BAM flag 0x100 set.
    #[arg(long = "skip-secondary", default_value_t = false)]
    pub skip_secondary: bool,

    /// Skip records with BAM flag 0x800 set.
    #[arg(long = "skip-supplementary", default_value_t = false)]
    pub skip_supplementary: bool,

    /// Scale all output values by this factor.
    #[arg(long = "scale-factor", default_value_t = 1.0)]
    pub scale_factor: f32,

    /// Worker threads for BAM decompression and BigWig writing.
    #[arg(short = '@', long = "threads", value_parser = clap::value_parser!(usize))]
    pub num_threads: Option<usize>,
}

impl ToBigWigCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let num_threads = determine_thread_counts_1(self.num_threads)?;
        bam_to_bigwig(
            &self.path_in,
            &self.path_out,
            ToBigWigOptions {
                bin_size: self.bin_size,
                skip_unmapped: self.skip_unmapped,
                skip_secondary: self.skip_secondary,
                skip_supplementary: self.skip_supplementary,
                scale_factor: self.scale_factor,
                num_threads,
            },
        )
    }
}
