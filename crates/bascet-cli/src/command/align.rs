//! `align` CLI subcommand: parses args, builds the budget + thread allocation, and
//! dispatches to the per-aligner implementation in `crate::align::*`. All pipeline logic
//! lives under `crate::align`.

use crate::bounded_parser;

use bascet_core::*;
use bascet_derive::Budget;

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use std::path::PathBuf;
#[cfg(any(
    feature = "bwa-mem2-rs-align",
    feature = "star-rs-align",
    feature = "minimap2-rs-align"
))]
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Args)]
pub struct AlignCMD {
    #[arg(
        short = 'i',
        long = "in",
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(short = 'u', long = "unsorted", help = "Output file for unsorted BAM")]
    pub path_out_unsorted: PathBuf,

    #[arg(short = 's', long = "sorted", help = "Output file for sorted BAM")]
    pub path_out_sorted: PathBuf,

    #[arg(long = "temp", help = "Temp directory; must exist already")]
    pub path_temp: PathBuf,

    #[arg(short = 'g', long = "genome", help = "Genome to use")]
    pub path_genome: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to std::threads::available parallelism)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(1),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
        value_name = "100%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,

    #[arg(
        long = "bwamem2-mem-overhead-per-input-byte",
        help = "BWAMEM2 in-flight memory charge per uncompressed input sequence byte",
        default_value_t = 10,
        hide_short_help = true,
        value_parser = clap::value_parser!(u64).range(1..),
    )]
    bwamem2_mem_overhead_per_input_byte: u64,

    #[arg(
        long = "aligner",
        help = "The command to send the data to",
        value_parser = ["BWAMEM2", "STAR", "minimap2"],
        hide_short_help = true
    )]
    aligner: String,

    #[arg(
        long = "minimap2-preset",
        help = "minimap2 preset to use when --aligner minimap2 is selected",
        default_value = "map-ont",
        hide_short_help = true
    )]
    minimap2_preset: String,

    #[arg(
        long = "max-read-pairs",
        help = "Stop after this many input read pairs [advanced/testing]",
        hide_short_help = true,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    max_read_pairs: Option<u64>,
}

#[derive(Budget, Debug)]
struct AlignBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating((total_threads as f64 * 0.15) as u64))]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[skip(budget)]
    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating(total_threads.min(8)))]
    numof_threads_writebam: BoundedU64<1, { u64::MAX }>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

#[derive(Debug, Clone, Copy)]
#[cfg(feature = "minimap2-rs-align")]
struct AlignThreadAllocation {
    read: BoundedU64<1, { u64::MAX }>,
    write_bam: usize,
}

#[cfg(feature = "minimap2-rs-align")]
impl AlignThreadAllocation {
    fn from_budget(budget: &AlignBudget) -> Self {
        let total_threads = budget.threads.get();
        let read = budget.numof_threads_read;
        let write_bam = budget.numof_threads_writebam.get() as usize;
        let reserved = read.get() + write_bam as u64;
        if reserved >= total_threads {
            info!(
                total_threads,
                read_threads = read.get(),
                write_bam_threads = write_bam,
                "Using oversubscribed helper threads; shared Rayon pool still caps CPU use"
            );
        }
        Self { read, write_bam }
    }
}

impl AlignCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let budget = AlignBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to determine available parallelism, using 2 threads");
                        2
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to convert parallelism to valid thread count, using 2 threads");
                        2.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();
        #[cfg(feature = "minimap2-rs-align")]
        let thread_allocation = AlignThreadAllocation::from_budget(&budget);
        // Shared by aligners that can run their internal parallel regions and helper work on a
        // common fixed-size worker pool.
        #[cfg(any(
            feature = "bwa-mem2-rs-align",
            feature = "star-rs-align",
            feature = "minimap2-rs-align"
        ))]
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(budget.threads.get() as usize)
                .build()?,
        );

        info!(
            threads = budget.threads.get(),
            memory = %budget.memory,
            aligner = %self.aligner,
            "Starting align"
        );

        #[cfg(feature = "bwa-mem2-rs-align")]
        if self.aligner == "BWAMEM2" {
            return crate::align::bwa::try_execute_bwa_mem2(
                self.path_in.path().path(),
                &self.path_genome,
                &self.path_out_unsorted,
                &self.path_out_sorted,
                &self.path_temp,
                budget.threads.get() as usize,
                budget.memory,
                budget.threads.get(),
                Arc::clone(&rayon_pool),
                self.bwamem2_mem_overhead_per_input_byte,
            );
        }

        #[cfg(feature = "star-rs-align")]
        if self.aligner == "STAR" {
            let star_threads = budget.threads.get() as usize;
            let star_bam_writer_threads = star_threads.div_ceil(2).clamp(1, 16);
            return crate::align::star::try_execute_star_rs(
                self.path_in.path().path(),
                &self.path_genome,
                &self.path_out_unsorted,
                &self.path_out_sorted,
                &self.path_temp,
                star_bam_writer_threads,
                star_threads,
                self.sizeof_stream_arena,
                budget.sizeof_stream_buffer,
                budget.memory,
                budget.threads.get(),
                Arc::clone(&rayon_pool),
                self.max_read_pairs,
            );
        }

        #[cfg(feature = "minimap2-rs-align")]
        if self.aligner.eq_ignore_ascii_case("minimap2") {
            return crate::align::minimap2::try_execute_minimap2(
                self.path_in.path().path(),
                &self.path_genome,
                &self.path_out_unsorted,
                &self.path_out_sorted,
                &self.path_temp,
                thread_allocation.write_bam,
                budget.threads.get() as usize,
                thread_allocation.read,
                self.sizeof_stream_arena,
                budget.sizeof_stream_buffer,
                &self.minimap2_preset,
                budget.memory,
                budget.threads.get(),
                Arc::clone(&rayon_pool),
            );
        }

        anyhow::bail!(
            "aligner {} is not available; use --aligner BWAMEM2 with the Rust BWA feature, --aligner STAR with the Rust STAR feature, or --aligner minimap2 with the Rust minimap2 feature",
            self.aligner
        )
    }
}
