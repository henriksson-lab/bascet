use crate::{
    bounded_parser,
    utils::{atomic_temp_path, publish_atomic_output},
};

use bascet_core::*;
use bascet_derive::Budget;

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use std::{fs::File, num::NonZeroUsize, path::PathBuf};
use tracing::{info, warn};

pub const DEFAULT_PATH_TEMP: &str = "temp";

#[derive(Args)]
pub struct ToFastqCMD {
    #[arg(
        short = 'i',
        long = "in",
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(long = "out-r1", help = "FASTQ output file R1")]
    pub path_r1: PathBuf,

    #[arg(long = "out-r2", help = "FASTQ output file R2")]
    pub path_r2: PathBuf,

    #[arg(long = "temp", help = "Temp directory; must exist already")]
    pub path_temp: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to std::threads::available parallelism)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-read",
        help = "Number of reader threads",
        value_name = "1.. (default is 1)", // 50% of total threads
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-write",
        help = "Number of writer threads",
        value_name = "1.. (default is 1)", // 50% of total threads
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_write: Option<BoundedU64<1, { u64::MAX }>>,

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
}

#[derive(Budget, Debug)]
struct ToFastqBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 ) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 ) as u64).unwrap())]
    numof_threads_write: BoundedU64<1, { u64::MAX }>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl ToFastqCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let budget = ToFastqBudget::builder()
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
            .maybe_numof_threads_read(self.numof_threads_read)
            .maybe_numof_threads_write(self.numof_threads_write)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();

        info!(
            using = %budget,
            input_path = ?self.path_in,
            path_r1 = ?self.path_r1,
            path_r2 = ?self.path_r2,
            "Converting to fastq"
        );

        /////////////////////////////////////////////////////////////////////////////////////
        // Set up writers
        let write_threads = budget.numof_threads_write.get() as usize;
        let writer_threads_per_file =
            NonZeroUsize::new((write_threads / 2).max(1)).expect("thread count is nonzero");

        let path_r1_tmp = atomic_temp_path(&self.path_r1);
        let path_r2_tmp = atomic_temp_path(&self.path_r2);
        let file_r1 = File::create(&path_r1_tmp)?;
        let file_r2 = File::create(&path_r2_tmp)?;
        let mut writer_r1 = noodles::bgzf::io::MultithreadedWriter::with_worker_count(
            writer_threads_per_file,
            file_r1,
        );
        let mut writer_r2 = noodles::bgzf::io::MultithreadedWriter::with_worker_count(
            writer_threads_per_file,
            file_r2,
        );

        /////////////////////////////////////////////////////////////////////////////////////
        // All threads are now set up. Send all readpairs to writers.
        // This function blocks until reading is done
        crate::align::common::write_tirp_to_2fq(
            self.path_in.path().path(),
            &mut writer_r1,
            &mut writer_r2,
            budget.numof_threads_read,
            self.sizeof_stream_arena,
            budget.sizeof_stream_buffer,
        )?;
        writer_r1.finish()?;
        writer_r2.finish()?;
        publish_atomic_output(path_r1_tmp, &self.path_r1)?;
        publish_atomic_output(path_r2_tmp, &self.path_r2)?;

        info!("Conversion complete");

        //Move temp files to their right positions TODO

        Ok(())
    }
}
