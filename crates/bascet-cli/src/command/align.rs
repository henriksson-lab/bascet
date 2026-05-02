use crate::{
    bounded_parser,
    utils::{atomic_temp_path, publish_atomic_output},
};

use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, tirp};

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{debug, info, warn};

#[derive(Args)]
pub struct AlignCMD {
    #[arg(
        short = 'i',
        long = "in",
        //num_args = 1..,
        //value_delimiter = ',',
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(short = 'u', long = "unsorted", help = "Output file for unsorted BAM")]
    pub path_out_unsorted: PathBuf,

    #[arg(short = 's', long = "sorted", help = "Output file for sorted BAM")]
    pub path_out_sorted: PathBuf,

    #[arg(long = "temp", help = "Temp directory; must exist already")]
    pub path_temp: PathBuf,

    #[arg(
        short = 'g',
        long = "genome",
        //num_args = 1..,
        //value_delimiter = ',',
        help = "Genome to use"
    )]
    pub path_genome: PathBuf, //File might not exist, so use regular PathBuf

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
        long = "aligner",
        help = "The command to send the data to",
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
    #[threads(TWrite, |total_threads: u64, _| bounded_integer::BoundedU64::new_saturating(total_threads))]
    numof_threads_writebam: BoundedU64<1, { u64::MAX }>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

#[derive(Debug, Clone, Copy)]
struct AlignThreadAllocation {
    read: BoundedU64<1, { u64::MAX }>,
    write_bam: usize,
}

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
        let thread_allocation = AlignThreadAllocation::from_budget(&budget);
        let rayon_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(budget.threads.get() as usize)
                .build()?,
        );

        info!(
            using = %budget,
            rayon_pool_threads = budget.threads.get(),
            read_threads = thread_allocation.read.get(),
            write_bam_threads = thread_allocation.write_bam,
            input_path = ?self.path_in,
            unsorted_output_path = ?self.path_out_unsorted,
            sorted_output_path = ?self.path_out_sorted,
            "Starting Align"
        );

        #[cfg(feature = "bwa-mem2-rs-align")]
        if self.aligner == "BWA" {
            return super::align_bwa::try_execute_bwa_mem2(
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
                budget.memory,
                budget.threads.get(),
                Arc::clone(&rayon_pool),
            );
        }

        #[cfg(feature = "star-rs-align")]
        if self.aligner == "STAR" {
            let star_threads = budget.threads.get() as usize;
            return super::align_star::try_execute_star_rs(
                self.path_in.path().path(),
                &self.path_genome,
                &self.path_out_unsorted,
                &self.path_out_sorted,
                &self.path_temp,
                thread_allocation.write_bam,
                star_threads,
                thread_allocation.read,
                self.sizeof_stream_arena,
                budget.sizeof_stream_buffer,
                budget.memory,
                budget.threads.get(),
                Arc::clone(&rayon_pool),
            );
        }

        #[cfg(feature = "minimap2-rs-align")]
        if self.aligner.eq_ignore_ascii_case("minimap2") {
            return super::align_minimap2::try_execute_minimap2(
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
            "aligner {} is not available; use --aligner BWA with the Rust BWA feature, --aligner STAR with the Rust STAR feature, or --aligner minimap2 with the Rust minimap2 feature",
            self.aligner
        )
    }

    ///
    /// Get a TIRP, stream to fastq
    ///
    pub fn write_tirp_to_2fq<P>(
        path_in: P,
        writer_r1: &mut impl Write,
        writer_r2: &mut impl Write,
        num_threads: BoundedU64<1, { u64::MAX }>,
        sizeof_stream_arena: ByteSize,
        sizeof_stream_buffer: ByteSize,
    ) -> Result<()>
    where
        P: AsRef<Path>,
    {
        /////////////////////////////////////////////////////////////////////////////////////
        // Streamer from input TIRP
        let decoder = codec::BBGZDecoder::builder()
            .with_path(path_in)
            .countof_threads(num_threads)
            .build();
        let parser = parse::Tirp::builder().build();

        let mut stream = Stream::builder()
            .with_decoder(decoder)
            .with_parser(parser)
            .sizeof_decode_arena(sizeof_stream_arena)
            .sizeof_decode_buffer(sizeof_stream_buffer)
            .build();

        let mut query = stream.query::<tirp::Record>();

        debug!("Sending read pairs");
        let mut num_read: u64 = 0;
        loop {
            match query.next_into::<tirp::Record>() {
                Ok(Some(record)) => {
                    //println!("one read pair");

                    let record_id = *record.get_ref::<Id>();
                    let record_r1 = *record.get_ref::<R1>();
                    let record_r2 = *record.get_ref::<R2>();
                    let record_q1 = *record.get_ref::<Q1>();
                    let record_q2 = *record.get_ref::<Q2>();
                    let record_umi = *record.get_ref::<Umi>();

                    fn write_read_bascetfq<W>(
                        writer: &mut W,
                        record_id: &[u8],
                        record_read: &[u8],
                        record_qual: &[u8],
                        record_umi: &[u8],
                        num_read: u64,
                    ) -> Result<()>
                    where
                        W: Write,
                    {
                        writer.write_all(b"@")?;
                        writer.write_all(record_id)?;
                        writer.write_all(b":")?;
                        writer.write_all(record_umi)?;
                        writer.write_all(b":")?;
                        writer.write_all(format!("{}", num_read).as_bytes())?;

                        writer.write_all(b"\n")?;
                        writer.write_all(record_read)?;
                        writer.write_all(b"\n+\n")?;
                        writer.write_all(record_qual)?;
                        writer.write_all(b"\n")?;
                        Ok(())
                    }

                    write_read_bascetfq(
                        writer_r1,
                        &record_id,
                        &record_r1,
                        &record_q1,
                        &record_umi,
                        num_read,
                    )?;

                    write_read_bascetfq(
                        writer_r2,
                        &record_id,
                        &record_r2,
                        &record_q2,
                        &record_umi,
                        num_read,
                    )?;

                    //What about Q? it might not be used at all. but could output as an option
                    /*
                    Encoding: BWA-MEM defaults to Phred+33, which is standard for Illumina data
                    @SEQ_ID
                    GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
                    +
                    !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
                    */
                    num_read += 1;
                    if num_read % 1_000_000 == 0 {
                        info!("{}M Read pairs written", num_read / 1_000_000);
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    panic!("{:?}", e);
                }
            };
        }
        debug!("All readpairs sent");

        //Ensure data is properly pushed out
        writer_r1.flush()?;
        writer_r2.flush()?;
        debug!("All readpairs flushed");

        Ok(())
    }
}

pub(super) fn stream_buffer_after_index_load(
    aligner_name: &str,
    total_memory: ByteSize,
    requested_stream_buffer: ByteSize,
    sizeof_stream_arena: ByteSize,
    total_threads: u64,
) -> ByteSize {
    let minimum_stream_buffer = ByteSize(
        (sizeof_stream_arena.as_u64() * 2)
            .max(ByteSize::mib(64).as_u64())
            .min(requested_stream_buffer.as_u64()),
    );
    let memory_headroom = ByteSize(
        ByteSize::mib(512)
            .as_u64()
            .max((total_memory.as_u64() as f64 * 0.05) as u64),
    );
    let future_reading_reserve = ByteSize(
        ByteSize::mib(256)
            .as_u64()
            .max(total_threads.saturating_mul(ByteSize::mib(64).as_u64())),
    );

    let Some(memory) = memory_stats::memory_stats() else {
        warn!(
            aligner = aligner_name,
            total_memory = %total_memory,
            requested_stream_buffer = %requested_stream_buffer,
            "Could not read current RSS after aligner index load; using requested stream buffer"
        );
        return requested_stream_buffer;
    };

    let index_loaded_rss = ByteSize(memory.physical_mem as u64);
    let available_for_stream = total_memory
        .as_u64()
        .saturating_sub(index_loaded_rss.as_u64())
        .saturating_sub(memory_headroom.as_u64())
        .saturating_sub(future_reading_reserve.as_u64());
    let adjusted = ByteSize(
        available_for_stream
            .max(minimum_stream_buffer.as_u64())
            .min(requested_stream_buffer.as_u64()),
    );

    if adjusted == minimum_stream_buffer && adjusted < requested_stream_buffer {
        warn!(
            aligner = aligner_name,
            index_loaded_rss = %index_loaded_rss,
            total_memory = %total_memory,
            requested_stream_buffer = %requested_stream_buffer,
            memory_headroom = %memory_headroom,
            future_reading_reserve = %future_reading_reserve,
            adjusted_stream_buffer = %adjusted,
            "Aligner index leaves little budget for stream buffers; using minimum stream buffer"
        );
    } else {
        info!(
            aligner = aligner_name,
            index_loaded_rss = %index_loaded_rss,
            total_memory = %total_memory,
            requested_stream_buffer = %requested_stream_buffer,
            memory_headroom = %memory_headroom,
            future_reading_reserve = %future_reading_reserve,
            adjusted_stream_buffer = %adjusted,
            "Adjusted aligner stream buffer after index load"
        );
    }

    adjusted
}

pub(super) fn warn_if_index_disk_size_exceeds_memory(
    aligner_name: &str,
    index_path: &Path,
    index_disk_size_bytes: u64,
    total_memory: ByteSize,
) {
    let index_disk_size = ByteSize(index_disk_size_bytes);
    if index_disk_size.as_u64() > total_memory.as_u64() {
        warn!(
            aligner = aligner_name,
            index_path = ?index_path,
            index_disk_size = %index_disk_size,
            total_memory = %total_memory,
            "Aligner index files on disk are larger than the provided memory budget"
        );
    }
}

///
/// Sort a given BAM file, to a new a file
///
pub fn sort_bam<P>(path_in: P, path_out: P, path_temp: P, num_threads: u64) -> Result<()>
where
    P: AsRef<Path>,
{
    let num_threads = format!("{}", num_threads);

    let path_in = format!("{}", path_in.as_ref().as_os_str().to_str().expect("os str"));
    let path_out_final = path_out.as_ref().to_path_buf();
    let path_out_tmp = atomic_temp_path(&path_out_final);
    let path_out_arg = format!("{}", path_out_tmp.as_os_str().to_str().expect("os str"));

    let path_temp_prefix = PathBuf::from(path_temp.as_ref()).join("sort");
    let path_temp_prefix = format!("{}", path_temp_prefix.as_os_str().to_str().expect("os str"));

    let args = vec![
        "sort",
        "-@",
        &num_threads,
        &path_in,
        "-T",
        &path_temp_prefix,
        "-o",
        &path_out_arg,
    ];

    let _proc_out = std::process::Command::new("samtools")
        .args(args)
        .status()
        .expect("failed to run samtools sort");
    publish_atomic_output(path_out_tmp, path_out_final)?;
    Ok(())
}

///
/// Index a given BAM file
///
pub fn index_bam(path_in: &str) -> Result<()> {
    let path_index = PathBuf::from(format!("{path_in}.bai"));
    let path_index_tmp = atomic_temp_path(&path_index);
    let path_index_tmp_arg = path_index_tmp
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid BAM index path"))?;
    let args = vec!["index", "-o", path_index_tmp_arg, path_in];

    let _proc_out = std::process::Command::new("samtools")
        .args(args)
        .status()
        .expect("failed to run samtools index");
    publish_atomic_output(path_index_tmp, path_index)?;

    Ok(())
}

//TODO: single-end reads
