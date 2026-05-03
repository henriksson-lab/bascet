use crate::bounded_parser;

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
};
#[cfg(any(feature = "star-rs-align", feature = "minimap2-rs-align"))]
use std::sync::Arc;
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
        // Only the STAR and minimap2 paths consume an external rayon pool; BWAMEM2 manages
        // its own internal pool via the stock-driver pipeline.
        #[cfg(any(feature = "star-rs-align", feature = "minimap2-rs-align"))]
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
        if self.aligner == "BWAMEM2" {
            return super::align_bwa::try_execute_bwa_mem2(
                self.path_in.path().path(),
                &self.path_genome,
                &self.path_out_unsorted,
                &self.path_out_sorted,
                &self.path_temp,
                budget.threads.get() as usize,
                budget.memory,
                budget.threads.get(),
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
            "aligner {} is not available; use --aligner BWAMEM2 with the Rust BWA feature, --aligner STAR with the Rust STAR feature, or --aligner minimap2 with the Rust minimap2 feature",
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

//TODO: single-end reads
