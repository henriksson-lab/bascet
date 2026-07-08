use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::thread;

use anyhow::{Context, Result, bail};
use bascet_core::DEFAULT_SIZEOF_ARENA;
use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bytesize::ByteSize;
use clap::Args;
use crossbeam::channel::{Receiver, Sender};
use fastqc_rs::{config::FastQCConfig, sequence::Sequence};
use tracing::{info, warn};
use zip::ZipWriter;

use crate::{
    fileformat::ReadPair,
    utils::{atomic_temp_path, publish_atomic_output},
};

const DEFAULT_SIZEOF_STREAM_BUFFER: ByteSize = ByteSize::gib(4);

#[derive(Args)]
pub struct FastqcCMD {
    /// Input TIRP file.
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    /// Output zip file.
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    /// Total thread budget. One or more threads are used for TIRP reading; the rest process cells.
    #[arg(short = 't', long = "threads")]
    pub threads: Option<usize>,

    /// Number of cells to process concurrently.
    #[arg(short = '@', long = "fastqc-workers", conflicts_with = "threads")]
    pub fastqc_workers: Option<usize>,

    /// Threads used by the TIRP BGZF decoder.
    #[arg(long = "num-threads-read", default_value_t = 1)]
    pub num_threads_read: usize,

    /// Do not group bases in the FastQC per-base modules.
    #[arg(long = "nogroup")]
    pub nogroup: bool,

    /// Use exponential base grouping in the FastQC per-base modules.
    #[arg(long = "expgroup")]
    pub expgroup: bool,

    /// K-mer size for FastQC k-mer content.
    #[arg(long = "kmer-size", default_value_t = 7)]
    pub kmer_size: usize,

    /// Minimum sequence length to include.
    #[arg(long = "min-length", default_value_t = 0)]
    pub min_length: usize,

    /// Length to truncate sequences for duplication detection.
    #[arg(long = "dup-length", default_value_t = 50)]
    pub dup_length: usize,

    /// Maximum read pairs per cell fed to FastQC. 0 disables the cap. When a cell
    /// exceeds this, only the first N read pairs encountered in the file are used
    /// (no random subsampling).
    #[arg(long = "max-reads-per-cell", default_value_t = 0)]
    pub max_reads_per_cell: usize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
        default_value_t = DEFAULT_SIZEOF_STREAM_BUFFER,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: ByteSize,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,
}

impl FastqcCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        self.validate()?;
        let fastqc_workers = self.effective_fastqc_workers();

        let config = FastqcParams {
            nogroup: self.nogroup,
            expgroup: self.expgroup,
            kmer_size: self.kmer_size,
            min_length: self.min_length,
            dup_length: self.dup_length,
        };

        run_fastqc_cells(
            self.path_in.clone(),
            self.path_out.clone(),
            self.num_threads_read,
            fastqc_workers,
            self.max_reads_per_cell,
            self.sizeof_stream_arena,
            self.sizeof_stream_buffer,
            config,
        )
    }

    fn validate(&self) -> Result<()> {
        if self.threads == Some(0) {
            bail!("--threads must be > 0");
        }
        if self.fastqc_workers == Some(0) {
            bail!("--fastqc-workers must be > 0");
        }
        if self.num_threads_read == 0 {
            bail!("--num-threads-read must be > 0");
        }
        if let Some(threads) = self.threads {
            if threads <= self.num_threads_read {
                bail!("--threads must be greater than --num-threads-read");
            }
        }
        if self.kmer_size == 0 {
            bail!("--kmer-size must be > 0");
        }
        if self.dup_length == 0 {
            bail!("--dup-length must be > 0");
        }
        Ok(())
    }

    fn effective_fastqc_workers(&self) -> usize {
        self.fastqc_workers.unwrap_or_else(|| {
            let total_threads = self.threads.unwrap_or_else(available_threads);
            total_threads.saturating_sub(self.num_threads_read).max(1)
        })
    }
}

fn available_threads() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

#[derive(Clone)]
struct FastqcParams {
    nogroup: bool,
    expgroup: bool,
    kmer_size: usize,
    min_length: usize,
    dup_length: usize,
}

impl FastqcParams {
    fn to_config(&self) -> FastQCConfig {
        FastQCConfig {
            nogroup: self.nogroup,
            expgroup: self.expgroup,
            quiet: true,
            kmer_size: self.kmer_size,
            min_length: self.min_length,
            dup_length: self.dup_length,
            ..FastQCConfig::default()
        }
    }
}

struct CellReads {
    cell_id: String,
    reads: Vec<ReadPair>,
}

struct CellFastqc {
    cell_id: String,
    r1_report: Vec<u8>,
    r2_report: Vec<u8>,
}

fn run_fastqc_cells(
    path_in: PathBuf,
    path_out: PathBuf,
    num_threads_read: usize,
    fastqc_workers: usize,
    max_reads_per_cell: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    params: FastqcParams,
) -> Result<()> {
    let queue_size = fastqc_workers * 2;
    let (tx_cells, rx_cells) = crossbeam::channel::bounded::<Result<CellReads>>(queue_size);
    let (tx_reports, rx_reports) = crossbeam::channel::bounded::<Result<CellFastqc>>(queue_size);

    let reader = thread::spawn(move || {
        let result = stream_tirp_cells(
            path_in,
            num_threads_read,
            max_reads_per_cell,
            sizeof_stream_arena,
            sizeof_stream_buffer,
            tx_cells.clone(),
        );
        if let Err(e) = result {
            let _ = tx_cells.send(Err(e));
        }
    });

    let mut workers = Vec::with_capacity(fastqc_workers);
    for worker_id in 0..fastqc_workers {
        let rx_cells = rx_cells.clone();
        let tx_reports = tx_reports.clone();
        let params = params.clone();
        workers.push(thread::spawn(move || {
            while let Ok(cell) = rx_cells.recv() {
                let report = cell.and_then(|cell| {
                    info!("fastqc worker {} processing {}", worker_id, cell.cell_id);
                    process_cell(cell, &params)
                });
                if tx_reports.send(report).is_err() {
                    break;
                }
            }
        }));
    }
    drop(rx_cells);
    drop(tx_reports);

    let writer = thread::spawn(move || write_zip(path_out, rx_reports));

    reader
        .join()
        .map_err(|_| anyhow::anyhow!("TIRP reader thread panicked"))?;

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("fastqc worker thread panicked"))?;
    }

    writer
        .join()
        .map_err(|_| anyhow::anyhow!("zip writer thread panicked"))??;

    Ok(())
}

fn stream_tirp_cells(
    path_in: PathBuf,
    num_threads_read: usize,
    max_reads_per_cell: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    tx_cells: Sender<Result<CellReads>>,
) -> Result<()> {
    info!("Streaming TIRP input {}", path_in.display());
    let num_threads = bounded_integer::BoundedU64::new(num_threads_read as u64)
        .context("invalid read thread count")?;
    let decoder: bascet_io::BBGZDecoder = bascet_io::codec::BBGZDecoder::builder()
        .with_path(&path_in)
        .countof_threads(num_threads)
        .build();
    let parser = bascet_io::parse::Tirp::builder().build();

    let mut stream = bascet_core::Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();

    let mut query = stream.query::<bascet_io::tirp::Record>();
    let mut current_cell_id = Vec::new();
    let mut current_reads = Vec::new();
    // Per-cell totals counted across all records (independent of the cap), so the
    // logged reads/bases reflect the true cell size even when we stop adding.
    let mut current_read_pairs: usize = 0;
    let mut current_bases: usize = 0;
    let mut num_cells_queued = 0_u64;

    while let Some(record) = query
        .next_into::<bascet_io::tirp::Record>()
        .context("failed to read TIRP record")?
    {
        let record_id = *record.get_ref::<Id>();
        if record_id != current_cell_id.as_slice() {
            if send_current_cell(
                &tx_cells,
                &mut current_cell_id,
                &mut current_reads,
                current_read_pairs,
                current_bases,
                max_reads_per_cell,
            )? {
                num_cells_queued += 1;
            }
            current_read_pairs = 0;
            current_bases = 0;
            current_cell_id = record_id.to_vec();
            if num_cells_queued > 0 && num_cells_queued % 1000 == 0 {
                info!("queued {} cells", num_cells_queued);
            }
        }

        let r1 = record.get_ref::<R1>();
        let r2 = record.get_ref::<R2>();
        // Feed FastQC only the first `max_reads_per_cell` pairs (0 = no cap). We keep
        // counting all records so the per-cell log shows the true totals.
        if max_reads_per_cell == 0 || current_read_pairs < max_reads_per_cell {
            current_reads.push(ReadPair {
                r1: (*r1).to_vec(),
                r2: (*r2).to_vec(),
                q1: (*record.get_ref::<Q1>()).to_vec(),
                q2: (*record.get_ref::<Q2>()).to_vec(),
                umi: (*record.get_ref::<Umi>()).to_vec(),
            });
        }
        current_read_pairs += 1;
        current_bases += r1.len() + r2.len();
    }

    if send_current_cell(
        &tx_cells,
        &mut current_cell_id,
        &mut current_reads,
        current_read_pairs,
        current_bases,
        max_reads_per_cell,
    )? {
        num_cells_queued += 1;
    }
    info!("queued final total of {} cells", num_cells_queued);
    Ok(())
}

fn send_current_cell(
    tx_cells: &Sender<Result<CellReads>>,
    current_cell_id: &mut Vec<u8>,
    current_reads: &mut Vec<ReadPair>,
    read_pairs: usize,
    bases: usize,
    max_reads_per_cell: usize,
) -> Result<bool> {
    if current_reads.is_empty() {
        return Ok(false);
    }

    let cell_id = String::from_utf8(std::mem::take(current_cell_id))
        .context("cell id in TIRP is not valid UTF-8")?;
    validate_zip_cell_id(&cell_id)?;
    let used_pairs = if max_reads_per_cell > 0 {
        read_pairs.min(max_reads_per_cell)
    } else {
        read_pairs
    };
    if used_pairs < read_pairs {
        warn!(
            "cell {} has {} read pairs ({} bases), exceeding --max-reads-per-cell {}; using the first {} and dropping {}",
            cell_id,
            read_pairs,
            bases,
            max_reads_per_cell,
            used_pairs,
            read_pairs - used_pairs
        );
    } else {
        info!("cell {} reads={} bases={}", cell_id, read_pairs, bases);
    }
    let reads = std::mem::take(current_reads);
    tx_cells
        .send(Ok(CellReads { cell_id, reads }))
        .context("failed to send cell reads to fastqc workers")?;
    Ok(true)
}

fn process_cell(cell: CellReads, params: &FastqcParams) -> Result<CellFastqc> {
    let config = params.to_config();
    let r1_report = fastqc_data_report(
        cell.reads
            .iter()
            .enumerate()
            .map(|(idx, read)| sequence_from_read(&cell.cell_id, idx, 1, &read.r1, &read.q1)),
        &config,
        &cell.cell_id,
    )?;
    let r2_report = fastqc_data_report(
        cell.reads
            .iter()
            .enumerate()
            .map(|(idx, read)| sequence_from_read(&cell.cell_id, idx, 2, &read.r2, &read.q2)),
        &config,
        &cell.cell_id,
    )?;

    Ok(CellFastqc {
        cell_id: cell.cell_id,
        r1_report: r1_report.into_bytes(),
        r2_report: r2_report.into_bytes(),
    })
}

fn sequence_from_read(
    cell_id: &str,
    idx: usize,
    mate: usize,
    sequence: &[u8],
    quality: &[u8],
) -> Sequence {
    Sequence::new(
        format!("{cell_id}_R{mate}.fastq"),
        String::from_utf8_lossy(sequence).into_owned(),
        String::from_utf8_lossy(quality).into_owned(),
        format!("{cell_id}:{idx}/{mate}"),
    )
}

fn fastqc_data_report(
    sequences: impl Iterator<Item = Sequence>,
    config: &FastQCConfig,
    cell_id: &str,
) -> Result<String> {
    let mut modules = fastqc_rs::create_modules(config);
    let sequences = sequences.map(|sequence| Ok(sequence) as Result<_, Box<dyn std::error::Error>>);
    fastqc_rs::analysis::run_analysis(sequences, &mut modules, true, config.min_length)
        .map_err(|e| anyhow::anyhow!("fastqc analysis failed for cell {}: {}", cell_id, e))?;
    Ok(fastqc_rs::report::generate_data_report(
        &mut modules,
        fastqc_rs::VERSION,
    ))
}

fn write_zip(path_out: PathBuf, rx_reports: Receiver<Result<CellFastqc>>) -> Result<()> {
    let path_tmp = atomic_temp_path(&path_out);
    let file = File::create(&path_tmp)
        .with_context(|| format!("failed to create output zip {}", path_tmp.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(file));
    let options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let mut num_cells = 0_u64;
    for report in rx_reports {
        let report = report?;
        let entry_name = format!("{}/r1_fastqc_data.txt", report.cell_id);
        zip_writer.start_file(entry_name, options)?;
        let mut r1_report = report.r1_report.as_slice();
        std::io::copy(&mut r1_report, &mut zip_writer)?;

        let entry_name = format!("{}/r2_fastqc_data.txt", report.cell_id);
        zip_writer.start_file(entry_name, options)?;
        let mut r2_report = report.r2_report.as_slice();
        std::io::copy(&mut r2_report, &mut zip_writer)?;

        num_cells += 1;
        if num_cells % 100 == 0 {
            info!("wrote fastqc output for {} cells", num_cells);
        }
    }

    zip_writer.finish()?;
    publish_atomic_output(&path_tmp, &path_out)?;
    info!("wrote fastqc output for final total of {} cells", num_cells);
    Ok(())
}

fn validate_zip_cell_id(cell_id: &str) -> Result<()> {
    if cell_id.is_empty() {
        bail!("empty cell id is not supported");
    }
    if cell_id.contains('/') || cell_id.contains('\\') || cell_id == "." || cell_id == ".." {
        bail!("cell id {:?} cannot be used as a zip directory", cell_id);
    }
    Ok(())
}
