use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::thread;

use anyhow::{Context, Result, bail};
use bascet_core::DEFAULT_SIZEOF_ARENA;
use bascet_core::{
    attr::{meta::*, sequence::*},
    *,
};
use bytesize::ByteSize;
use clap::Args;
use crossbeam::channel::{Receiver, Sender};
use tracing::{info, warn};
use zip::ZipWriter;

use crate::utils::{atomic_temp_path, publish_atomic_output};

const DEFAULT_SIZEOF_STREAM_BUFFER: ByteSize = ByteSize::gib(4);
const SKESA_MIN_MEMORY_PER_WORKER_GIB: usize = 4;
const SKESA_MIN_MEMORY_HEADROOM: ByteSize = ByteSize::gib(4);
const SKESA_MEMORY_HEADROOM_FRACTION: f64 = 0.15;

#[derive(Args)]
pub struct SkesaCMD {
    /// Input TIRP file.
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    /// Output zip file.
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    /// Number of cells to assemble concurrently.
    #[arg(short = '@', long = "skesa-workers", default_value_t = 1)]
    pub skesa_workers: usize,

    /// Number of cores to give each skesa assembly.
    #[arg(long = "skesa-cores", default_value_t = 1)]
    pub skesa_cores: usize,

    /// Threads used by the TIRP BGZF decoder.
    #[arg(long = "num-threads-read", default_value_t = 1)]
    pub num_threads_read: usize,

    /// Total memory budget.
    #[arg(
        short = 'm',
        long = "memory",
        default_value_t = ByteSize::gib(32),
        value_parser = clap::value_parser!(ByteSize),
    )]
    pub total_memory: ByteSize,

    /// Minimal k-mer length for assembly.
    #[arg(long = "kmer", default_value_t = 21)]
    pub kmer: usize,

    /// Maximal k-mer length for assembly. 0 means auto.
    #[arg(long = "max-kmer", default_value_t = 0)]
    pub max_kmer: usize,

    /// Number of assembly iterations from minimal to maximal k-mer length.
    #[arg(long = "steps", default_value_t = 11)]
    pub steps: usize,

    /// Minimal count for k-mers retained. If omitted, skesa may estimate this.
    #[arg(long = "min-count")]
    pub min_count: Option<usize>,

    /// Maximum k-mer count for fork tie-breaking. Used with min-count estimation.
    #[arg(long = "max-kmer-count", default_value_t = 10)]
    pub max_kmer_count: usize,

    /// Percentage of reads containing 19-mer for adapter detection. 1.0 disables.
    #[arg(long = "vector-percent", default_value_t = 0.05)]
    pub vector_percent: f64,

    /// Expected insert size for paired reads. 0 means auto.
    #[arg(long = "insert-size", default_value_t = 0)]
    pub insert_size: usize,

    /// Maximum noise to signal ratio acceptable for extension.
    #[arg(long = "fraction", default_value_t = 0.1)]
    pub fraction: f64,

    /// Maximal SNP length.
    #[arg(long = "max-snp-len", default_value_t = 150)]
    pub max_snp_len: usize,

    /// Minimal contig length reported in output.
    #[arg(long = "min-contig", default_value_t = 200)]
    pub min_contig: usize,

    /// Allow additional step for SNP discovery.
    #[arg(long = "allow-snps")]
    pub allow_snps: bool,

    /// Do not use paired-end information.
    #[arg(long = "force-single-ends")]
    pub force_single_ends: bool,

    /// Maximum read pairs per cell fed to assembly. 0 disables the cap. When a cell
    /// exceeds this, only the first N read pairs encountered in the file are used
    /// (no random subsampling).
    #[arg(long = "max-reads-per-cell", default_value_t = 0)]
    pub max_reads_per_cell: usize,

    /// Use the legacy single-pass k-mer counter. This is faster for small cells but can exceed
    /// the memory budget for high-coverage cells.
    #[arg(long = "single-pass-counter", default_value_t = false)]
    pub single_pass_counter: bool,

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

impl SkesaCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        self.validate()?;
        skesa_rs::sorted_counter::set_single_pass_counter(self.single_pass_counter);

        let memory_gb_per_worker = self.memory_gb_per_worker();
        if self.single_pass_counter {
            warn!(
                "SKESA single-pass counter is enabled; this legacy path can exceed the memory budget for high-coverage cells"
            );
        }
        info!(
            total_memory = %self.total_memory,
            sizeof_stream_buffer = %self.sizeof_stream_buffer,
            skesa_workers = self.skesa_workers,
            skesa_memory_gb_per_worker = memory_gb_per_worker,
            "SKESA memory budget"
        );
        let params = SkesaParams {
            memory_gb: memory_gb_per_worker,
            kmer: self.kmer,
            max_kmer: self.max_kmer,
            steps: self.steps,
            min_count: self.min_count.unwrap_or(2),
            estimate_min_count: self.min_count.is_none(),
            max_kmer_count: self.max_kmer_count,
            vector_percent: self.vector_percent,
            insert_size: self.insert_size,
            fraction: self.fraction,
            max_snp_len: self.max_snp_len,
            min_contig: self.min_contig,
            allow_snps: self.allow_snps,
            force_single_ends: self.force_single_ends,
            skesa_cores: self.skesa_cores,
        };

        run_skesa_cells(
            self.path_in.clone(),
            self.path_out.clone(),
            self.num_threads_read,
            self.skesa_workers,
            self.max_reads_per_cell,
            self.sizeof_stream_arena,
            self.sizeof_stream_buffer,
            params,
        )
    }

    fn validate(&self) -> Result<()> {
        if self.skesa_workers == 0 {
            bail!("--skesa-workers must be > 0");
        }
        if self.skesa_cores == 0 {
            bail!("--skesa-cores must be > 0");
        }
        if self.num_threads_read == 0 {
            bail!("--num-threads-read must be > 0");
        }
        if self.memory_gb_per_worker() < SKESA_MIN_MEMORY_PER_WORKER_GIB {
            bail!(
                "--memory must leave at least {SKESA_MIN_MEMORY_PER_WORKER_GIB} GiB per skesa worker after stream and runtime reserves"
            );
        }
        if self.kmer < 21 || self.kmer % 2 == 0 {
            bail!("--kmer must be an odd number >= 21");
        }
        if self.kmer > skesa_rs::kmer::MAX_KMER || self.max_kmer > skesa_rs::kmer::MAX_KMER {
            bail!("unsupported kmer length");
        }
        if self.steps == 0 {
            bail!("--steps must be > 0");
        }
        if self.min_count == Some(0) {
            bail!("--min-count must be > 0");
        }
        if self.max_kmer_count == 0 {
            bail!("--max-kmer-count must be > 0");
        }
        if !(0.0..=1.0).contains(&self.vector_percent) || self.vector_percent == 0.0 {
            bail!("--vector-percent must be > 0 and <= 1");
        }
        if !(0.0..1.0).contains(&self.fraction) {
            bail!("--fraction must be >= 0 and < 1");
        }
        if self.min_contig == 0 {
            bail!("--min-contig must be > 0");
        }
        Ok(())
    }

    fn memory_gb_per_worker(&self) -> usize {
        skesa_memory_gb_per_worker(
            self.total_memory,
            self.skesa_workers,
            self.sizeof_stream_buffer,
        )
    }
}

fn skesa_memory_gb_per_worker(
    total_memory: ByteSize,
    skesa_workers: usize,
    sizeof_stream_buffer: ByteSize,
) -> usize {
    let fractional_headroom =
        ByteSize((total_memory.as_u64() as f64 * SKESA_MEMORY_HEADROOM_FRACTION) as u64);
    let runtime_headroom = SKESA_MIN_MEMORY_HEADROOM
        .as_u64()
        .max(fractional_headroom.as_u64());
    let available = total_memory
        .as_u64()
        .saturating_sub(sizeof_stream_buffer.as_u64())
        .saturating_sub(runtime_headroom);

    (available / skesa_workers.max(1) as u64 / ByteSize::gib(1).as_u64()) as usize
}

#[derive(Clone)]
struct SkesaParams {
    memory_gb: usize,
    kmer: usize,
    max_kmer: usize,
    steps: usize,
    min_count: usize,
    estimate_min_count: bool,
    max_kmer_count: usize,
    vector_percent: f64,
    insert_size: usize,
    fraction: f64,
    max_snp_len: usize,
    min_contig: usize,
    allow_snps: bool,
    force_single_ends: bool,
    skesa_cores: usize,
}

struct CellReads {
    cell_id: String,
    read_set: skesa_rs::api::ReadSet,
}

struct CellAssembly {
    cell_id: String,
    contigs: Vec<u8>,
    log: Vec<u8>,
}

fn run_skesa_cells(
    path_in: PathBuf,
    path_out: PathBuf,
    num_threads_read: usize,
    skesa_workers: usize,
    max_reads_per_cell: usize,
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
    params: SkesaParams,
) -> Result<()> {
    let queue_size = skesa_workers.max(1);
    let (tx_cells, rx_cells) = crossbeam::channel::bounded::<Result<CellReads>>(queue_size);
    let (tx_assemblies, rx_assemblies) =
        crossbeam::channel::bounded::<Result<CellAssembly>>(queue_size);

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

    let mut workers = Vec::with_capacity(skesa_workers);
    for worker_id in 0..skesa_workers {
        let rx_cells = rx_cells.clone();
        let tx_assemblies = tx_assemblies.clone();
        let params = params.clone();
        workers.push(thread::spawn(move || {
            while let Ok(cell) = rx_cells.recv() {
                let assembly = cell.and_then(|cell| {
                    info!("skesa worker {} assembling {}", worker_id, cell.cell_id);
                    assemble_cell(cell, &params)
                });
                if tx_assemblies.send(assembly).is_err() {
                    break;
                }
            }
        }));
    }
    drop(rx_cells);
    drop(tx_assemblies);

    let writer = thread::spawn(move || write_zip(path_out, rx_assemblies));

    reader
        .join()
        .map_err(|_| anyhow::anyhow!("TIRP reader thread panicked"))?;

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("skesa worker thread panicked"))?;
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
    let mut current_read_set = skesa_rs::api::ReadSet::new();
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
                &mut current_read_set,
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
        // Feed the assembly only the first `max_reads_per_cell` pairs (0 = no cap).
        // We keep counting all records so the per-cell log shows the true totals.
        if max_reads_per_cell == 0 || current_read_pairs < max_reads_per_cell {
            // Only r1/r2 are used by assembly; quality/UMI are skipped. Adding straight
            // into the skesa ReadSet avoids materializing an intermediate Vec<ReadPair>
            // and stores the reads 2-bit packed.
            current_read_set.add_pair_bytes(r1, r2);
        }
        current_read_pairs += 1;
        current_bases += r1.len() + r2.len();
    }

    if send_current_cell(
        &tx_cells,
        &mut current_cell_id,
        &mut current_read_set,
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
    current_read_set: &mut skesa_rs::api::ReadSet,
    read_pairs: usize,
    bases: usize,
    max_reads_per_cell: usize,
) -> Result<bool> {
    if current_read_set.read_count() == 0 {
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
    let read_set = std::mem::take(current_read_set);
    tx_cells
        .send(Ok(CellReads { cell_id, read_set }))
        .context("failed to send cell reads to skesa workers")?;
    Ok(true)
}

fn assemble_cell(cell: CellReads, params: &SkesaParams) -> Result<CellAssembly> {
    let CellReads { cell_id, read_set } = cell;
    let mut reads = read_set.into_pairs();

    let output = skesa_rs::output::SharedWriterOutput::with_stream_labels(Vec::new());
    if params.vector_percent < 1.0 {
        skesa_rs::reads_getter::clip_adapters_with_output(
            &mut reads,
            params.vector_percent,
            100,
            &output,
        );
    }

    let raw_kmer_num: usize = reads
        .iter()
        .map(|read_pair| read_pair[0].kmer_num(params.kmer) + read_pair[1].kmer_num(params.kmer))
        .sum();
    skesa_rs::sorted_counter::sorted_counter_plan(
        raw_kmer_num,
        reads.len(),
        params.kmer,
        params.memory_gb,
    )
    .map_err(|e| anyhow::anyhow!("skesa memory plan failed for cell {}: {}", cell_id, e))?;

    let assembler_params = skesa_rs::assembler::AssemblerParams {
        min_kmer: params.kmer,
        max_kmer: params.max_kmer,
        steps: params.steps,
        fraction: params.fraction,
        max_snp_len: params.max_snp_len,
        min_count: params.min_count,
        estimate_min_count: params.estimate_min_count,
        max_kmer_count: params.max_kmer_count,
        force_single_reads: params.force_single_ends,
        insert_size: params.insert_size,
        allow_snps: params.allow_snps,
        ncores: params.skesa_cores,
        memory_gb: params.memory_gb,
        retain_all_graphs: false,
        retain_all_iterations: false,
    };

    let result =
        skesa_rs::assembler::run_assembly_with_output(&reads, &assembler_params, &[], &output);
    let log = output.into_inner().map_err(|_| {
        anyhow::anyhow!("skesa output log writer lock poisoned for cell {}", cell_id)
    })?;
    let mut contigs = Vec::new();
    if let Some((kmer_len, kmers)) = result.graphs.first() {
        skesa_rs::contig_output::write_contigs_with_abundance(
            &mut contigs,
            &result.contigs,
            kmers,
            *kmer_len,
            params.min_contig,
        )?;
    } else {
        skesa_rs::contig_output::write_contigs(&mut contigs, &result.contigs, params.min_contig)?;
    }

    Ok(CellAssembly {
        cell_id,
        contigs,
        log,
    })
}

fn write_zip(path_out: PathBuf, rx_assemblies: Receiver<Result<CellAssembly>>) -> Result<()> {
    let path_tmp = atomic_temp_path(&path_out);
    let file = File::create(&path_tmp)
        .with_context(|| format!("failed to create output zip {}", path_tmp.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(file));
    let options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let mut num_cells = 0_u64;
    for assembly in rx_assemblies {
        let assembly = assembly?;
        let entry_name = format!("{}/contigs.fa", assembly.cell_id);
        zip_writer.start_file(entry_name, options)?;
        let mut contigs = assembly.contigs.as_slice();
        std::io::copy(&mut contigs, &mut zip_writer)?;

        let entry_name = format!("{}/skesa.log", assembly.cell_id);
        zip_writer.start_file(entry_name, options)?;
        let mut log = assembly.log.as_slice();
        std::io::copy(&mut log, &mut zip_writer)?;

        num_cells += 1;
        if num_cells % 100 == 0 {
            info!("wrote skesa output for {} cells", num_cells);
        }
    }

    zip_writer.finish()?;
    publish_atomic_output(&path_tmp, &path_out)?;
    info!("wrote skesa output for final total of {} cells", num_cells);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skesa_worker_memory_reserves_stream_and_runtime_headroom() {
        let memory_gb =
            skesa_memory_gb_per_worker(ByteSize::gb(33), 1, DEFAULT_SIZEOF_STREAM_BUFFER);

        assert_eq!(memory_gb, 22);
    }

    #[test]
    fn skesa_worker_memory_is_split_across_workers_after_reserves() {
        let memory_gb = skesa_memory_gb_per_worker(ByteSize::gib(64), 4, ByteSize::gib(4));

        assert_eq!(memory_gb, 12);
    }
}
