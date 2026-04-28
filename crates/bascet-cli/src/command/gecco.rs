use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Context, Result};
use clap::Args;
use crossbeam::channel::{Receiver, Sender};
use gecco::{orf::SeqRecord, Gecco};
use tracing::info;
use zip::{read::ZipArchive, ZipWriter};

use crate::utils::{atomic_temp_path, publish_atomic_output};

#[derive(Args)]
pub struct GeccoCMD {
    /// Input zip file containing CELLID/contigs.fa entries.
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    /// Output zip file.
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    /// Total GECCO thread budget. Defaults to all available threads.
    #[arg(short = 't', long = "threads")]
    pub threads: Option<usize>,

    /// Number of cells to process concurrently.
    #[arg(short = '@', long = "gecco-workers")]
    pub gecco_workers: Option<usize>,

    /// GECCO data directory containing HMM, CRF model, and InterPro files.
    #[arg(long = "data-dir", value_parser = clap::value_parser!(PathBuf))]
    pub data_dir: Option<PathBuf>,

    /// Minimum probability for cluster membership.
    #[arg(long = "threshold", default_value_t = 0.8)]
    pub threshold: f64,

    /// Minimum number of annotated CDS in a cluster.
    #[arg(long = "cds", default_value_t = 3)]
    pub cds: usize,

    /// Do not mask ambiguous nucleotides during gene prediction.
    #[arg(long = "no-mask")]
    pub no_mask: bool,
}

impl GeccoCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        self.validate()?;
        let threads = self.threads.unwrap_or_else(available_threads);
        let gecco_workers = self.gecco_workers.unwrap_or(threads);
        let gecco_threads = (threads / gecco_workers).max(1);

        let mut builder = Gecco::builder()
            .jobs(gecco_threads)
            .mask(!self.no_mask)
            .threshold(self.threshold)
            .n_cds(self.cds);
        if gecco_threads > 1 {
            let thread_pool = Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .context("failed to initialize GECCO Rayon thread pool")?,
            );
            builder = builder.thread_pool(Arc::clone(&thread_pool));
        }
        if let Some(data_dir) = &self.data_dir {
            builder = builder.data_dir(data_dir.clone());
        }
        let pipeline = builder
            .build()
            .context("failed to initialize GECCO pipeline")?;

        run_gecco_zip(
            self.path_in.clone(),
            self.path_out.clone(),
            Arc::new(pipeline),
            gecco_workers,
        )
    }

    fn validate(&self) -> Result<()> {
        if self.threads == Some(0) {
            bail!("--threads must be > 0");
        }
        if self.gecco_workers == Some(0) {
            bail!("--gecco-workers must be > 0");
        }
        if !(0.0..=1.0).contains(&self.threshold) {
            bail!("--threshold must be between 0 and 1");
        }
        if self.cds == 0 {
            bail!("--cds must be > 0");
        }
        Ok(())
    }
}

fn available_threads() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

struct CellContigs {
    cell_id: String,
    contigs: Vec<u8>,
}

struct CellGecco {
    cell_id: String,
    reports: GeccoReports,
}

fn run_gecco_zip(
    path_in: PathBuf,
    path_out: PathBuf,
    pipeline: Arc<Gecco>,
    gecco_workers: usize,
) -> Result<()> {
    let queue_size = gecco_workers * 2;
    let (tx_cells, rx_cells) = crossbeam::channel::bounded::<Result<CellContigs>>(queue_size);
    let (tx_reports, rx_reports) = crossbeam::channel::bounded::<Result<CellGecco>>(queue_size);

    let writer = thread::spawn(move || write_zip(path_out, rx_reports));

    let mut workers = Vec::with_capacity(gecco_workers);
    for worker_id in 0..gecco_workers {
        let rx_cells = rx_cells.clone();
        let tx_reports = tx_reports.clone();
        let pipeline = Arc::clone(&pipeline);
        workers.push(thread::spawn(move || {
            while let Ok(cell) = rx_cells.recv() {
                let result = cell.and_then(|cell| {
                    info!("gecco worker {} processing {}", worker_id, cell.cell_id);
                    let records = read_fasta_records(&cell.contigs, &cell.cell_id)?;
                    let reports = run_gecco_cell(&pipeline, &records)
                        .with_context(|| format!("GECCO failed for cell {}", cell.cell_id))?;
                    Ok(CellGecco {
                        cell_id: cell.cell_id,
                        reports,
                    })
                });
                if tx_reports.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(rx_cells);
    drop(tx_reports);

    if let Err(e) = read_zip_cells(path_in, tx_cells.clone()) {
        let _ = tx_cells.send(Err(e));
    }
    drop(tx_cells);

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("gecco worker thread panicked"))?;
    }

    writer
        .join()
        .map_err(|_| anyhow::anyhow!("zip writer thread panicked"))??;

    Ok(())
}

fn read_zip_cells(path_in: PathBuf, tx_cells: Sender<Result<CellContigs>>) -> Result<()> {
    info!("Reading GECCO input zip {}", path_in.display());
    let input = File::open(&path_in)
        .with_context(|| format!("failed to open input zip {}", path_in.display()))?;
    let mut archive = ZipArchive::new(BufReader::new(input))
        .with_context(|| format!("failed to read input zip {}", path_in.display()))?;

    let mut num_cells = 0_u64;
    for index in 0..archive.len() {
        let mut file = archive.by_index(index)?;
        let Some(cell_id) = contigs_cell_id(file.name()).map(str::to_owned) else {
            continue;
        };
        validate_zip_cell_id(&cell_id)?;

        let mut contigs = Vec::new();
        file.read_to_end(&mut contigs)
            .with_context(|| format!("failed to read {}/contigs.fa", cell_id))?;

        num_cells += 1;
        tx_cells
            .send(Ok(CellContigs { cell_id, contigs }))
            .context("failed to send contigs to gecco workers")?;
        if num_cells % 1000 == 0 {
            info!("queued {} cells for GECCO", num_cells);
        }
    }

    info!("queued final total of {} cells for GECCO", num_cells);
    Ok(())
}

fn contigs_cell_id(path: &str) -> Option<&str> {
    path.strip_suffix("/contigs.fa")
}

fn read_fasta_records(contigs: &[u8], cell_id: &str) -> Result<Vec<SeqRecord>> {
    let reader = bio::io::fasta::Reader::new(Cursor::new(contigs));
    let mut records = Vec::new();
    for record in reader.records() {
        let record =
            record.with_context(|| format!("failed to parse FASTA for cell {}", cell_id))?;
        records.push(SeqRecord {
            id: record.id().to_string(),
            seq: String::from_utf8_lossy(record.seq()).into_owned(),
        });
    }
    Ok(records)
}

struct GeccoReports {
    genes: Vec<u8>,
    features: Vec<u8>,
    clusters: Vec<u8>,
    clusters_gbk: Vec<u8>,
    log: Vec<u8>,
}

fn run_gecco_cell(pipeline: &Gecco, records: &[SeqRecord]) -> Result<GeccoReports> {
    let output = gecco::output::SharedWriterOutput::with_stream_labels(Vec::new());
    let results = pipeline.scan_with_output(records, &output)?;
    let log = output
        .into_inner()
        .map_err(|_| anyhow::anyhow!("GECCO output log writer lock poisoned"))?;

    let mut genes = Vec::new();
    results.write_gene_table(&mut genes)?;

    let mut features = Vec::new();
    results.write_feature_table(&mut features)?;

    let mut clusters = Vec::new();
    results.write_cluster_table(&mut clusters)?;

    let mut clusters_gbk = Vec::new();
    results.write_clusters_merged_gbk(&mut clusters_gbk)?;

    Ok(GeccoReports {
        genes,
        features,
        clusters,
        clusters_gbk,
        log,
    })
}

fn write_zip(path_out: PathBuf, rx_reports: Receiver<Result<CellGecco>>) -> Result<()> {
    let path_tmp = atomic_temp_path(&path_out);
    let output = File::create(&path_tmp)
        .with_context(|| format!("failed to create output zip {}", path_tmp.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(output));
    let options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let mut num_cells = 0_u64;
    for cell in rx_reports {
        let cell = cell?;
        write_cell_reports(&mut zip_writer, options, &cell.cell_id, cell.reports)?;

        num_cells += 1;
        if num_cells % 100 == 0 {
            info!("wrote GECCO output for {} cells", num_cells);
        }
    }

    zip_writer.finish()?;
    publish_atomic_output(&path_tmp, &path_out)?;
    info!("wrote GECCO output for final total of {} cells", num_cells);
    Ok(())
}

fn write_cell_reports(
    zip_writer: &mut ZipWriter<BufWriter<File>>,
    options: zip::write::FileOptions<()>,
    cell_id: &str,
    reports: GeccoReports,
) -> Result<()> {
    write_zip_entry(zip_writer, options, cell_id, "genes.tsv", &reports.genes)?;
    write_zip_entry(
        zip_writer,
        options,
        cell_id,
        "features.tsv",
        &reports.features,
    )?;
    write_zip_entry(
        zip_writer,
        options,
        cell_id,
        "clusters.tsv",
        &reports.clusters,
    )?;
    write_zip_entry(
        zip_writer,
        options,
        cell_id,
        "clusters.gbk",
        &reports.clusters_gbk,
    )?;
    write_zip_entry(zip_writer, options, cell_id, "gecco.log", &reports.log)?;
    Ok(())
}

fn write_zip_entry(
    zip_writer: &mut ZipWriter<BufWriter<File>>,
    options: zip::write::FileOptions<()>,
    cell_id: &str,
    file_name: &str,
    contents: &[u8],
) -> Result<()> {
    zip_writer.start_file(format!("{}/{}", cell_id, file_name), options)?;
    let mut contents = contents;
    std::io::copy(&mut contents, zip_writer)?;
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
