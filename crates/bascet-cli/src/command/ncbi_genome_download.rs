use anyhow::{Context, bail};
use clap::Args;
use flate2::read::GzDecoder;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use tracing::info;
use zip::{ZipArchive, ZipWriter};

use crate::fileformat::shard::ReadPair;
use crate::fileformat::tirp::{BascetTIRPWriterFactory, get_tbi_path_for_tirp};
use crate::fileformat::{ConstructFromPath, ReadPairWriter};
use crate::utils::{atomic_temp_path_in_dir, publish_atomic_output};

#[derive(Args)]
pub struct NcbiGenomeDownloadCMD {
    #[arg(
        short = 'i',
        long = "input",
        help = "NCBI genome list shard with cell_id and ftp_path or url columns"
    )]
    pub input: PathBuf,

    #[arg(short = 'o', long = "out", help = "Output bascet zip")]
    pub out: PathBuf,

    #[arg(
        long = "temp",
        help = "Temporary directory for downloads and zip fragments"
    )]
    pub temp: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        default_value_t = 4,
        help = "Number of parallel genome workers"
    )]
    pub threads: usize,

    #[arg(
        long = "queue-size",
        help = "Maximum completed compressed fragments waiting for final zip writing; defaults to max(threads, 4)"
    )]
    pub queue_size: Option<usize>,

    #[arg(
        long = "download-starts-per-second",
        default_value_t = 2.0,
        help = "Global rate limit for starting NCBI downloads"
    )]
    pub download_starts_per_second: f64,

    #[arg(
        long = "max-retries",
        default_value_t = 5,
        help = "Download retry count"
    )]
    pub max_retries: usize,

    #[arg(
        long = "keep-temp",
        help = "Keep downloaded .gz and fragment zip files"
    )]
    pub keep_temp: bool,
}

#[derive(Debug, Clone)]
struct GenomeJob {
    index: usize,
    cell_id: String,
    url: String,
}

#[derive(Debug)]
struct CompletedFragment {
    cell_id: String,
    url: String,
    fragment_path: PathBuf,
    entry_name: String,
}

#[derive(Debug)]
struct CompletedReads {
    index: usize,
    cell_id: String,
    reads: Vec<ReadPair>,
}

enum CompletedGenome {
    Zip(CompletedFragment),
    Tirp(CompletedReads),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Zip,
    Tirp,
}

impl NcbiGenomeDownloadCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        if self.threads == 0 {
            bail!("--threads must be at least 1");
        }
        if self.download_starts_per_second <= 0.0 || !self.download_starts_per_second.is_finite() {
            bail!("--download-starts-per-second must be a positive finite number");
        }
        let queue_size = self.queue_size.unwrap_or_else(|| self.threads.max(4));
        if queue_size == 0 {
            bail!("--queue-size must be at least 1");
        }

        fs::create_dir_all(&self.temp)
            .with_context(|| format!("failed to create temp directory {}", self.temp.display()))?;
        let download_dir = self.temp.join("ncbi-genomes-download");
        let fragment_dir = self.temp.join("ncbi-genomes-fragments");
        fs::create_dir_all(&download_dir)?;
        fs::create_dir_all(&fragment_dir)?;

        let jobs = read_jobs(&self.input)?;
        if jobs.is_empty() {
            bail!(
                "input shard contains no genome rows: {}",
                self.input.display()
            );
        }
        let output_format = OutputFormat::from_path(&self.out)?;

        info!(
            genomes = jobs.len(),
            threads = self.threads,
            queue_size = queue_size,
            output = %self.out.display(),
            output_format = ?output_format,
            "Downloading NCBI genomes"
        );

        let out_tmp = atomic_temp_path_in_dir(&self.out, &self.temp);
        let (tx_completed, rx_completed) =
            mpsc::sync_channel::<anyhow::Result<CompletedGenome>>(queue_size);
        let work = Arc::new(Mutex::new(jobs.into_iter()));
        let rate_limiter = Arc::new(DownloadRateLimiter::new(self.download_starts_per_second));
        let mut workers = Vec::with_capacity(self.threads);

        for worker_id in 0..self.threads {
            let tx_completed = tx_completed.clone();
            let work = Arc::clone(&work);
            let rate_limiter = Arc::clone(&rate_limiter);
            let fragment_dir = fragment_dir.clone();
            let max_retries = self.max_retries;
            let worker_output_format = output_format;

            workers.push(
                std::thread::Builder::new()
                    .name(format!("ncbi-genome-worker-{worker_id}"))
                    .spawn(move || {
                        loop {
                            let Some(job) = work
                                .lock()
                                .expect("NCBI genome work queue mutex poisoned")
                                .next()
                            else {
                                break;
                            };

                            let result = process_job(
                                worker_id,
                                &job,
                                &fragment_dir,
                                &rate_limiter,
                                max_retries,
                                worker_output_format,
                            );
                            let failed = result.is_err();
                            if tx_completed.send(result).is_err() || failed {
                                break;
                            }
                        }
                    })
                    .context("failed to spawn NCBI genome worker")?,
            );
        }
        drop(tx_completed);

        let writer_result =
            write_final_output(&out_tmp, output_format, rx_completed, self.keep_temp);

        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("NCBI genome worker panicked"))?;
        }

        let written = writer_result?;
        if written == 0 {
            bail!("no genome fragments were written");
        }
        publish_atomic_output(&out_tmp, &self.out)
            .with_context(|| format!("failed to publish {}", self.out.display()))?;
        if output_format == OutputFormat::Tirp {
            let out_tbi = get_tbi_path_for_tirp(&self.out);
            let out_tbi_tmp = get_tbi_path_for_tirp(&out_tmp);
            publish_atomic_output(&out_tbi_tmp, &out_tbi)
                .with_context(|| format!("failed to publish {}", out_tbi.display()))?;
        }
        info!(genomes = written, "Finished NCBI genome download");
        Ok(())
    }
}

impl OutputFormat {
    fn from_path(path: &Path) -> anyhow::Result<Self> {
        let path = path.to_string_lossy();
        if path.ends_with(".zip") {
            Ok(Self::Zip)
        } else if path.ends_with(".tirp.gz") || path.ends_with(".tirp") {
            Ok(Self::Tirp)
        } else {
            bail!(
                "cannot infer output format from {}; expected .zip or .tirp.gz",
                path
            )
        }
    }
}

fn read_jobs(path: &Path) -> anyhow::Result<Vec<GenomeJob>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        bail!("empty input shard: {}", path.display());
    }
    let header: Vec<String> = header
        .trim_end_matches(['\r', '\n'])
        .split('\t')
        .map(|s| s.to_string())
        .collect();
    let cell_idx = find_column(&header, &["cell_id", "cell", "assembly_accession"])?;
    let url_idx = find_column(&header, &["url", "genomic_fna_url"]).ok();
    let ftp_idx = find_column(&header, &["ftp_path", "ftp"]).ok();
    if url_idx.is_none() && ftp_idx.is_none() {
        bail!("input shard must contain either a url/genomic_fna_url or ftp_path column");
    }

    let mut jobs = Vec::new();
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let cell_id = get_field(&fields, cell_idx, line_no + 2, "cell_id")?.to_string();
        validate_zip_cell_id(&cell_id)?;
        let url = if let Some(url_idx) = url_idx {
            get_field(&fields, url_idx, line_no + 2, "url")?.to_string()
        } else {
            genomic_fna_url(get_field(
                &fields,
                ftp_idx.unwrap(),
                line_no + 2,
                "ftp_path",
            )?)?
        };
        jobs.push(GenomeJob {
            index: jobs.len(),
            cell_id,
            url,
        });
    }
    Ok(jobs)
}

fn find_column(header: &[String], names: &[&str]) -> anyhow::Result<usize> {
    header
        .iter()
        .position(|column| names.iter().any(|name| column == name))
        .ok_or_else(|| anyhow::anyhow!("missing required column; expected one of {:?}", names))
}

fn get_field<'a>(
    fields: &'a [&str],
    idx: usize,
    line_no: usize,
    column: &str,
) -> anyhow::Result<&'a str> {
    let value = fields.get(idx).copied().unwrap_or("").trim();
    if value.is_empty() {
        bail!("line {line_no}: empty {column}");
    }
    Ok(value)
}

fn genomic_fna_url(ftp_path: &str) -> anyhow::Result<String> {
    let ftp_path = ftp_path.trim_end_matches('/');
    if ftp_path.ends_with("_genomic.fna.gz") {
        return Ok(normalize_ncbi_url(ftp_path));
    }
    let basename = ftp_path
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("cannot derive basename from ftp_path {ftp_path:?}"))?;
    Ok(format!(
        "{}/{}_genomic.fna.gz",
        normalize_ncbi_url(ftp_path),
        basename
    ))
}

fn normalize_ncbi_url(url: &str) -> String {
    url.replacen("ftp://", "https://", 1)
}

fn process_job(
    worker_id: usize,
    job: &GenomeJob,
    fragment_dir: &Path,
    rate_limiter: &DownloadRateLimiter,
    max_retries: usize,
    output_format: OutputFormat,
) -> anyhow::Result<CompletedGenome> {
    match output_format {
        OutputFormat::Zip => {
            let fragment_path = fragment_dir.join(format!("{}-{}.zip", job.index, job.cell_id));
            download_and_write_fragment_with_retries(
                worker_id,
                job,
                &fragment_path,
                rate_limiter,
                max_retries,
            )
            .with_context(|| {
                format!("failed to download and compress genome for {}", job.cell_id)
            })?;
            Ok(CompletedGenome::Zip(CompletedFragment {
                cell_id: job.cell_id.clone(),
                url: job.url.clone(),
                fragment_path,
                entry_name: format!("{}/contigs.fa", job.cell_id),
            }))
        }
        OutputFormat::Tirp => {
            let reads =
                download_and_parse_reads_with_retries(worker_id, job, rate_limiter, max_retries)
                    .with_context(|| {
                        format!("failed to download and parse genome for {}", job.cell_id)
                    })?;
            Ok(CompletedGenome::Tirp(CompletedReads {
                index: job.index,
                cell_id: job.cell_id.clone(),
                reads,
            }))
        }
    }
}

fn download_and_write_fragment_with_retries(
    worker_id: usize,
    job: &GenomeJob,
    fragment_path: &Path,
    rate_limiter: &DownloadRateLimiter,
    max_retries: usize,
) -> anyhow::Result<()> {
    let mut delay = Duration::from_secs(2);
    for attempt in 0..=max_retries {
        rate_limiter.wait();
        info!(
            cell = %job.cell_id,
            worker = worker_id,
            url = %job.url,
            attempt = attempt + 1,
            "Starting genome download"
        );
        let result = download_and_write_fragment_once(&job.cell_id, &job.url, fragment_path);
        if result.is_ok() {
            return result;
        }
        if attempt == max_retries {
            return result
                .with_context(|| format!("download failed after {} attempts", attempt + 1));
        }
        let _ = fs::remove_file(fragment_path);
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_secs(60));
    }
    unreachable!()
}

fn download_and_write_fragment_once(
    cell_id: &str,
    url: &str,
    fragment_path: &Path,
) -> anyhow::Result<()> {
    if let Some(path) = url.strip_prefix("file://") {
        let mut input =
            File::open(path).with_context(|| format!("failed to open local source {path}"))?;
        write_fragment_zip_from_gzip_reader(cell_id, &mut input, fragment_path)?;
        return Ok(());
    }

    let mut response = ureq::get(url)
        .header("user-agent", concat!("bascet/", env!("CARGO_PKG_VERSION")))
        .call()
        .with_context(|| format!("HTTP GET failed for {url}"))?;
    write_fragment_zip_from_gzip_reader(
        cell_id,
        &mut response.body_mut().as_reader(),
        fragment_path,
    )
    .with_context(|| format!("failed to stream and compress response body for {url}"))?;
    Ok(())
}

fn download_and_parse_reads_with_retries(
    worker_id: usize,
    job: &GenomeJob,
    rate_limiter: &DownloadRateLimiter,
    max_retries: usize,
) -> anyhow::Result<Vec<ReadPair>> {
    let mut delay = Duration::from_secs(2);
    for attempt in 0..=max_retries {
        rate_limiter.wait();
        info!(
            cell = %job.cell_id,
            worker = worker_id,
            url = %job.url,
            attempt = attempt + 1,
            "Starting genome download"
        );
        let result = download_and_parse_reads_once(&job.url);
        if result.is_ok() {
            return result;
        }
        if attempt == max_retries {
            return result
                .with_context(|| format!("download failed after {} attempts", attempt + 1));
        }
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_secs(60));
    }
    unreachable!()
}

fn download_and_parse_reads_once(url: &str) -> anyhow::Result<Vec<ReadPair>> {
    if let Some(path) = url.strip_prefix("file://") {
        let input =
            File::open(path).with_context(|| format!("failed to open local source {path}"))?;
        let decoder = GzDecoder::new(BufReader::new(input));
        return fasta_reads_from_reader(BufReader::new(decoder));
    }

    let mut response = ureq::get(url)
        .header("user-agent", concat!("bascet/", env!("CARGO_PKG_VERSION")))
        .call()
        .with_context(|| format!("HTTP GET failed for {url}"))?;
    let decoder = GzDecoder::new(response.body_mut().as_reader());
    fasta_reads_from_reader(BufReader::new(decoder))
        .with_context(|| format!("failed to stream and parse response body for {url}"))
}

fn fasta_reads_from_reader<R: BufRead>(reader: R) -> anyhow::Result<Vec<ReadPair>> {
    let mut reads = Vec::new();
    let mut seq = Vec::new();
    let mut saw_header = false;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim_end_matches('\r');
        if line.starts_with('>') {
            if saw_header {
                push_fasta_read(&mut reads, &mut seq);
            }
            saw_header = true;
        } else if !line.is_empty() && !line.starts_with(';') {
            seq.extend_from_slice(line.as_bytes());
        }
    }

    if saw_header {
        push_fasta_read(&mut reads, &mut seq);
    }
    if reads.is_empty() {
        bail!("FASTA contained no records");
    }
    Ok(reads)
}

fn push_fasta_read(reads: &mut Vec<ReadPair>, seq: &mut Vec<u8>) {
    if seq.is_empty() {
        return;
    }
    let r1 = std::mem::take(seq);
    let q1 = vec![b'F'; r1.len()];
    reads.push(ReadPair {
        r1,
        r2: Vec::new(),
        q1,
        q2: Vec::new(),
        umi: Vec::new(),
    });
}

fn write_fragment_zip_from_gzip_reader<R: Read>(
    cell_id: &str,
    gzip_reader: R,
    fragment_path: &Path,
) -> anyhow::Result<()> {
    let mut decoder = GzDecoder::new(BufReader::new(gzip_reader));
    let output = File::create(fragment_path)
        .with_context(|| format!("failed to create fragment {}", fragment_path.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(output));
    let options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    zip_writer.start_file(format!("{cell_id}/contigs.fa"), options)?;
    std::io::copy(&mut decoder, &mut zip_writer)?;
    zip_writer.finish()?;
    Ok(())
}

fn write_final_output(
    out_tmp: &Path,
    output_format: OutputFormat,
    rx_completed: mpsc::Receiver<anyhow::Result<CompletedGenome>>,
    keep_temp: bool,
) -> anyhow::Result<usize> {
    match output_format {
        OutputFormat::Zip => write_final_zip(out_tmp, rx_completed, keep_temp),
        OutputFormat::Tirp => write_final_tirp(out_tmp, rx_completed),
    }
}

fn write_final_zip(
    out_tmp: &Path,
    rx_completed: mpsc::Receiver<anyhow::Result<CompletedGenome>>,
    keep_temp: bool,
) -> anyhow::Result<usize> {
    let output = File::create(out_tmp)
        .with_context(|| format!("failed to create output zip {}", out_tmp.display()))?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(output));
    let manifest_options: zip::write::FileOptions<()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    let mut manifest = Vec::new();
    writeln!(manifest, "cell_id\turl\tzip_entry\tstatus")?;

    let mut written = 0usize;
    for completed in rx_completed {
        let completed = match completed? {
            CompletedGenome::Zip(completed) => completed,
            CompletedGenome::Tirp(_) => bail!("internal error: got TIRP reads while writing ZIP"),
        };
        let input = File::open(&completed.fragment_path).with_context(|| {
            format!(
                "failed to open fragment zip {}",
                completed.fragment_path.display()
            )
        })?;
        let mut archive = ZipArchive::new(BufReader::new(input))?;
        if archive.len() != 1 {
            bail!(
                "fragment {} has {} entries, expected 1",
                completed.fragment_path.display(),
                archive.len()
            );
        }
        let file = archive.by_index(0)?;
        zip_writer.raw_copy_file(file)?;
        writeln!(
            manifest,
            "{}\t{}\t{}\tok",
            completed.cell_id, completed.url, completed.entry_name
        )?;
        written += 1;
        if !keep_temp {
            fs::remove_file(&completed.fragment_path).with_context(|| {
                format!("failed to remove {}", completed.fragment_path.display())
            })?;
        }
        if written % 100 == 0 {
            info!(genomes = written, "Wrote NCBI genome fragments");
        }
    }

    zip_writer.start_file("manifest.tsv", manifest_options)?;
    zip_writer.write_all(&manifest)?;
    zip_writer.finish()?;
    Ok(written)
}

fn write_final_tirp(
    out_tmp: &Path,
    rx_completed: mpsc::Receiver<anyhow::Result<CompletedGenome>>,
) -> anyhow::Result<usize> {
    let factory = BascetTIRPWriterFactory::new();
    let mut writer = factory.new_from_path(&out_tmp.to_path_buf())?;
    let mut pending = BTreeMap::new();
    let mut next_to_write = 0usize;
    let mut written = 0usize;

    for completed in rx_completed {
        let completed = match completed? {
            CompletedGenome::Tirp(completed) => completed,
            CompletedGenome::Zip(_) => bail!("internal error: got ZIP fragment while writing TIRP"),
        };
        pending.insert(completed.index, completed);

        while let Some(completed) = pending.remove(&next_to_write) {
            let reads = Arc::new(completed.reads);
            writer.write_reads_for_cell(&completed.cell_id, &reads);
            written += 1;
            next_to_write += 1;
            if written % 100 == 0 {
                info!(genomes = written, "Wrote NCBI genomes to TIRP");
            }
        }
    }

    if !pending.is_empty() {
        bail!("not all TIRP genomes could be written in input order");
    }
    writer.writing_done()?;
    Ok(written)
}

struct DownloadRateLimiter {
    interval: Duration,
    next_start: Mutex<Instant>,
}

impl DownloadRateLimiter {
    fn new(starts_per_second: f64) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / starts_per_second),
            next_start: Mutex::new(Instant::now()),
        }
    }

    fn wait(&self) {
        let mut next_start = self
            .next_start
            .lock()
            .expect("download rate limiter mutex poisoned");
        let now = Instant::now();
        if *next_start > now {
            std::thread::sleep(*next_start - now);
        }
        let now = Instant::now();
        *next_start = now.max(*next_start) + self.interval;
    }
}

fn validate_zip_cell_id(cell_id: &str) -> anyhow::Result<()> {
    if cell_id.is_empty() {
        bail!("empty cell id is not supported");
    }
    if cell_id.contains('/') || cell_id.contains('\\') || cell_id == "." || cell_id == ".." {
        bail!("cell id {:?} cannot be used as a zip directory", cell_id);
    }
    Ok(())
}
