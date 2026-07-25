use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Context, Result, bail};
use clap::Args;
use tracing::{info, warn};
use zip::read::ZipArchive;

use crate::fileformat::new_anndata::SparseMatrixAnnDataWriter;
use crate::utils::{atomic_temp_path, publish_atomic_output};

const DEFAULT_CONTIG_NAME: &str = "contigs.fa";

#[derive(Args)]
pub struct QuastCMD {
    /// Input Bascet contig zip. Each cell is expected to contain contigs.fa.
    #[arg(short = 'i', value_parser = clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    /// Output h5ad file with empty X and QUAST metrics in obs.
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,

    /// Number of cells to process concurrently.
    #[arg(short = '@', long = "quast-workers")]
    pub quast_workers: Option<usize>,

    /// Minimum contig length included in primary QUAST metrics.
    #[arg(long = "min-contig", default_value_t = 0)]
    pub min_contig: usize,

    /// Cell-local contig file name.
    #[arg(long = "contig-name", default_value = DEFAULT_CONTIG_NAME)]
    pub contig_name: String,
}

impl QuastCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        self.validate()?;
        run_quast_cells(
            self.path_in.clone(),
            self.path_out.clone(),
            self.effective_quast_workers(),
            self.min_contig,
            self.contig_name.clone(),
        )
    }

    fn validate(&self) -> Result<()> {
        if self.quast_workers == Some(0) {
            bail!("--quast-workers must be > 0");
        }
        if self.contig_name.is_empty()
            || self.contig_name.contains('/')
            || self.contig_name.contains('\\')
        {
            bail!("--contig-name must be a cell-local file name");
        }
        Ok(())
    }

    fn effective_quast_workers(&self) -> usize {
        self.quast_workers.unwrap_or_else(available_threads)
    }
}

fn available_threads() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

struct CellInput {
    cell_id: String,
    entry_name: String,
}

struct CellQuast {
    cell_id: String,
    stats: AssemblyStats,
}

#[derive(Debug, Default)]
struct AssemblyStats {
    all_lengths: Vec<usize>,
    lengths: Vec<usize>,
    total_gc: usize,
    total_acgt: usize,
    ns: usize,
}

fn run_quast_cells(
    path_in: PathBuf,
    path_out: PathBuf,
    quast_workers: usize,
    min_contig: usize,
    contig_name: String,
) -> Result<()> {
    let cells = list_contig_cells(&path_in, &contig_name)?;
    info!("queued {} cells with {}", cells.len(), contig_name);

    let queue_size = quast_workers.max(1) * 2;
    let (tx_cells, rx_cells) = crossbeam::channel::bounded::<CellInput>(queue_size);
    let (tx_reports, rx_reports) = crossbeam::channel::bounded::<Result<CellQuast>>(queue_size);

    let reader = thread::spawn(move || {
        for cell in cells {
            if tx_cells.send(cell).is_err() {
                break;
            }
        }
    });

    let mut workers = Vec::with_capacity(quast_workers);
    for worker_id in 0..quast_workers {
        let path_in = path_in.clone();
        let rx_cells = rx_cells.clone();
        let tx_reports = tx_reports.clone();
        workers.push(thread::spawn(move || {
            while let Ok(cell) = rx_cells.recv() {
                info!("quast worker {} processing {}", worker_id, cell.cell_id);
                let report = process_cell(&path_in, cell, min_contig);
                if tx_reports.send(report).is_err() {
                    break;
                }
            }
        }));
    }
    drop(rx_cells);
    drop(tx_reports);

    let mut reports = Vec::new();
    for report in rx_reports {
        reports.push(report?);
    }

    reader
        .join()
        .map_err(|_| anyhow::anyhow!("quast reader thread panicked"))?;
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("quast worker thread panicked"))?;
    }
    write_anndata(path_out, reports)
}

fn list_contig_cells(path_in: &Path, contig_name: &str) -> Result<Vec<CellInput>> {
    let file = File::open(path_in)
        .with_context(|| format!("failed to open input zip {}", path_in.display()))?;
    let mut zip = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("failed to read input zip {}", path_in.display()))?;
    let expected_suffix = format!("/{contig_name}");
    let mut cells = Vec::new();
    let mut seen_cells = BTreeSet::new();

    for i in 0..zip.len() {
        let entry = zip.by_index(i)?;
        if !entry.is_file() {
            continue;
        }
        let Some(entry_name) = entry
            .enclosed_name()
            .map(|p| p.to_string_lossy().into_owned())
        else {
            warn!("skipping unsafe zip entry name {:?}", entry.name());
            continue;
        };
        if !entry_name.ends_with(&expected_suffix) {
            continue;
        }
        let Some((cell_id, _)) = entry_name.split_once('/') else {
            continue;
        };
        validate_zip_cell_id(cell_id)?;
        if seen_cells.insert(cell_id.to_string()) {
            cells.push(CellInput {
                cell_id: cell_id.to_string(),
                entry_name,
            });
        }
    }

    if cells.is_empty() {
        bail!(
            "no cells with {} found in input zip {}",
            contig_name,
            path_in.display()
        );
    }
    Ok(cells)
}

fn process_cell(path_in: &Path, cell: CellInput, min_contig: usize) -> Result<CellQuast> {
    let file = File::open(path_in)
        .with_context(|| format!("failed to open input zip {}", path_in.display()))?;
    let mut zip = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("failed to read input zip {}", path_in.display()))?;
    let mut entry = zip
        .by_name(&cell.entry_name)
        .with_context(|| format!("missing zip entry {}", cell.entry_name))?;

    let stats = read_fasta_stats(&mut entry, min_contig)
        .with_context(|| format!("failed to parse {}", cell.entry_name))?;
    Ok(CellQuast {
        cell_id: cell.cell_id,
        stats,
    })
}

fn read_fasta_stats<R: Read>(reader: R, min_contig: usize) -> Result<AssemblyStats> {
    let mut stats = AssemblyStats::default();
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::new();
    let mut current_len = 0usize;
    let mut current_gc = 0usize;
    let mut current_acgt = 0usize;
    let mut current_ns = 0usize;
    let mut saw_record = false;

    loop {
        buf.clear();
        let bytes = std::io::BufRead::read_until(&mut reader, b'\n', &mut buf)?;
        if bytes == 0 {
            break;
        }
        while matches!(buf.last(), Some(b'\n' | b'\r')) {
            buf.pop();
        }
        if buf.starts_with(b">") {
            if saw_record {
                add_contig(
                    &mut stats,
                    current_len,
                    current_gc,
                    current_acgt,
                    current_ns,
                    min_contig,
                );
            }
            saw_record = true;
            current_len = 0;
            current_gc = 0;
            current_acgt = 0;
            current_ns = 0;
            continue;
        }
        for b in &buf {
            current_len += 1;
            match b.to_ascii_uppercase() {
                b'G' | b'C' => {
                    current_gc += 1;
                    current_acgt += 1;
                }
                b'A' | b'T' => current_acgt += 1,
                b'N' => current_ns += 1,
                _ => {}
            }
        }
    }

    if saw_record {
        add_contig(
            &mut stats,
            current_len,
            current_gc,
            current_acgt,
            current_ns,
            min_contig,
        );
    }
    stats.all_lengths.sort_unstable_by(|a, b| b.cmp(a));
    stats.lengths.sort_unstable_by(|a, b| b.cmp(a));
    Ok(stats)
}

fn add_contig(
    stats: &mut AssemblyStats,
    len: usize,
    gc: usize,
    acgt: usize,
    ns: usize,
    min_contig: usize,
) {
    stats.all_lengths.push(len);
    if len >= min_contig {
        stats.lengths.push(len);
        stats.total_gc += gc;
        stats.total_acgt += acgt;
        stats.ns += ns;
    }
}

fn write_anndata(path_out: PathBuf, mut reports: Vec<CellQuast>) -> Result<()> {
    reports.sort_unstable_by(|a, b| a.cell_id.cmp(&b.cell_id));
    let path_tmp = atomic_temp_path(&path_out);
    let mut file = SparseMatrixAnnDataWriter::create_anndata(&path_tmp)?;
    let n_rows = reports.len() as u32;
    let n_cols = 0;
    let empty_matrix = sprs::CsMat::<u32>::new(
        (n_rows as usize, n_cols as usize),
        vec![0; n_rows as usize + 1],
        Vec::new(),
        Vec::new(),
    );

    let empty_features = Vec::new();
    let cell_names: Vec<String> = reports
        .iter()
        .map(|report| report.cell_id.clone())
        .collect();
    file.store_feature_names(&empty_features)?;
    file.store_cell_obs_f64(&cell_names, &obs_columns(&reports))?;
    file.store_sparse_count_matrix(&empty_matrix, n_rows, n_cols)?;
    file.close()?;
    publish_atomic_output(&path_tmp, &path_out)?;
    info!("wrote quast output for final total of {} cells", n_rows);
    Ok(())
}

fn obs_columns(reports: &[CellQuast]) -> Vec<(&'static str, Vec<f64>)> {
    let mut contigs_ge_0bp = Vec::with_capacity(reports.len());
    let mut contigs_ge_1000bp = Vec::with_capacity(reports.len());
    let mut contigs_ge_5000bp = Vec::with_capacity(reports.len());
    let mut contigs_ge_10000bp = Vec::with_capacity(reports.len());
    let mut contigs_ge_25000bp = Vec::with_capacity(reports.len());
    let mut contigs_ge_50000bp = Vec::with_capacity(reports.len());
    let mut contigs = Vec::with_capacity(reports.len());
    let mut largest_contig = Vec::with_capacity(reports.len());
    let mut total_length = Vec::with_capacity(reports.len());
    let mut total_length_ge_0bp = Vec::with_capacity(reports.len());
    let mut total_length_ge_1000bp = Vec::with_capacity(reports.len());
    let mut total_length_ge_5000bp = Vec::with_capacity(reports.len());
    let mut total_length_ge_10000bp = Vec::with_capacity(reports.len());
    let mut total_length_ge_25000bp = Vec::with_capacity(reports.len());
    let mut total_length_ge_50000bp = Vec::with_capacity(reports.len());
    let mut n50 = Vec::with_capacity(reports.len());
    let mut l50 = Vec::with_capacity(reports.len());
    let mut n90 = Vec::with_capacity(reports.len());
    let mut l90 = Vec::with_capacity(reports.len());
    let mut aun = Vec::with_capacity(reports.len());
    let mut gc_percent_values = Vec::with_capacity(reports.len());
    let mut ns = Vec::with_capacity(reports.len());
    let mut ns_per_100kbp = Vec::with_capacity(reports.len());

    for report in reports {
        let stats = &report.stats;
        let total = total_len(&stats.lengths);
        contigs_ge_0bp.push(count_at_least(&stats.all_lengths, 0) as f64);
        contigs_ge_1000bp.push(count_at_least(&stats.all_lengths, 1000) as f64);
        contigs_ge_5000bp.push(count_at_least(&stats.all_lengths, 5000) as f64);
        contigs_ge_10000bp.push(count_at_least(&stats.all_lengths, 10000) as f64);
        contigs_ge_25000bp.push(count_at_least(&stats.all_lengths, 25000) as f64);
        contigs_ge_50000bp.push(count_at_least(&stats.all_lengths, 50000) as f64);
        contigs.push(stats.lengths.len() as f64);
        largest_contig.push(stats.lengths.first().copied().unwrap_or(0) as f64);
        total_length.push(total as f64);
        total_length_ge_0bp.push(sum_at_least(&stats.all_lengths, 0) as f64);
        total_length_ge_1000bp.push(sum_at_least(&stats.all_lengths, 1000) as f64);
        total_length_ge_5000bp.push(sum_at_least(&stats.all_lengths, 5000) as f64);
        total_length_ge_10000bp.push(sum_at_least(&stats.all_lengths, 10000) as f64);
        total_length_ge_25000bp.push(sum_at_least(&stats.all_lengths, 25000) as f64);
        total_length_ge_50000bp.push(sum_at_least(&stats.all_lengths, 50000) as f64);
        n50.push(n_metric(&stats.lengths, 50).unwrap_or(0) as f64);
        l50.push(l_metric(&stats.lengths, 50).unwrap_or(0) as f64);
        n90.push(n_metric(&stats.lengths, 90).unwrap_or(0) as f64);
        l90.push(l_metric(&stats.lengths, 90).unwrap_or(0) as f64);
        aun.push(au_metric(&stats.lengths).unwrap_or(0.0));
        gc_percent_values.push(gc_percent(stats).unwrap_or(f64::NAN));
        ns.push(stats.ns as f64);
        ns_per_100kbp.push(if total == 0 {
            0.0
        } else {
            stats.ns as f64 * 100000.0 / total as f64
        });
    }

    vec![
        ("contigs_ge_0bp", contigs_ge_0bp),
        ("contigs_ge_1000bp", contigs_ge_1000bp),
        ("contigs_ge_5000bp", contigs_ge_5000bp),
        ("contigs_ge_10000bp", contigs_ge_10000bp),
        ("contigs_ge_25000bp", contigs_ge_25000bp),
        ("contigs_ge_50000bp", contigs_ge_50000bp),
        ("contigs", contigs),
        ("largest_contig", largest_contig),
        ("total_length", total_length),
        ("total_length_ge_0bp", total_length_ge_0bp),
        ("total_length_ge_1000bp", total_length_ge_1000bp),
        ("total_length_ge_5000bp", total_length_ge_5000bp),
        ("total_length_ge_10000bp", total_length_ge_10000bp),
        ("total_length_ge_25000bp", total_length_ge_25000bp),
        ("total_length_ge_50000bp", total_length_ge_50000bp),
        ("n50", n50),
        ("l50", l50),
        ("n90", n90),
        ("l90", l90),
        ("aun", aun),
        ("gc_percent", gc_percent_values),
        ("ns", ns),
        ("ns_per_100kbp", ns_per_100kbp),
    ]
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

fn total_len(lengths: &[usize]) -> usize {
    lengths.iter().sum()
}

fn count_at_least(lengths: &[usize], threshold: usize) -> usize {
    lengths.iter().filter(|&&len| len >= threshold).count()
}

fn sum_at_least(lengths: &[usize], threshold: usize) -> usize {
    lengths
        .iter()
        .copied()
        .filter(|&len| len >= threshold)
        .sum()
}

fn n_metric(lengths: &[usize], percentage: usize) -> Option<usize> {
    let target = total_len(lengths) as f64 * (100.0 - percentage as f64) / 100.0;
    let mut remaining = total_len(lengths) as f64;
    for &len in lengths {
        remaining -= len as f64;
        if remaining <= target {
            return Some(len);
        }
    }
    None
}

fn l_metric(lengths: &[usize], percentage: usize) -> Option<usize> {
    let target = total_len(lengths) as f64 * (100.0 - percentage as f64) / 100.0;
    let mut remaining = total_len(lengths) as f64;
    for (idx, &len) in lengths.iter().enumerate() {
        remaining -= len as f64;
        if remaining <= target {
            return Some(idx + 1);
        }
    }
    None
}

fn au_metric(lengths: &[usize]) -> Option<f64> {
    let total = total_len(lengths);
    if total == 0 {
        return None;
    }
    let sum_squares = lengths
        .iter()
        .map(|&length| {
            let length = length as f64;
            length * length
        })
        .sum::<f64>();
    Some(sum_squares / total as f64)
}

fn gc_percent(stats: &AssemblyStats) -> Option<f64> {
    if stats.total_acgt == 0 {
        None
    } else {
        Some(stats.total_gc as f64 * 100.0 / stats.total_acgt as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn n_and_l_metrics_match_quast_formula() {
        let lengths = vec![3980, 1610, 1120];
        assert_eq!(n_metric(&lengths, 50), Some(3980));
        assert_eq!(l_metric(&lengths, 50), Some(1));
        assert_eq!(n_metric(&lengths, 90), Some(1120));
        assert_eq!(l_metric(&lengths, 90), Some(3));
    }

    #[test]
    fn default_min_contig_keeps_short_contigs() {
        let fasta = b">a\nACGT\n>b\nAC\n";
        let stats = read_fasta_stats(fasta.as_slice(), 0).unwrap();

        assert_eq!(stats.lengths, vec![4, 2]);
        assert_eq!(total_len(&stats.lengths), 6);
    }

    #[test]
    fn gc_excludes_ns_from_denominator() {
        let stats = AssemblyStats {
            all_lengths: vec![8],
            lengths: vec![8],
            total_gc: 2,
            total_acgt: 4,
            ns: 4,
        };
        assert_eq!(format!("{:.2}", gc_percent(&stats).unwrap()), "50.00");
    }

    #[test]
    fn aun_is_sum_of_squares_over_total_length() {
        let lengths = vec![3980, 1610, 1120];
        assert_eq!(format!("{:.1}", au_metric(&lengths).unwrap()), "2934.0");
    }
}
