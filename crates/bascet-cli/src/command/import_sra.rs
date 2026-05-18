use anyhow::{Context, bail};
use clap::Args;
use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use tracing::info;

use crate::fileformat::shard::ReadPair;
use crate::fileformat::tirp::{BascetTIRPWriterFactory, get_tbi_path_for_tirp};
use crate::fileformat::{ConstructFromPath, ReadPairWriter};
use crate::utils::{atomic_temp_path_in_dir, publish_atomic_output};

#[derive(Args)]
pub struct ImportSraCMD {
    #[arg(long = "sralist", help = "File with one SRA run accession per line")]
    pub sralist: PathBuf,

    #[arg(
        long = "runinfo",
        help = "Optional RunInfo CSV retained for workflow compatibility; cells are currently named by Run"
    )]
    pub runinfo: Option<PathBuf>,

    #[arg(short = 'o', long = "out", help = "Output TIRP file")]
    pub out: PathBuf,

    #[arg(long = "temp", help = "Temporary directory for SRA and FASTQ files")]
    pub temp: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        default_value_t = 4,
        help = "Threads passed to fasterq-dump"
    )]
    pub threads: usize,

    #[arg(
        long = "prefetch",
        default_value = "prefetch",
        help = "prefetch executable"
    )]
    pub prefetch: PathBuf,

    #[arg(
        long = "fasterq-dump",
        default_value = "fasterq-dump",
        help = "fasterq-dump executable"
    )]
    pub fasterq_dump: PathBuf,

    #[arg(
        long = "max-runs",
        help = "Only import the first N runs from the SRA list"
    )]
    pub max_runs: Option<usize>,

    #[arg(
        long = "runs-ahead",
        default_value_t = 10,
        help = "Maximum number of completed fasterq-dump runs buffered ahead of TIRP writing"
    )]
    pub runs_ahead: usize,

    #[arg(
        long = "sra-workers",
        help = "Number of concurrent SRA Toolkit workers; defaults to --threads"
    )]
    pub sra_workers: Option<usize>,

    #[arg(long = "keep-temp", help = "Keep per-run FASTQ files after packing")]
    pub keep_temp: bool,
}

impl ImportSraCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        if let Some(runinfo) = &self.runinfo {
            if !runinfo.exists() {
                bail!("RunInfo file does not exist: {}", runinfo.display());
            }
        }

        let mut runs = read_sralist(&self.sralist)?;
        if let Some(max_runs) = self.max_runs {
            runs.truncate(max_runs);
        }
        if runs.is_empty() {
            bail!("SRA list contains no runs: {}", self.sralist.display());
        }
        if self.runs_ahead == 0 {
            bail!("--runs-ahead must be at least 1");
        }
        if self.threads == 0 {
            bail!("--threads must be at least 1");
        }
        let sra_workers = self.sra_workers.unwrap_or(self.threads);
        if sra_workers == 0 {
            bail!("--sra-workers must be at least 1");
        }

        fs::create_dir_all(&self.temp)
            .with_context(|| format!("failed to create temp directory {}", self.temp.display()))?;
        let sra_dir = self.temp.join("sra");
        let fastq_dir = self.temp.join("fastq");
        fs::create_dir_all(&sra_dir)?;
        fs::create_dir_all(&fastq_dir)?;

        info!(
            runs = runs.len(),
            output = %self.out.display(),
            temp = %self.temp.display(),
            runs_ahead = self.runs_ahead,
            sra_workers = sra_workers,
            "Importing SRA runs"
        );

        let out_tmp = atomic_temp_path_in_dir(&self.out, &self.temp);
        let out_tbi = get_tbi_path_for_tirp(&self.out);
        let out_tbi_tmp = get_tbi_path_for_tirp(&out_tmp);

        let factory = BascetTIRPWriterFactory::new();
        let mut writer = factory.new_from_path(&out_tmp)?;

        let (tx, rx) = mpsc::sync_channel(self.runs_ahead);
        let work = Arc::new(Mutex::new(
            runs.iter()
                .cloned()
                .enumerate()
                .collect::<Vec<(usize, String)>>()
                .into_iter(),
        ));
        let mut workers = Vec::new();
        for worker_id in 0..sra_workers {
            let tx = tx.clone();
            let work = Arc::clone(&work);
            let worker_prefetch = self.prefetch.clone();
            let worker_fasterq_dump = self.fasterq_dump.clone();
            let worker_sra_dir = sra_dir.clone();
            let worker_fastq_dir = fastq_dir.clone();
            let worker_threads = self.threads;
            workers.push(
                std::thread::Builder::new()
                    .name(format!("import-sra-toolkit-{worker_id}"))
                    .spawn(move || {
                        loop {
                            let Some((index, run)) =
                                work.lock().expect("SRA work queue mutex poisoned").next()
                            else {
                                break;
                            };

                            let result = (|| -> anyhow::Result<(usize, String)> {
                                info!(run = %run, worker = worker_id, "Downloading SRA run");
                                run_prefetch(&worker_prefetch, &run, &worker_sra_dir)?;

                                info!(
                                    run = %run,
                                    worker = worker_id,
                                    threads = worker_threads,
                                    "Converting SRA run to FASTQ"
                                );
                                let fasterq_temp_dir = worker_fastq_dir.join(format!("{run}.tmp"));
                                fs::create_dir_all(&fasterq_temp_dir).with_context(|| {
                                    format!(
                                        "failed to create fasterq-dump temp directory {}",
                                        fasterq_temp_dir.display()
                                    )
                                })?;
                                run_fasterq_dump(
                                    &worker_fasterq_dump,
                                    &run,
                                    &worker_sra_dir,
                                    &worker_fastq_dir,
                                    &fasterq_temp_dir,
                                    worker_threads,
                                )?;
                                Ok((index, run))
                            })();

                            let failed = result.is_err();
                            if tx.send(result).is_err() || failed {
                                break;
                            }
                        }
                    })
                    .context("failed to spawn import-sra SRA Toolkit worker")?,
            );
        }
        drop(tx);

        let mut pending = BTreeMap::new();
        let mut next_to_write = 0usize;
        let mut num_received = 0usize;
        while num_received < runs.len() {
            let (index, run) = rx
                .recv()
                .context("SRA Toolkit workers stopped before all runs were imported")??;
            num_received += 1;
            pending.insert(index, run);

            while let Some(run) = pending.remove(&next_to_write) {
                info!(run = %run, "Packing FASTQ into TIRP");
                let reads = read_run_fastq(&run, &fastq_dir)
                    .with_context(|| format!("failed to read FASTQ output for {run}"))?;
                writer.write_reads_for_cell(&run, &Arc::new(reads));

                if !self.keep_temp {
                    remove_run_fastq(&run, &fastq_dir)?;
                    remove_prefetched_run(&run, &sra_dir)?;
                }
                next_to_write += 1;
            }
        }

        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("SRA Toolkit worker panicked"))?;
        }

        if next_to_write != runs.len() {
            bail!(
                "not all runs were written to TIRP; wrote {} of {}",
                next_to_write,
                runs.len()
            );
        }

        writer.writing_done()?;
        publish_atomic_output(&out_tmp, &self.out)
            .with_context(|| format!("failed to publish {}", self.out.display()))?;
        publish_atomic_output(&out_tbi_tmp, &out_tbi)
            .with_context(|| format!("failed to publish {}", out_tbi.display()))?;
        info!("Finished SRA import");
        Ok(())
    }
}

fn read_sralist(path: &Path) -> anyhow::Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut runs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if !line.is_empty() && !line.starts_with('#') {
            runs.push(line.to_string());
        }
    }
    Ok(runs)
}

fn run_prefetch(prefetch: &Path, run: &str, sra_dir: &Path) -> anyhow::Result<()> {
    run_command(
        Command::new(prefetch)
            .arg(run)
            .arg("--output-directory")
            .arg(sra_dir),
        "prefetch",
    )
}

fn run_fasterq_dump(
    fasterq_dump: &Path,
    run: &str,
    sra_dir: &Path,
    fastq_dir: &Path,
    fasterq_temp_dir: &Path,
    threads: usize,
) -> anyhow::Result<()> {
    let local_sra = sra_dir.join(run).join(format!("{run}.sra"));
    let input = if local_sra.exists() {
        local_sra
    } else {
        sra_dir.join(run)
    };
    let input = if input.exists() {
        input
    } else {
        PathBuf::from(run)
    };

    run_command(
        Command::new(fasterq_dump)
            .arg("--split-files")
            .arg("--threads")
            .arg(threads.to_string())
            .arg("--outdir")
            .arg(fastq_dir)
            .arg("--temp")
            .arg(fasterq_temp_dir)
            .arg(input),
        "fasterq-dump",
    )
}

fn run_command(cmd: &mut Command, name: &str) -> anyhow::Result<()> {
    let status = cmd
        .stdin(Stdio::null())
        .status()
        .with_context(|| format!("failed to start {name}"))?;
    if !status.success() {
        bail!("{name} failed with status {status}");
    }
    Ok(())
}

fn read_run_fastq(run: &str, fastq_dir: &Path) -> anyhow::Result<Vec<ReadPair>> {
    let r1 = fastq_dir.join(format!("{run}_1.fastq"));
    let r2 = fastq_dir.join(format!("{run}_2.fastq"));
    let single = fastq_dir.join(format!("{run}.fastq"));

    if r1.exists() && r2.exists() {
        read_paired_fastq(&r1, &r2)
    } else if single.exists() {
        read_single_fastq(&single)
    } else if r1.exists() {
        read_single_fastq(&r1)
    } else {
        bail!(
            "no FASTQ output found for {run}; expected {}, {}, or {}",
            r1.display(),
            r2.display(),
            single.display()
        );
    }
}

fn read_paired_fastq(r1: &Path, r2: &Path) -> anyhow::Result<Vec<ReadPair>> {
    let mut reader1 = FastqReader::new(File::open(r1)?);
    let mut reader2 = FastqReader::new(File::open(r2)?);
    let mut reads = Vec::new();

    loop {
        match (reader1.next(), reader2.next()) {
            (Some(rec1), Some(rec2)) => {
                let rec1 = rec1?;
                let rec2 = rec2?;
                reads.push(ReadPair {
                    r1: rec1.seq().to_vec(),
                    r2: rec2.seq().to_vec(),
                    q1: rec1.qual().to_vec(),
                    q2: rec2.qual().to_vec(),
                    umi: Vec::new(),
                });
            }
            (None, None) => break,
            _ => bail!("paired FASTQ files have different numbers of records"),
        }
    }

    Ok(reads)
}

fn read_single_fastq(path: &Path) -> anyhow::Result<Vec<ReadPair>> {
    let mut reader = FastqReader::new(File::open(path)?);
    let mut reads = Vec::new();
    while let Some(rec) = reader.next() {
        let rec = rec?;
        reads.push(ReadPair {
            r1: rec.seq().to_vec(),
            r2: Vec::new(),
            q1: rec.qual().to_vec(),
            q2: Vec::new(),
            umi: Vec::new(),
        });
    }
    Ok(reads)
}

fn remove_run_fastq(run: &str, fastq_dir: &Path) -> anyhow::Result<()> {
    for suffix in [".fastq", "_1.fastq", "_2.fastq", "_3.fastq"] {
        let path = fastq_dir.join(format!("{run}{suffix}"));
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    let temp_path = fastq_dir.join(format!("{run}.tmp"));
    if temp_path.exists() {
        fs::remove_dir_all(&temp_path)
            .with_context(|| format!("failed to remove {}", temp_path.display()))?;
    }
    Ok(())
}

fn remove_prefetched_run(run: &str, sra_dir: &Path) -> anyhow::Result<()> {
    let path = sra_dir.join(run);
    if path.exists() {
        fs::remove_dir_all(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}
