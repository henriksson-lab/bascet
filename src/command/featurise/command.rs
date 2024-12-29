use crate::{
    command::constants::{DEFAULT_SEED_RANDOM, RDB_PATH_INDEX_CONTIGS},
    core::constants::{HUGE_PAGE_SIZE, KMC_COUNTER_MAX_DIGITS},
    utils::KMERCodec,
};

use super::{
    constants::{
        FEATURISE_DEFAULT_FEATURES_MAX, FEATURISE_DEFAULT_FEATURES_MIN, FEATURISE_DEFAULT_PATH_IN,
        FEATURISE_DEFAULT_PATH_OUT, FEATURISE_DEFAULT_PATH_TEMP, FEATURISE_DEFAULT_THREADS_READ,
        FEATURISE_DEFAULT_THREADS_WORK,
    },
    core::{core::RDBCounter, params, state::Threading},
};
use anyhow::Result;
use clap::Args;
use itertools::Itertools;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    thread,
};
use walkdir::WalkDir;
use zip::ZipArchive;

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = FEATURISE_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = FEATURISE_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = FEATURISE_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_FEATURES_MIN)]
    pub features_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_FEATURES_MAX)]
    pub features_nmax: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_THREADS_READ)]
    pub threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = FEATURISE_DEFAULT_THREADS_WORK)]
    pub threads_work: usize,
    #[arg(long, value_parser = clap::value_parser!(u64), default_value_t = *DEFAULT_SEED_RANDOM)]
    pub seed: u64,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let file_rdb = File::open(&self.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");
        let mut file_reads_index = archive_rdb_for_index
            .by_name(&RDB_PATH_INDEX_CONTIGS)
            .expect("Could not find rdb reads index file");
        let bufreader_reads_index = BufReader::new(&mut file_reads_index);

        for line_reads_index in bufreader_reads_index.lines() {
            if let Ok(line_reads_index) = line_reads_index {
                let index = line_reads_index
                    .split(',')
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .expect("Could not parse index file");

                let mut zipfile_read = archive_rdb
                    .by_index(index)
                    .expect(&format!("No file at index {}", &index));

                let path_read = zipfile_read.mangled_name();
                match path_read.file_name().and_then(|ext| ext.to_str()) {
                    Some("kmc.kmc_pre" | "kmc.kmc_suf") => {}
                    Some(_) => continue,
                    None => panic!("None value parsing read path"),
                }

                let path_barcode = path_read.parent().unwrap();
                let path_barcode_dir = self.path_tmp.join(path_barcode);
                let _ = fs::create_dir_all(&path_barcode_dir);

                let path_temp_reads = path_barcode_dir.join(path_read.file_name().unwrap());
                let file_temp_reads = File::create(&path_temp_reads).unwrap();
                let mut bufwriter_temp_reads = BufWriter::new(&file_temp_reads);

                std::io::copy(&mut zipfile_read, &mut bufwriter_temp_reads).unwrap();
            }
        }

        let dbs: Vec<PathBuf> = WalkDir::new(&self.path_tmp)
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() || path == self.path_tmp {
                    return None;
                }
                Some(path.to_path_buf())
            })
            .collect();

        let path_kmc_union_script = self.path_tmp.join("kmc_union");
        let file_kmc_union_script = File::create(&path_kmc_union_script).unwrap();
        let mut writer_kmc_union_script = BufWriter::new(&file_kmc_union_script);

        writeln!(writer_kmc_union_script, "INPUT:")?;
        for db in &dbs {
            let barcode = db.file_stem().unwrap();
            let barcode_sanitised = barcode.to_str().unwrap().replace("-", "");
            writeln!(
                writer_kmc_union_script,
                "{} = {}",
                barcode_sanitised,
                db.join("kmc").to_str().unwrap()
            )?;
        }
        writeln!(writer_kmc_union_script, "OUTPUT:")?;

        let dbs_union_kmc = dbs
            .iter()
            .map(|db| {
                let barcode = db.file_stem().unwrap();
                let barcode_sanitised = barcode.to_str().unwrap().replace("-", "");

                barcode_sanitised
            })
            .join(" + sum ");

        writeln!(
            writer_kmc_union_script,
            "{} = {dbs_union_kmc}",
            self.path_out.to_str().unwrap()
        )?;
        let _ = writer_kmc_union_script.flush();

        let kmc_union = std::process::Command::new("kmc_tools")
            .arg("complex")
            .arg(&path_kmc_union_script)
            .arg("-t")
            .arg(format!("{}", threads_io + threads_work))
            .output()?;

        if !kmc_union.status.success() {
            anyhow::bail!(
                "KMC merge failed: {}",
                String::from_utf8_lossy(&kmc_union.stderr)
            );
        }

        let path_dump = self.path_out.with_extension("dump");
        let kmc_dump = std::process::Command::new("kmc_tools")
            .arg("transform")
            .arg(&self.path_out)
            .arg("dump")
            .arg(&path_dump)
            .output()
            .expect("KMC dump command failed");

        if !kmc_dump.status.success() {
            anyhow::bail!(
                "KMC dump failed: {}",
                String::from_utf8_lossy(&kmc_dump.stderr)
            );
        }

        let thread_buffer_size = (HUGE_PAGE_SIZE / self.threads_work)
            - ((self.kmer_size as usize) + KMC_COUNTER_MAX_DIGITS);
        let thread_states: Vec<Arc<crate::core::threading::DefaultThreadState>> = (0..self
            .threads_work)
            .map(|_| {
                Arc::new(crate::core::threading::DefaultThreadState::from_seed(
                    seed,
                    thread_buffer_size,
                    self.features_nmin,
                    self.features_nmax,
                ))
            })
            .collect();

        let thread_pool = threadpool::ThreadPool::new(self.threads_work);

        let codec = KMERCodec::new(self.kmer_size);
        let path_features = self.path_out.parent().unwrap().join("features");
        let params_io = crate::core::params::IO {
            path_in: &path_dump,
        };
        let params_runtime = crate::core::params::Runtime {
            kmer_size: kmer_size,
            features_nmin: features_nmin,
            features_nmax: features_nmax,
            codec: codec,
            seed: seed,
        };
        let params_threading = crate::core::params::Threading {
            threads_io: threads_io,
            threads_work: threads_work,
            thread_buffer_size: thread_buffer_size,
            thread_pool: &thread_pool,
            thread_states: &thread_states,
        };

        let file_features = File::create(&path_features).unwrap();
        let mut bufwriter_features = BufWriter::new(&file_features);
        if let Ok((min_features, max_features)) =
            crate::core::core::KMCProcessor::extract(params_io, params_runtime, params_threading)
        {
            for feature in min_features.iter().chain(max_features.iter()) {
                let _ = writeln!(
                    bufwriter_features,
                    "{}, {}",
                    (feature << 64) >> 64,
                    unsafe { codec.decode(*feature) }
                );
            }
        }

        Ok(())
    }

    fn verify_input_file(&mut self) -> anyhow::Result<()> {
        if let Ok(file) = File::open(&self.path_in) {
            if file.metadata()?.len() == 0 {
                anyhow::bail!("Empty input file");
            }
        }
        match self.path_in.extension().and_then(|ext| ext.to_str()) {
            Some("zip") => return Ok(()),
            _ => anyhow::bail!("Input file must be a zip archive"),
        };
    }

    fn verify_kmer_size(&self) -> Result<usize> {
        if self.kmer_size < 48 {
            return Ok(self.kmer_size);
        }

        anyhow::bail!("Invalid kmer size k:{}", self.kmer_size);
    }

    fn verify_features(&self) -> Result<(usize, usize)> {
        if self.features_nmin == 0 && self.features_nmax == 0 {
            anyhow::bail!("Ref features_nmin and features_nmax cannot be 0");
        }
        Ok((self.features_nmin, self.features_nmax))
    }
}
