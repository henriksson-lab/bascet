use crate::{
    core::constants::{HUGE_PAGE_SIZE, OVLP_DIGITS},
    utils::KMERCodec,
};

use super::{
    constants::{
        FEATURISE_DEFAULT_FEATURES_MAX, FEATURISE_DEFAULT_FEATURES_MIN, FEATURISE_DEFAULT_PATH_IN,
        FEATURISE_DEFAULT_PATH_INDEX, FEATURISE_DEFAULT_PATH_OUT, FEATURISE_DEFAULT_PATH_TEMP,
    },
    core::{core::RDBCounter, params, threading::DefaultThreadState},
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
use zip::{ZipArchive};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = FEATURISE_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 'j', value_parser, default_value = FEATURISE_DEFAULT_PATH_INDEX)]
    pub path_index: PathBuf,
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
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_io: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads_work: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(u64))]
    pub seed: Option<u64>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        self.verify_input_file()?;
        let kmer_size = self.verify_kmer_size()?;
        let (threads_io, threads_work) = self.resolve_thread_config()?;
        let (features_nmin, features_nmax) = self.verify_features()?;
        let seed = self.seed.unwrap_or_else(rand::random);

        let thread_buffer_size =
            (HUGE_PAGE_SIZE / threads_work) - ((self.kmer_size as usize) + OVLP_DIGITS);
        let thread_pool = threadpool::ThreadPool::new(threads_io + threads_work);
        let thread_states: Vec<Arc<crate::core::threading::DefaultThreadState>> = (0..threads_work)
            .map(|_| {
                Arc::new(crate::core::threading::DefaultThreadState::from_seed(
                    seed,
                    thread_buffer_size,
                    features_nmin,
                    features_nmax,
                ))
            })
            .collect();

        let rdb_file = File::open(&self.path_in).expect("Failed to open RDB file");
        let index_file = File::open(&self.path_index).expect("Failed to open index file");
        let index_reader = BufReader::new(index_file);
        let mut archive_rdb = ZipArchive::new(rdb_file).expect("Unable to create zip archive");

        for line in index_reader.lines() {
            if let Ok(line) = line {
                let index = line
                    .split(',')
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .expect("Error parsing index file");

                let mut barcode_kmc = archive_rdb
                    .by_index(index)
                    .expect(&format!("No file at index {}", &index));

                let barcode_path = barcode_kmc.mangled_name();
                let barcode_kmc_ext = barcode_path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap();
                match barcode_kmc_ext {
                    "kmc_pre" | "kmc_suf" => {}
                    _ => continue,
                }

                let barcode = barcode_path.parent().unwrap();

                let path_dir_barcode = self.path_tmp.join(barcode);
                let _ = fs::create_dir_all(&path_dir_barcode);

                let path_temp_barcode_kmc = path_dir_barcode.join(format!("kmc.{barcode_kmc_ext}"));
                let mut file_temp_barcode_kmc = File::create(&path_temp_barcode_kmc).unwrap();
                std::io::copy(&mut barcode_kmc, &mut file_temp_barcode_kmc).unwrap();
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
            .arg("-t").arg(format!("{}", threads_io + threads_work))
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

        let codec = KMERCodec::new(kmer_size);
        let path_features = self.path_out.parent().unwrap().join("features");
        let params_io = crate::core::params::IO {
            path_in: &path_dump,
            path_out: &path_features,
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
            crate::core::core::KMCProcessor::extract(params_io, params_runtime, params_threading) {
            
            for feature in min_features.iter().chain(max_features.iter()) {
                let _ = writeln!(bufwriter_features, "{}", unsafe { codec.decode(*feature) });
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

    fn resolve_thread_config(&self) -> Result<(usize, usize)> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if available_threads < 2 {
            anyhow::bail!("At least two threads must be available");
        }

        let (threads_io, threads_work) = match (self.threads_io, self.threads_work) {
            (Some(i), Some(w)) => (i, w),
            (Some(i), None) => (i, available_threads.saturating_sub(i).max(1)),
            (None, Some(w)) => (1, w),
            (None, None) => (1, available_threads.saturating_sub(1).max(1)),
        };

        if threads_io == 0 {
            anyhow::bail!("At least one IO thread required");
        }
        if threads_work == 0 {
            anyhow::bail!("At least one work thread required");
        }

        Ok((threads_io, threads_work))
    }
}
