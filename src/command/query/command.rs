use crate::{
    command::constants::{DEFAULT_SEED_RANDOM, RDB_PATH_INDEX_KMC_DBS, RDB_PATH_INDEX_KMC_DUMPS},
    core::constants::{HUGE_PAGE_SIZE, KMC_COUNTER_MAX_DIGITS},
    utils::KMERCodec,
};
use anyhow::Result;
use clap::Args;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};
use zip::ZipArchive;

use super::constants::{
    QUERY_DEFAULT_FEATURES_MAX, QUERY_DEFAULT_FEATURES_MIN, QUERY_DEFAULT_PATH_IN,
    QUERY_DEFAULT_PATH_OUT, QUERY_DEFAULT_PATH_REF, QUERY_DEFAULT_PATH_TEMP,
    QUERY_DEFAULT_THREADS_READ, QUERY_DEFAULT_THREADS_WORK,
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = QUERY_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = QUERY_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = QUERY_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser, default_value = QUERY_DEFAULT_PATH_REF)]
    pub path_ref: PathBuf,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_MIN)]
    pub features_nmin: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_FEATURES_MAX)]
    pub features_nmax: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_THREADS_READ)]
    pub threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = QUERY_DEFAULT_THREADS_WORK)]
    pub threads_work: usize,
    #[arg(long, value_parser = clap::value_parser!(u64), default_value_t = *DEFAULT_SEED_RANDOM)]
    pub seed: u64,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        // TODO: rewrite to use a read, work and write threading pattern

        let file_rdb = File::open(&self.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let file_rdb_for_index = File::open(&self.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb_for_index);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");

        let mut file_reads_index = archive_rdb_for_index
            .by_name(RDB_PATH_INDEX_KMC_DUMPS)
            .expect("Could not find rdb reads index file");
        let bufreader_reads_index = BufReader::new(&mut file_reads_index);

        let mut queries: Vec<(usize, PathBuf)> = Vec::new();
        for line_reads_index in bufreader_reads_index.lines() {
            if let Ok(line_reads_index) = line_reads_index {
                let line_reads_split: Vec<&str> = line_reads_index.split(',').collect();
                {
                    let index_found = line_reads_split[0].parse::<usize>().expect(&format!(
                        "Could not parse index file at line: {}",
                        line_reads_index
                    ));

                    let mut zipfile_found = archive_rdb
                        .by_index(index_found)
                        .expect(&format!("No file at index {}", &index_found));

                    let zippath_found = zipfile_found.mangled_name();
                    match zippath_found.file_name().and_then(|ext| ext.to_str()) {
                        Some("dump.txt") => {}
                        Some(_) => continue,
                        None => panic!("None value parsing read path"),
                    }

                    let path_barcode = zippath_found.parent().unwrap();
                    let path_temp_dir = self.path_tmp.join(path_barcode);
                    let _ = fs::create_dir(&path_temp_dir);

                    let path_temp = path_temp_dir.join(zippath_found.file_name().unwrap());
                    let file_temp = File::create(&path_temp).unwrap();
                    let mut bufwriter_temp = BufWriter::new(&file_temp);

                    let mut bufreader_found = BufReader::new(&mut zipfile_found);
                    std::io::copy(&mut bufreader_found, &mut bufwriter_temp).unwrap();
                    queries.push((index_found, path_temp));
                }
            }
        }
        let thread_buffer_size =
            (HUGE_PAGE_SIZE / self.threads_work) - (self.kmer_size + KMC_COUNTER_MAX_DIGITS);
        let thread_states: Arc<Vec<crate::state::Threading>> = Arc::new(
            (0..self.threads_work)
                .map(|_| {
                    crate::state::Threading::from_seed(
                        self.seed,
                        thread_buffer_size,
                        self.features_nmin,
                        self.features_nmax,
                    )
                })
                .collect(),
        );
        let thread_pool = threadpool::ThreadPool::new(self.threads_work);

        let codec = KMERCodec::new(self.kmer_size);
        let params_runtime = Arc::new(crate::core::params::Runtime {
            kmer_size: self.kmer_size,
            features_nmin: self.features_nmin,
            features_nmax: self.features_nmax,
            codec: codec,
            seed: self.seed,
        });
        let params_threading = Arc::new(crate::core::params::Threading {
            threads_read: self.threads_read,
            threads_work: self.threads_work,
            threads_buffer_size: thread_buffer_size,
        });
        let mut features_reference: HashMap<u128, usize> = HashMap::new();
        let file_features_ref = File::open(&self.path_ref).unwrap();
        let bufreader_features_ref = BufReader::new(&file_features_ref);
        for (feature_index, rline) in bufreader_features_ref.lines().enumerate() {
            if let Ok(line) = rline {
                let feature = line
                    .split(',')
                    .next()
                    .unwrap()
                    .parse::<u128>()
                    .expect("Error parsing feature");
                features_reference.insert(feature, feature_index + 1);
            }
        }

        let file_feature_matrix = File::create(&self.path_out).unwrap();
        let mut bufwriter_feature_matrix = BufWriter::new(&file_feature_matrix);
        let header = "%%MatrixMarket matrix coordinate integer general";
        writeln!(bufwriter_feature_matrix, "{}", header).unwrap();
        writeln!(bufwriter_feature_matrix, "0 0 0").unwrap();

        let mut count_lines_written = 0;
        for (cell_index, path_dump) in &queries {
            let params_io = crate::params::IO {
                path_in: path_dump.clone(),
            };

            if let Ok((min_features, max_features)) = crate::KMCProcessor::extract(
                &Arc::new(params_io),
                &Arc::clone(&params_runtime),
                &Arc::clone(&params_threading),
                &Arc::clone(&thread_states),
                &thread_pool,
            ) {
                for feature in min_features.iter().chain(max_features.iter()) {
                    let kmer = (feature << 64) >> 64;
                    let count = feature >> 96;

                    if let Some(feature_index) = features_reference.get(&kmer) {
                        writeln!(
                            bufwriter_feature_matrix,
                            "\t{} {} {}",
                            cell_index, feature_index, count
                        )
                        .unwrap();
                        count_lines_written += 1;
                    }
                }
            }

            fs::remove_dir_all(path_dump.parent().unwrap()).unwrap();
        }

        let _ = bufwriter_feature_matrix.flush();
        let mut file = OpenOptions::new().write(true).open(&self.path_out).unwrap();
        file.seek(SeekFrom::Start(header.len() as u64 + 1)).unwrap(); // +1 for newline char

        writeln!(
            file,
            "{} {} {}",
            self.features_nmin + self.features_nmax - 1,
            (&queries).iter().map(|(i, _)| i).max().unwrap() - 1,
            count_lines_written - 1
        )
        .unwrap();

        Ok(())
    }
}
