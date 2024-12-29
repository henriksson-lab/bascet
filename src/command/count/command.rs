use anyhow::Result;
use clap::Args;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::PathBuf,
    sync::Arc,
};
use zip::{write::FileOptions, ZipArchive, ZipWriter};

use crate::{
    command::constants::{RDB_PATH_INDEX_KMC_DBS, RDB_PATH_INDEX_KMC_DUMPS},
    utils::merge_archives_and_delete,
};

use super::{
    constants::{
        COUNT_DEFAULT_PATH_IN, COUNT_DEFAULT_PATH_OUT, COUNT_DEFAULT_PATH_TEMP,
        COUNT_DEFAULT_THREADS_READ, COUNT_DEFAULT_THREADS_WORK,
    },
    core::{core::RDBCounter, params, state},
};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = COUNT_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 't', value_parser, default_value = COUNT_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'o', value_parser, default_value = COUNT_DEFAULT_PATH_OUT)]
    pub path_out: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = COUNT_DEFAULT_THREADS_READ)]
    pub threads_read: usize,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = COUNT_DEFAULT_THREADS_WORK)]
    pub threads_work: usize,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let paths_threading_temp_out: Vec<PathBuf> = (0..self.threads_work)
            .map(|index| self.path_tmp.join(format!("temp-{}.rdb", index)))
            .collect();

        let thread_states: Vec<state::Threading> = paths_threading_temp_out
            .iter()
            .map(|path| {
                let file = File::create(path).unwrap();
                return state::Threading::new(file);
            })
            .collect();

        let thread_pool = threadpool::ThreadPool::new(self.threads_read + self.threads_work);
        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            kmer_size: self.kmer_size,
        };
        let params_threading = params::Threading {
            threads_read: self.threads_read,
            threads_work: self.threads_work,
        };

        let _ = RDBCounter::extract(
            &Arc::new(params_io),
            &Arc::new(params_runtime),
            &Arc::new(params_threading),
            &Arc::new(thread_states),
            &thread_pool,
        );

        /* Merge temp zip archive into a new zip archive */
        merge_archives_and_delete(&self.path_out, &paths_threading_temp_out).unwrap();
        let mut files_kmc_dump_to_index: Vec<(usize, String)> = Vec::new();
        let mut files_kmc_dbs_to_index: Vec<(usize, usize, String)> = Vec::new();

        /* NOTE: Collect files from archive into a Vec */
        {
            let file_rdb_out = File::open(&self.path_out).unwrap();
            let mut bufreader_rdb_out = BufReader::new(&file_rdb_out);
            let archive_rdb_out = ZipArchive::new(&mut bufreader_rdb_out).unwrap();
            // Get files in the new archive
            for i in 0..archive_rdb_out.len() {
                let filename = archive_rdb_out
                    .name_for_index(i)
                    .expect("Failed to read ZIP entry");

                if filename.ends_with("dump.txt") {
                    files_kmc_dump_to_index.push((i, String::from(filename)));
                }
                // HACK: kmc stores their dbs as two files, I will, for simlicity, ignore the second file
                else if filename.ends_with("kmc_pre") {
                    let kmc_db = String::from(filename).replace(".kmc_pre", "");

                    // HACK: .kmc_suf file is one file index away from the .kmc_pre file
                    files_kmc_dbs_to_index.push((i, i + 1, kmc_db));
                }
            }
        }

        /* NOTE: Write to index file */
        {
            let file_rdb_out = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path_out)
                .unwrap();

            let mut zipwriter_rdb_out = ZipWriter::new_append(&file_rdb_out).unwrap();
            let opts_zipwriter: FileOptions<()> =
                FileOptions::default().compression_method(zip::CompressionMethod::Stored);
            if let Ok(_) = &zipwriter_rdb_out.start_file(RDB_PATH_INDEX_KMC_DUMPS, opts_zipwriter) {
                for file_to_index in files_kmc_dump_to_index {
                    writeln!(
                        &mut zipwriter_rdb_out,
                        "{},{}",
                        file_to_index.0, file_to_index.1
                    )
                    .expect("Failed to write index entry")
                }
            }
        }
        /* NOTE: Write to index file */
        {
            let file_rdb_out = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path_out)
                .unwrap();

            let mut zipwriter_rdb_out = ZipWriter::new_append(&file_rdb_out).unwrap();
            let opts_zipwriter: FileOptions<()> =
                FileOptions::default().compression_method(zip::CompressionMethod::Stored);
            if let Ok(_) = &zipwriter_rdb_out.start_file(RDB_PATH_INDEX_KMC_DBS, opts_zipwriter) {
                for file_to_index in files_kmc_dbs_to_index {
                    writeln!(
                        &mut zipwriter_rdb_out,
                        "{},{},{}",
                        file_to_index.0, file_to_index.1, file_to_index.2
                    )
                    .expect("Failed to write index entry")
                }
            }
        }
        Ok(())
    }
}
