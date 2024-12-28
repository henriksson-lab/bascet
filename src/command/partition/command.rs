use crate::{command::constants::RDB_PATH_INDEX_READS, utils::merge_archives_and_delete};

use super::{
    constants::{
        PREPARE_DEFAULT_CLEANUP, PREPARE_DEFAULT_MIN_READS_PER_CELL, PREPARE_DEFAULT_PATH_OUT,
        PREPARE_DEFAULT_PATH_TMP, PREPARE_DEFAULT_THREADS_READ, PREPARE_DEFAULT_THREADS_WRITE,
    },
    core::{core::BAMProcessor, params, state},
};
use anyhow::Result;
use clap::Args;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::PathBuf,
    sync::Arc,
};
use zip::{write::FileOptions, ZipArchive, ZipWriter};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', long, value_parser)]
    path_in: PathBuf,
    #[arg(short = 'o', value_parser, default_value = PREPARE_DEFAULT_PATH_OUT)]
    path_out: PathBuf,
    #[arg(long, value_parser, default_value = PREPARE_DEFAULT_PATH_TMP)]
    path_tmp: PathBuf,
    #[arg(long, value_parser, default_value_t = PREPARE_DEFAULT_MIN_READS_PER_CELL)]
    min_reads_per_cell: usize,
    #[arg(long, default_value_t = PREPARE_DEFAULT_CLEANUP)]
    cleanup: bool,
    #[arg(long, default_value_t = PREPARE_DEFAULT_THREADS_READ)]
    threads_read: u32,
    #[arg(long, default_value_t = PREPARE_DEFAULT_THREADS_WRITE)]
    threads_write: usize,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        let paths_threading_temp_out: Vec<PathBuf> = (0..self.threads_write)
            .map(|index| self.path_tmp.join(format!("temp-{}.rdb", index)))
            .collect();

        let thread_states: Vec<state::Threading> = paths_threading_temp_out
            .iter()
            .map(|path| {
                let file = File::create(path).unwrap();
                return state::Threading::new(file);
            })
            .collect();

        let (thread_pool_write, thread_pool_read) = (
            threadpool::ThreadPool::new(self.threads_write),
            rust_htslib::tpool::ThreadPool::new(self.threads_read)?,
        );

        let params_io = params::IO {
            path_in: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_out: self.path_out.clone(),
        };
        let params_runtime = params::Runtime {
            min_reads_per_cell: self.min_reads_per_cell,
        };
        let params_threading = params::Threading {
            threads_write: self.threads_write,
            threads_read: self.threads_read,
        };

        let _ = BAMProcessor::extract_cells(
            &Arc::new(params_io),
            &Arc::new(params_runtime),
            &Arc::new(params_threading),
            &Arc::new(thread_states),
            &thread_pool_read,
            &thread_pool_write,
        );

        /* Merge temp zip archive into a new zip archive */
        merge_archives_and_delete(&self.path_out, &paths_threading_temp_out).unwrap();
        let mut files_to_index: Vec<(usize, String)> = Vec::new();

        /* NOTE: Collect files from archive into a Vec */
        {
            let file_rdb_out = File::open(&self.path_out).unwrap();
            let mut bufreader_rdb_out = BufReader::new(&file_rdb_out);
            let archive_rdb_out = ZipArchive::new(&mut bufreader_rdb_out).unwrap();
            // Get files in the new archive
            for i in 0..archive_rdb_out.len() {
                let file_name = archive_rdb_out
                    .name_for_index(i)
                    .expect("Failed to read ZIP entry");

                files_to_index.push((i, String::from(file_name)));
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
            if let Ok(_) = &zipwriter_rdb_out.start_file(RDB_PATH_INDEX_READS, opts_zipwriter) {
                for file_to_index in files_to_index {
                    writeln!(
                        &mut zipwriter_rdb_out,
                        "{},{}",
                        file_to_index.0, file_to_index.1
                    )
                    .expect("Failed to write index entry")
                }
            }
        }

        Ok(())
    }

    // fn ensure_paths(&self) -> Result<()> {
    //     if let Ok(file) = File::open(&self.path_in) {
    //         if file.metadata()?.len() == 0 {
    //             anyhow::bail!("Empty input file");
    //         }
    //         match self.path_in.extension().and_then(|ext| ext.to_str()) {
    //             Some("cram" | "bam") => return Ok(()),
    //             _ => anyhow::bail!("Input file must be a cram or bam file"),
    //         };
    //     } else {
    //         anyhow::bail!("Input file does not exist or cannot be opened");
    //     }
    // }
}
