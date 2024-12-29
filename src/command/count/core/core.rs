use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rev_buf_reader::RevBufReader;
use zip::ZipArchive;

use crate::command::constants::RDB_PATH_INDEX_CONTIGS;

use super::{params, state};

pub struct RDBCounter {}

impl RDBCounter {
    pub fn extract<'a>(
        params_io: &Arc<params::IO>,
        params_runtime: &Arc<params::Runtime>,
        params_threading: &Arc<params::Threading>,

        thread_states: &Arc<Vec<state::Threading>>,
        thread_pool: &threadpool::ThreadPool,
    ) -> anyhow::Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for tidx in 0..params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let params_io = Arc::clone(&params_io);
            let params_runtime = Arc::clone(&params_runtime);
            let params_threading = Arc::clone(&params_threading);
            let thread_states = Arc::clone(&thread_states);

            thread_pool.execute(move || {
                println!("Worker {tidx} started");
                let thread_state = &thread_states[tidx];

                while let Ok(Some(barcode)) = rx.recv() {
                    let path_temp = params_io.path_tmp.join(&barcode);
                    let path_contigs = path_temp.join("contigs.fasta");
                    let path_kmc_db = path_temp.join("kmc");
                    let path_kmc_dump = path_temp.join("dump.txt");

                    let kmc = std::process::Command::new("kmc")
                        .arg(format!("-cs{}", u32::MAX - 1))
                        .arg(format!("-k{}", &params_runtime.kmer_size))
                        .arg("-ci=1")
                        .arg("-fa")
                        .arg(&path_contigs)
                        .arg(&path_kmc_db)
                        .arg(&path_temp)
                        .output()
                        .map_err(|e| eprintln!("Failed to execute KMC command: {}", e))
                        .expect("KMC command failed");

                    if !kmc.status.success() {
                        eprintln!("KMC command failed with status: {}", kmc.status);
                        std::io::stderr()
                            .write_all(&kmc.stderr)
                            .expect("Failed to write to stderr");
                    }

                    let kmc_dump = std::process::Command::new("kmc_tools")
                        .arg("transform")
                        .arg(&path_kmc_db)
                        .arg("dump")
                        .arg(&path_kmc_dump)
                        .output()
                        .map_err(|e| eprintln!("Failed to execute KMC dump command: {}", e))
                        .expect("KMC dump command failed");

                    if !kmc_dump.status.success() {
                        eprintln!("KMC dump command failed with status: {}", kmc_dump.status);
                        std::io::stderr()
                            .write_all(&kmc_dump.stderr)
                            .expect("Failed to write to stderr");
                    }

                    {
                        let mut zipwriter_rdb = thread_state.zip_writer.lock().unwrap();
                        let opts_zipwriter: zip::write::FileOptions<()> =
                            zip::write::FileOptions::default()
                                .compression_method(zip::CompressionMethod::Stored);

                        let file_dump = File::open(&path_kmc_dump).unwrap();
                        let mut bufreader_dump = BufReader::new(&file_dump);
                        let zippath_dump = barcode.join("dump.txt");
                        if let Ok(_) =
                            zipwriter_rdb.start_file_from_path(&zippath_dump, opts_zipwriter)
                        {
                            std::io::copy(&mut bufreader_dump, &mut *zipwriter_rdb).unwrap();
                        }

                        let file_kmc_pre =
                            File::open(path_kmc_db.with_extension("kmc_pre")).unwrap();
                        let mut bufreader_kmc_pre = BufReader::new(&file_kmc_pre);
                        let zippath_kmc_pre = barcode.join("kmc.kmc_pre");
                        if let Ok(_) =
                            zipwriter_rdb.start_file_from_path(&zippath_kmc_pre, opts_zipwriter)
                        {
                            std::io::copy(&mut bufreader_kmc_pre, &mut *zipwriter_rdb).unwrap();
                        }

                        let file_kmc_suf =
                            File::open(path_kmc_db.with_extension("kmc_suf")).unwrap();
                        let mut bufreader_kmc_suf = BufReader::new(&file_kmc_suf);
                        let zippath_kmc_suf = barcode.join("kmc.kmc_suf");
                        if let Ok(_) =
                            zipwriter_rdb.start_file_from_path(&zippath_kmc_suf, opts_zipwriter)
                        {
                            std::io::copy(&mut bufreader_kmc_suf, &mut *zipwriter_rdb).unwrap();
                        }
                    }

                    fs::remove_dir_all(&path_temp).unwrap();
                    println!("Finished {barcode:?}");
                }
            });
        }

        let file_rdb = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let file_rdb_for_index = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb_for_index);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");

        let mut file_reads_index = archive_rdb_for_index
            .by_name(RDB_PATH_INDEX_CONTIGS)
            .expect("Could not find rdb reads index file");
        let bufreader_reads_index = BufReader::new(&mut file_reads_index);
        for line_reads_index in bufreader_reads_index.lines() {
            if let Ok(line_reads_index) = line_reads_index {
                let line_reads_split: Vec<&str> = line_reads_index.split(",").collect();
                let index_found = line_reads_split[1].parse::<usize>().expect(&format!(
                    "Could not parse index file at line: {}",
                    line_reads_index
                ));

                let mut zipfile_found = archive_rdb
                    .by_index(index_found)
                    .expect(&format!("No file at index {}", &index_found));

                let zippath_found = zipfile_found.mangled_name();
                match zippath_found.file_name().and_then(|ext| ext.to_str()) {
                    Some("contigs.fasta") => {}
                    Some(_) => continue,
                    None => panic!("None value parsing read path"),
                }

                let path_barcode = zippath_found.parent().unwrap();
                let path_temp_dir = params_io.path_tmp.join(path_barcode);
                let _ = fs::create_dir(&path_temp_dir);

                let path_temp = path_temp_dir.join(zippath_found.file_name().unwrap());
                let file_temp = File::create(&path_temp).unwrap();
                let mut bufwriter_temp = BufWriter::new(&file_temp);

                let mut bufreader_found = BufReader::new(&mut zipfile_found);
                std::io::copy(&mut bufreader_found, &mut bufwriter_temp).unwrap();
                tx.send(Some(path_barcode.to_path_buf())).unwrap();
            }
        }

        for _ in 0..params_threading.threads_work {
            let _ = tx.send(None);
        }

        // Wait for the threads to complete
        thread_pool.join();
        Ok(())
    }
}
