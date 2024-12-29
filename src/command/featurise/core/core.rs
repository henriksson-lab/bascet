use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use rev_buf_reader::RevBufReader;
use zip::ZipArchive;

use crate::command::constants::RDB_PATH_INDEX_CONTIGS;

use super::{params, state::DefaultThreadState};

pub struct RDBCounter {}

impl RDBCounter {
    pub fn extract<'a>(
        params_io: Arc<params::IO>,
        params_runtime: Arc<params::Runtime>,
        params_threading: Arc<params::Threading<'a>>,
        thread_states: Vec<Arc<DefaultThreadState>>,
        thread_pool: &threadpool::ThreadPool,
    ) -> anyhow::Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for tidx in 0..params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let params_io = Arc::clone(&params_io);
            let params_runtime = Arc::clone(&params_runtime);
            let params_threading = Arc::clone(&params_threading);

            let thread_state = Arc::clone(&thread_states[tidx]);
            let mut zip_writer = unsafe { &mut *thread_state.zip_writer.get() };
            let thread_temp_path = Arc::clone(&thread_state.temp_path);

            params_threading.thread_pool.execute(move || {
                while let Ok(Some(barcode)) = rx.recv() {
                    let path_dir = params_io.path_tmp.join(&barcode);
                    let path_temp_reads = path_dir.join("reads").with_extension("fastq");
                    let kmc_path_db = path_dir.join("kmc");
                    let kmc_path_dump = path_dir.join("dump.txt");

                    println!("Featurising path {path_dir:?}");
                    let kmc = std::process::Command::new("kmc")
                        .arg(format!("-cs{}", u32::MAX - 1))
                        .arg(format!("-k{}", &params_runtime.kmer_size))
                        .arg(&path_temp_reads)
                        .arg(&kmc_path_db)
                        .arg(&*thread_temp_path)
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
                        .arg(&kmc_path_db)
                        .arg("dump")
                        .arg(&kmc_path_dump)
                        .output()
                        .map_err(|e| eprintln!("Failed to execute KMC dump command: {}", e))
                        .expect("KMC dump command failed");

                    if !kmc_dump.status.success() {
                        eprintln!("KMC dump command failed with status: {}", kmc_dump.status);
                        std::io::stderr()
                            .write_all(&kmc_dump.stderr)
                            .expect("Failed to write to stderr");
                    }

                    let opts: zip::write::FileOptions<()> = zip::write::FileOptions::default()
                        .compression_method(zip::CompressionMethod::Stored);

                    let mut dump_file = File::open(&kmc_path_dump).unwrap();
                    let zip_path = barcode.join("dump.txt");
                    if let Ok(_) = zip_writer.start_file_from_path(&zip_path, opts) {
                        std::io::copy(&mut dump_file, &mut zip_writer).unwrap();
                    }

                    let mut pre_file = File::open(path_dir.join("kmc.kmc_pre")).unwrap();
                    let zip_path = barcode.join("kmc.kmc_pre");
                    if let Ok(_) = zip_writer.start_file_from_path(&zip_path, opts) {
                        std::io::copy(&mut pre_file, &mut zip_writer).unwrap();
                    }

                    let mut suf_file = File::open(path_dir.join("kmc.kmc_suf")).unwrap();
                    let zip_path = barcode.join("kmc.kmc_suf");
                    if let Ok(_) = zip_writer.start_file_from_path(&zip_path, opts) {
                        std::io::copy(&mut suf_file, &mut zip_writer).unwrap();
                    }

                    let _ = fs::remove_dir_all(&path_dir);
                }
            });
        }

        let file_rdb = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");
        let mut file_reads_index = archive_rdb_for_index
            .by_name(RDB_PATH_INDEX_CONTIGS)
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
                    Some("contigs.fastq") => {}
                    Some(_) => continue,
                    None => panic!("None value parsing read path"),
                }

                let path_barcode = path_read.parent().unwrap();
                let path_barcode_dir = params_io.path_tmp.join(path_barcode);
                let _ = fs::create_dir(&path_barcode_dir);

                let path_temp_reads = path_barcode_dir.join("contigs.fastq");
                let file_temp_reads = File::create(&path_temp_reads).unwrap();
                let mut bufwriter_temp_reads = BufWriter::new(&file_temp_reads);

                std::io::copy(&mut zipfile_read, &mut bufwriter_temp_reads).unwrap();

                tx.send(Some(path_barcode.to_path_buf())).unwrap();
            }
        }

        for _ in 0..params_threading.threads_write {
            let _ = tx.send(None);
        }
        // Wait for the threads to complete
        thread_pool.join();
        Ok(())
    }
}
