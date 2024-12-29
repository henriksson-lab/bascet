use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rev_buf_reader::RevBufReader;
use zip::ZipArchive;

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
                let thread_state = &thread_states[tidx];

                while let Ok(Some(barcode)) = rx.recv() {
                    let path_temp = params_io.path_tmp.join(&barcode);
                    let path_contigs = path_temp.join("contigs.fasta");
                    let path_kmc_db = path_temp.join("kmc");
                    let path_kmc_dump = path_temp.join("dump.txt");

                    let kmc = std::process::Command::new("kmc")
                        .arg(format!("-cs{}", u32::MAX - 1))
                        .arg(format!("-k{}", &params_runtime.kmer_size))
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

                        let mut file_kmc_pre =
                            File::open(path_kmc_db.with_extension("kmc_pre")).unwrap();
                        let mut bufreader_kmc_pre = BufReader::new(&file_dump);
                        let zippath_kmc_pre = barcode.join("kmc.kmc_pre");
                        if let Ok(_) =
                            zipwriter_rdb.start_file_from_path(&zippath_kmc_pre, opts_zipwriter)
                        {
                            std::io::copy(&mut bufreader_kmc_pre, &mut *zipwriter_rdb).unwrap();
                        }

                        let mut file_kmc_suf =
                            File::open(path_kmc_db.with_extension("kmc_suf")).unwrap();
                        let mut bufreader_kmc_suf = BufReader::new(&file_dump);
                        let zippath_kmc_suf = barcode.join("kmc.kmc_suf");
                        if let Ok(_) =
                            zipwriter_rdb.start_file_from_path(&zippath_kmc_suf, opts_zipwriter)
                        {
                            std::io::copy(&mut bufreader_kmc_suf, &mut *zipwriter_rdb).unwrap();
                        }
                    }
                }
            });
        }

        for line in index_reader.lines() {
            if let Ok(line) = line {
                let index = line
                    .split(',')
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .expect("Error parsing index file");
                if let Ok(mut archive_rdb) = archive_rdb.write() {
                    let mut barcode_read = archive_rdb
                        .by_index(index)
                        .expect(&format!("No file at index {}", &index));

                    let barcode_read_path = barcode_read.mangled_name();
                    let barcode = barcode_read_path.parent().unwrap();

                    let path_dir_barcode = io_params_io.path_tmp.join(barcode);
                    let _ = fs::create_dir_all(&path_dir_barcode);

                    let path_temp_barcode_reads = path_dir_barcode.join("reads.fastq");
                    let mut file_temp_barcode_reads =
                        File::create(&path_temp_barcode_reads).unwrap();
                    std::io::copy(&mut barcode_read, &mut file_temp_barcode_reads).unwrap();

                    let _ = io_tx.send(Some(barcode.to_path_buf()));
                }
            }
        }

        for _ in 0..io_worker_threads {
            let _ = io_tx.send(None);
        }

        // Wait for the threads to complete
        thread_pool.join();
        Ok(())
    }
}
