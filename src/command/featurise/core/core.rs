use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rev_buf_reader::RevBufReader;
use zip::ZipArchive;

use super::{params, threading::DefaultThreadState};

pub struct RDBCounter {}

impl RDBCounter {
    pub fn extract<'a>(
        params_io: Arc<params::IO>,
        params_runtime: Arc<params::Runtime>,
        params_threading: Arc<params::Threading<'a>>,
        thread_states: Vec<Arc<DefaultThreadState>>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for state in &thread_states {
            let union_dir = &state.temp_path;
            // create an empty fastq to create an empty kmc database as a merge target
            let path_empty_reads = Arc::new(union_dir.join("reads").with_extension("fastq"));
            let _ = File::create(&*path_empty_reads);

            let path_kmc_union = Arc::new(union_dir.join("kmc"));
            let _ = std::process::Command::new("kmc")
                .arg(format!("-cs{}", u32::MAX))
                .arg(format!("-k{}", &params_runtime.kmer_size))
                .arg(&*path_empty_reads)
                .arg(&*path_kmc_union)
                .arg(&params_io.path_tmp)
                .arg("-t")
                .arg("1")
                .output()?;
        }

        for tidx in 0..params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let params_io = Arc::clone(&params_io);
            let params_runtime = Arc::clone(&params_runtime);
            let params_threading = Arc::clone(&params_threading);

            let thread_state = Arc::clone(&thread_states[tidx]);
            let mut zip_writer = unsafe { &mut *thread_state.zip_writer.get() };
            let thread_temp_path = Arc::clone(&thread_state.temp_path);
            let union_kmc = thread_temp_path.join("kmc");
            let union_kmc_write = thread_temp_path.join("kmc_write");

            params_threading.thread_pool.execute(move || {
                while let Ok(Some(barcode)) = rx.recv() {
                    let path_dir = params_io.path_tmp.join(&barcode);
                    let path_temp_reads = path_dir.join("reads").with_extension("fastq");
                    let kmc_path_db = path_dir.join("kmc");
                    let kmc_path_dump = path_dir.join("dump.txt");
                    println!("Evaluating path {path_dir:?}");
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

                    // let kmc_union = std::process::Command::new("kmc_tools")
                    //     .arg("simple")
                    //     .arg(&*union_kmc)
                    //     .arg(&kmc_path_db)
                    //     .arg("union")
                    //     .arg(&*union_kmc_write)
                    //     .output()
                    //     .map_err(|e| eprintln!("Failed to execute KMC union command: {}", e))
                    //     .expect("KMC union command failed");

                    // if !kmc_union.status.success() {
                    //     eprintln!("KMC union command failed with status: {}", kmc_union.status);
                    //     std::io::stderr().write_all(&kmc_union.stderr).expect("Failed to write to stderr");
                    // }
                    let opts: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default()
                        .compression_method(zip::CompressionMethod::Stored);
                    let mut dump_file = File::open(&kmc_path_dump).unwrap();

                    // this is so stupid
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
                    // let _ = fs::rename(
                    //     &union_kmc_write.with_extension("kmc_pre"),
                    //     &union_kmc.with_extension("kmc_pre"),
                    // );
                    // let _ = fs::rename(
                    //     &union_kmc_write.with_extension("kmc_suf"),
                    //     &union_kmc.with_extension("kmc_suf"),
                    // );
                    let _ = fs::remove_dir_all(&path_dir);
                }
            });
        }

        let index_file = File::open(&params_io.path_idx)?;
        let mut index_reader = BufReader::new(index_file);
        let mut progress_index_rev_reader = RevBufReader::new(&mut index_reader);
        let mut progress_index_last_line = String::new();
        progress_index_rev_reader.read_line(&mut progress_index_last_line)?;
        let _progress_index_last = progress_index_last_line
            .split(",")
            .next()
            .unwrap()
            .parse::<usize>()?;

        let io_tx = Arc::clone(&tx);
        let io_params_io = Arc::clone(&params_io);
        let io_worker_threads = params_threading.threads_write;

        let rdb_file = File::open(&io_params_io.path_in).expect("Failed to open RDB file");
        let index_file = File::open(&io_params_io.path_idx).expect("Failed to open index file");
        let index_reader = BufReader::new(index_file);
        let archive_rdb = Arc::new(RwLock::new(
            ZipArchive::new(rdb_file).expect("Unable to create zip archive"),
        ));

        params_threading.thread_pool.execute(move || {
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
        });

        // Wait for the threads to complete
        params_threading.thread_pool.join();
        Ok(())
    }
}
