use std::{
    fmt::format,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rev_buf_reader::RevBufReader;
use zip::{ZipArchive, ZipWriter};

use super::{params, threading::DefaultThreadState};

pub struct RDBAssembler {}

impl RDBAssembler {
    pub fn assemble<'a>(
        params_io: Arc<params::IO>,
        params_runtime: Arc<params::Runtime>,
        params_threading: Arc<params::Threading>,
        thread_pool: &threadpool::ThreadPool,
    ) -> anyhow::Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        let work_params_io = Arc::clone(&params_io);
        let work_params_runtime = Arc::clone(&params_runtime);
        let work_params_threading = Arc::clone(&params_threading);

        let file_archive = File::open(&work_params_io.path_out).unwrap();
        let bufwriter_archive = BufWriter::new(file_archive);
        let mut zip_writer = ZipWriter::new(bufwriter_archive);

        thread_pool.execute(move || {
            while let Ok(Some(barcode)) = rx.recv() {
                let path_dir = work_params_io.path_tmp.join(&barcode);
                let path_reads = path_dir.join("reads").with_extension("fastq");
                let path_spades = path_dir.join("spades");
                let path_contigs = path_spades.join("contigs.fasta");
                println!("Evaluating path {path_dir:?}");

                let spades = std::process::Command::new("spades.py")
                    .arg("-s")
                    .arg(&path_reads)
                    .arg("--sc")
                    .arg("-o")
                    .arg(&path_spades)
                    .arg("-t")
                    .arg(format!("{}", work_params_threading.threads_work))
                    .output()
                    .expect("spades command failed");

                if !spades.status.success() {
                    eprintln!("spades command failed with status: {}", spades.status);
                    std::io::stderr()
                        .write_all(&spades.stderr)
                        .expect("Failed to write to stderr");
                }

                let opts: zip::write::FileOptions<()> = zip::write::FileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);

                let mut file_contigs = File::open(&path_contigs).unwrap();
                let zip_path = barcode.join("contigs.fastq");
                if let Ok(_) = zip_writer.start_file_from_path(&zip_path, opts) {
                    std::io::copy(&mut file_contigs, &mut zip_writer).unwrap();
                }

                // let _ = fs::remove_dir_all(&path_dir);
            }
        });

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

        thread_pool.execute(move || {
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

                        let barcode_path = barcode_read.mangled_name();
                        let file_name = barcode_path
                            .file_name()
                            .and_then(|ext| ext.to_str())
                            .unwrap();

                        match file_name {
                            "reads.fastq" => {}
                            _ => continue,
                        }

                        let barcode = barcode_path.parent().unwrap();

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
        thread_pool.join();
        Ok(())
    }
}
