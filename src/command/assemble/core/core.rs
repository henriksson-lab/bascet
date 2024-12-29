use std::{
    fmt::format,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use rev_buf_reader::RevBufReader;
use zip::{ZipArchive, ZipWriter};

use crate::command::constants::RDB_PATH_INDEX_READS;

use super::{params, state, threading::DefaultThreadState};

pub struct RDBAssembler {}

impl RDBAssembler {
    pub fn assemble(
        params_io: &Arc<params::IO>,
        params_runtime: &Arc<params::Runtime>,
        params_threading: &Arc<params::Threading>,

        thread_states: &Arc<Vec<state::Threading>>,
        thread_pool: &threadpool::ThreadPool,
    ) -> anyhow::Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for tidx in 0..params_threading.threads_write {
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
                    let path_reads = path_temp.join("reads").with_extension("fastq");
                    let path_spades = path_temp.join("spades");
                    let path_contigs = path_spades.join("contigs.fasta");

                    let spades = std::process::Command::new("spades.py")
                        .arg("-s")
                        .arg(&path_reads)
                        .arg("--sc")
                        .arg("-o")
                        .arg(&path_spades)
                        .arg("-t")
                        .arg(format!("{}", params_threading.threads_work))
                        .output()
                        .expect("spades command failed");

                    if !spades.status.success() {
                        eprintln!("spades command failed with status: {}", spades.status);
                        std::io::stderr()
                            .write_all(&spades.stderr)
                            .expect("Failed to write to stderr");
                    }

                    let mut file_contigs = File::open(&path_contigs).unwrap();
                    let zippath_contigs = barcode.join("contigs.fastq");
                    let opts_zipwriter: zip::write::FileOptions<()> =
                        zip::write::FileOptions::default()
                            .compression_method(zip::CompressionMethod::Stored);
                    {
                        let mut zipwriter_rdb = thread_state.zip_writer.lock().unwrap();
                        if let Ok(_) = zipwriter_rdb.start_file(
                            zippath_contigs.to_str().unwrap().to_string(),
                            opts_zipwriter,
                        ) {
                            std::io::copy(&mut file_contigs, &mut *zipwriter_rdb).unwrap();
                        }
                    }
                    let _ = fs::remove_dir_all(&path_temp);
                    println!("Finished {barcode:?}")
                }
                let mut zipwriter_rdb = thread_state.zip_writer.lock().unwrap();
                zipwriter_rdb.finish().unwrap();
                println!("Worker {tidx} exiting");
            });
        }

        // let index_file = File::open(&params_io.path_idx)?;
        // let mut index_reader = BufReader::new(index_file);
        // let mut progress_index_rev_reader = RevBufReader::new(&mut index_reader);
        // let mut progress_index_last_line = String::new();
        // progress_index_rev_reader.read_line(&mut progress_index_last_line)?;
        // let _progress_index_last = progress_index_last_line
        //     .split(",")
        //     .next()
        //     .unwrap()
        //     .parse::<usize>()?;

        let file_rdb = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");
        let mut file_reads_index = archive_rdb_for_index
            .by_name(&RDB_PATH_INDEX_READS)
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
                    Some("reads.fastq") => {}
                    Some(_) => continue,
                    None => panic!("None value parsing read path"),
                }

                let path_barcode = path_read.parent().unwrap();
                let path_barcode_dir = params_io.path_tmp.join(path_barcode);
                let _ = fs::create_dir_all(&path_barcode_dir);

                let path_temp_reads = path_barcode_dir.join("reads.fastq");
                let file_temp_reads = File::create(&path_temp_reads).unwrap();
                let mut bufwriter_temp_reads = BufWriter::new(&file_temp_reads);

                std::io::copy(&mut zipfile_read, &mut bufwriter_temp_reads).unwrap();

                tx.send(Some(path_barcode.to_path_buf())).unwrap();
            }
        }

        for i in 0..params_threading.threads_write {
            println!("Sending termination signal {i}");
            tx.send(None).unwrap();
        }

        // Wait for the threads to complete
        thread_pool.join();
        println!("Finished Assembling!");
        Ok(())
    }
}
