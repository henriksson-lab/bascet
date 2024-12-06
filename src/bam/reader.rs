use anyhow::Result;
use crossbeam::channel::{bounded, Sender};
use regex::Regex;
use rust_htslib::bam::{record::Aux, Read};
use threadpool::ThreadPool;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone)]
struct CBBatch {
    cb: Vec<u8>,
    sequences: Vec<Vec<u8>>,
}

pub struct Reader {}

static CB_PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Z]+[0-9]+-[A-Z]+[0-9]+-[A-Z]+[0-9]+").unwrap());

impl Reader {
    
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_cell_index<P>(
        file: P,
        n_read_threads: u32,
        n_write_threads: usize
    ) -> Result<()>
    where
        P: AsRef<std::path::Path>,
    {
        let read_pool = rust_htslib::tpool::ThreadPool::new(n_read_threads).unwrap();
        let (tx, rx) = bounded::<Option<CBBatch>>(64);
        
        let mut bam = rust_htslib::bam::Reader::from_path(file)?;
        bam.set_thread_pool(&read_pool)?;

        std::thread::spawn(move || {
            let mut record = rust_htslib::bam::Record::new();
            let mut current_cb = Vec::new();
            let mut current_sequences = Vec::new();

            while bam.read(&mut record).is_some() {
                if let Ok(aux) = record.aux(b"CB") {
                    if let Aux::String(cb) = aux {
                        if !cb.is_empty() {
                            let cb_vec = cb.as_bytes().to_vec();
                            if current_cb != cb_vec && !current_cb.is_empty() {
                                let batch = CBBatch {
                                    cb: current_cb.clone(),
                                    sequences: current_sequences,
                                };
                                let _ = tx.send(Some(batch));
                                current_sequences = Vec::new();
                            }
                            
                            current_cb = cb_vec;
                            current_sequences.push(record.seq().as_bytes());
                        }
                    }
                }
            }
            
            // Send final batch
            if !current_sequences.is_empty() {
                let _ = tx.send(Some(CBBatch {
                    cb: current_cb,
                    sequences: current_sequences,
                }));
            }
            // Send None to all worker threads to allow cleanup
            for _ in 0..n_write_threads {
                let _ = tx.send(None);
            }
        });

        let write_pool = ThreadPool::new(n_write_threads);
        
        let tar_path = Path::new("./data/temp/output.tar");
        let _ = fs::create_dir_all(tar_path.parent().unwrap());
        let tar_file = File::create(tar_path).expect("Failed to create tar file");
        let builder = Arc::new(Mutex::new(tar::Builder::new(tar_file)));

        while let Ok(Some(batch)) = rx.recv() {
            let builder = Arc::clone(&builder);
            write_pool.execute(move || {
                let cb = String::from_utf8_lossy(&batch.cb);
                if CB_PATTERN.is_match(&cb) && batch.sequences.len() > 1000 {
                    // No idea why &* is necessary
                    let temp_path = Path::new("./data/temp").join(&*cb);
                    let _ = fs::create_dir_all(&temp_path);
                    let reads_path = temp_path.join("reads.fastq");
                    
                    if let Ok(file) = File::create(&reads_path) {
                        let mut writer = BufWriter::new(file);
                        for (i, seq) in batch.sequences.iter().enumerate() {
                            let _ = writeln!(writer, "@{}::{}", cb, i);
                            let _ = writeln!(writer, "{}", String::from_utf8_lossy(seq));
                            let _ = writeln!(writer, "+\n{}", String::from_utf8(vec![b'F'; seq.len()]).unwrap());
                        }
                        let _ = writer.flush();
                        
                        let spades_out_dir = temp_path.join("spades_out");
                        let _ = fs::create_dir_all(&spades_out_dir);
                        
                        let status = Command::new("spades.py")
                            .arg("-s").arg(&reads_path)
                            .arg("--isolate")
                            .arg("-t").arg("1")
                            .arg("-o").arg(&spades_out_dir)
                            .stdout(std::process::Stdio::null())  // Redirect stdout to null
                            .stderr(std::process::Stdio::null())
                            .status()
                            .expect("Failed to execute SPAdes");
                        
                        if status.success() {
                            let contigs_path = spades_out_dir.join("contigs.fasta");
                            if let Ok(mut builder) = builder.lock() {
                                if let Err(e) = builder.append_path_with_name(
                                    &contigs_path,
                                    format!("{}/contigs.fasta", cb)
                                ) {
                                    eprintln!("Failed to add contigs to tar: {}", e);
                                }
                                if let Err(e) = builder.append_path_with_name(
                                    &reads_path,
                                    format!("{}/reads.fastq", cb)
                                ) {
                                    eprintln!("Failed to add contigs to tar: {}", e);
                                }
                            }
                            
                            let _ = fs::remove_dir_all(&temp_path);
                        }
                    }
                }
            });
        }
        write_pool.join();

        if let Ok(mut builder) = builder.lock() {
            if let Err(e) = builder.finish() {
                eprintln!("Failed to finish tar file: {}", e);
            }
        }
        Ok(())
    }
}