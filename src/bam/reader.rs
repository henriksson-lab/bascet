use anyhow::Result;
use crossbeam::channel::{bounded, Sender};
use regex::Regex;
use rust_htslib::bam::{record::Aux, Read};
use threadpool::ThreadPool;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::LazyLock;

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
        let (tx, rx) = bounded::<CBBatch>(64);
        
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
                                let _ = tx.send(batch);
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
                let _ = tx.send(CBBatch {
                    cb: current_cb,
                    sequences: current_sequences,
                });
            }
        });

        let write_pool = ThreadPool::new(n_write_threads);
        
        while let Ok(batch) = rx.recv() {
            write_pool.execute(move || {
                let cb = String::from_utf8_lossy(&batch.cb);
                if CB_PATTERN.is_match(&cb) && batch.sequences.len() > 1000 {
                    let filename = format!("./data/temp/{}.fasta", cb);
                    if let Ok(file) = File::create(filename) {
                        let mut writer = BufWriter::new(file);
                        
                        for (i, seq) in batch.sequences.iter().enumerate() {
                            let _ = writeln!(writer, ">{}::{}", cb, i);
                            let _ = writeln!(writer, "{}", String::from_utf8_lossy(seq));
                        }
                       
                        let _ = writer.flush();
                    }
                }
            });
        }

        write_pool.join();
        Ok(())
    }
}