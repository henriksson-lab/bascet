use super::constants::CB_PATTERN;
use super::params;
use anyhow::Result;
use rust_htslib::bam::{record::Aux, Read};
use std::any::Any;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::process::Command;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone)]
struct Batch {
    pub barcode: Vec<u8>,
    pub sequences: Vec<String>,
    pub qualities: Vec<String>,
}

impl Batch {
    fn new() -> Self {
        return Self {
            barcode: Vec::new(),
            sequences: Vec::new(),
            qualities: Vec::new(),
        };
    }
}

pub struct BAMProcessor<'a> {
    pub params_io: Arc<params::IO>,
    pub params_runtime: Arc<params::Runtime>,
    pub params_threading: Arc<params::Threading<'a>>,
}

impl<'a> BAMProcessor<'a> {
    pub fn new(
        params_io: params::IO,
        params_runtime: params::Runtime,
        params_threading: params::Threading<'a>,
    ) -> Self {
        Self {
            params_io: Arc::new(params_io),
            params_runtime: Arc::new(params_runtime),
            params_threading: Arc::new(params_threading),
        }
    }

    pub fn process_bam(&self) -> Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<Arc<RwLock<Batch>>>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for _ in 0..self.params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let params_io = Arc::clone(&self.params_io);
            let params_runtime = Arc::clone(&self.params_runtime);
            let params_threading = Arc::clone(&self.params_threading);

            params_threading.thread_pool_write.execute(move || {
                while let Ok(Some(batch)) = rx.recv() {
                    if let Ok(batch) = batch.read() {
                        if batch.sequences.len() < params_runtime.min_reads {
                            continue;
                        }
                        let barcode = &batch.barcode;
                        let sequences = &batch.sequences;
                        let qualities = &batch.qualities;
                        
                        let barcode_as_string =
                        String::from_utf8_lossy(barcode).to_string();
                        let cell_dir = params_io.path_tmp.join(&barcode_as_string);
                        let _ = fs::create_dir_all(&cell_dir);

                        let cell_reads_path = cell_dir.join("reads.fastq");
                        if let Ok(reads_file) = File::create(&cell_reads_path) {
                            let mut reads_writer = BufWriter::new(reads_file);
                            for i in 0..batch.sequences.len() {
                                let _ = writeln!(
                                    reads_writer,
                                    "@{}::{}",
                                    &barcode_as_string, i
                                );
                                let _ = writeln!(reads_writer, "{}", sequences[i]);
                                let _ = writeln!(reads_writer, "+");
                                let _ = writeln!(reads_writer, "{}", qualities[i]);
                            }
                            let _ = reads_writer.flush();
                        }
                    }
                }
            });
        }

        // Process BAM file
        let mut bam = rust_htslib::bam::Reader::from_path(&self.params_io.path_in)?;
        let _ = bam.set_thread_pool(self.params_threading.thread_pool_read);
        let mut record = rust_htslib::bam::Record::new();
        let mut batch = Arc::new(RwLock::new(Batch::new()));

        while bam.read(&mut record).is_some() {
            if let Ok(aux) = record.aux(b"CB") {
                if let Aux::String(cb) = aux {
                    if !cb.is_empty() && CB_PATTERN.is_match(cb) {
                        let mut keep_batch = true;
                        if let Ok(old_batch) = batch.read() {
                            keep_batch = &old_batch.barcode == cb.as_bytes();
                        }
                        if !keep_batch {
                            let _ = tx.send(Some(Arc::clone(&batch)));
                            batch = Arc::new(RwLock::new(Batch::new()));
                            batch.write().unwrap().barcode = cb.as_bytes().to_vec();
                        }
                        let (seq, qual) = (record.seq(), record.qual());
                        let seq_string = String::from_utf8(seq.as_bytes())?;
                        let qual_string = String::from_utf8_lossy(qual);

                        if let Ok(mut batch) = batch.write() {
                            batch.sequences.push(seq_string.to_string());
                            batch.qualities.push(qual_string.to_string());  
                        }
                    }
                }
            }
        }

        // Send final batch if not empty
        if let Ok(final_batch) = batch.read() {
            if !final_batch.sequences.is_empty() {
                let _ = tx.send(Some(Arc::clone(&batch)));
            }
        }

        // Send termination signals
        for _ in 0..self.params_threading.threads_write {
            let _ = tx.send(None);
        }

        // Wait for all writer threads to complete
        self.params_threading.thread_pool_write.join();

        Ok(())
    }
}