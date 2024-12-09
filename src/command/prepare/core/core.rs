use super::constants::CB_PATTERN;
use super::params;
use anyhow::Result;
use rust_htslib::bam::{record::Aux, Read};
use std::io::{Write, BufWriter};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use zip::write::{FileOptions, ZipWriter};
use std::fs::{self, File};

#[derive(Clone)]
struct Batch {
    pub barcode: Vec<u8>,
    pub sequences: Vec<String>,
    pub qualities: Vec<String>,
}

impl Batch {
    fn new() -> Self {
        Self {
            barcode: Vec::new(),
            sequences: Vec::new(),
            qualities: Vec::new(),
        }
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
        let file_index = Arc::new(AtomicUsize::new(0));

        fs::create_dir_all(&self.params_io.path_tmp)?;
        
        let zip_writer = Arc::new(Mutex::new(ZipWriter::new(File::create(&self.params_io.path_out)?)));
        let index_writer = Arc::new(Mutex::new(BufWriter::new(File::create(self.params_io.path_tmp.join("index.tsv"))?)));
        
        for _ in 0..self.params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let params_runtime = Arc::clone(&self.params_runtime);
            let file_index = Arc::clone(&file_index);
            let zip_writer = Arc::clone(&zip_writer);
            let index_writer = Arc::clone(&index_writer);
            let path_tmp = self.params_io.path_tmp.clone();

            self.params_threading.thread_pool_write.execute(move || {
                while let Ok(Some(batch)) = rx.recv() {
                    if let Ok(batch) = batch.read() {
                        if batch.sequences.len() < params_runtime.min_reads {
                            continue;
                        }

                        let barcode_as_string = String::from_utf8_lossy(&batch.barcode).to_string();
                        let fastq_path = format!("{}/reads.fastq", &barcode_as_string);
                        let temp_file_path = path_tmp.join(&fastq_path);

                        // Create directories if needed
                        if let Some(parent) = temp_file_path.parent() {
                            let _ = fs::create_dir_all(parent);
                        }

                        // Write FASTQ file
                        if let Ok(file) = File::create(&temp_file_path) {
                            let mut writer = BufWriter::new(file);
                            for i in 0..batch.sequences.len() {
                                let _ = writeln!(writer, "@{}::{}", &barcode_as_string, i);
                                let _ = writeln!(writer, "{}", batch.sequences[i]);
                                let _ = writeln!(writer, "+");
                                let _ = writeln!(writer, "{}", batch.qualities[i]);
                            }
                            let _ = writer.flush();

                            // Add to zip archive
                            if let Ok(mut zip) = zip_writer.lock() {
                                let opts = FileOptions::default()
                                    .compression_method(zip::CompressionMethod::Deflated);
                                if let Ok(_) = zip.start_file::<_, ()>(&fastq_path, opts) {
                                    let mut f = File::open(&temp_file_path).unwrap();
                                    std::io::copy(&mut f, &mut *zip).unwrap();
                                }
                            }

                            // Write to index
                            let current_index = file_index.fetch_add(1, Ordering::SeqCst);
                            if let Ok(mut index) = index_writer.lock() {
                                let _ = writeln!(index, "{}\t{}", current_index, fastq_path);
                            }

                            // Clean up temp file and directory
                            let _ = fs::remove_file(&temp_file_path);
                            if let Some(parent) = temp_file_path.parent() {
                                let _ = fs::remove_dir(parent);
                            }
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

        // Finalize zip
        if let Ok(zip) = Arc::try_unwrap(zip_writer) {
            if let Ok(mut zip) = zip.into_inner() {
                let _ = zip.finish();
            }
        }

        Ok(())
    }
}