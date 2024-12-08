use super::constants::CB_MIN_SIZE;
use super::params;
use anyhow::Result;
use rust_htslib::bam::{record::Aux, Read};
use std::any::Any;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Batch {
    pub barcode: Arc<Vec<u8>>,
    pub sequences: Arc<Mutex<Vec<String>>>,
    pub qualities: Arc<Mutex<Vec<String>>>,
}

impl Batch {
    fn new() -> Self {
        return Self {
            barcode: Arc::new(Vec::with_capacity(CB_MIN_SIZE)),
            sequences: Arc::new(Mutex::new(Vec::new())),
            qualities: Arc::new(Mutex::new(Vec::new())),
        };
    }
}

pub struct BAMProcessor<'a> {
    pub params_io: Arc<params::IO<'a>>,
    pub params_runtime: Arc<params::Runtime>,
    pub params_threading: Arc<params::Threading<'a>>,
}

impl<'a> BAMProcessor<'a> {
    pub fn new(
        params_io: params::IO<'a>,
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
        let (tx, rx) = crossbeam::channel::unbounded::<Option<Batch>>();
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        let mut bam = rust_htslib::bam::Reader::from_path(self.params_io.path_in)?;
        bam.set_thread_pool(self.params_threading.thread_pool_read)?;

        let params_runtime = self.params_runtime;

        let mut record = rust_htslib::bam::Record::new();
        let mut batch = Batch::new();
        while bam.read(&mut record).is_some() {
            if let Ok(aux) = record.aux(b"CB") {
                if let Aux::String(cb) = aux {
                    if !cb.is_empty() {
                        let new_barcode = cb.as_bytes();
                        if batch.barcode.as_slice() == new_barcode {
                            let (seq, qual) = (record.seq(), record.qual());
                            let seq_string = String::from_utf8_lossy(&seq.as_bytes());
                            let qual_string = String::from_utf8_lossy(qual);

                            if let (Ok(sequences), Ok(qualities)) =
                                (batch.sequences.lock(), batch.qualities.lock())
                            {
                                sequences.push(seq_string.to_string());
                                qualities.push(qual_string.to_string());
                            }
                        } else {
                            tx.send(Some(batch));
                            batch = Batch::new();
                            batch.barcode = Arc::new(Vec::from(new_barcode));
                        }
                    }
                }
            }

            // Send final batch
            if let Ok(seq) = batch.sequences.lock() {
                if !seq.is_empty() {
                    let _ = tx.send(Some(batch));
                }
            }

            for _ in 0..self.params_threading.threads_write {
                let _ = tx.send(None);
            }
        }

        for i in 0..self.params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let runtime_params = Arc::clone(&self.params_runtime);

            self.params_threading.thread_pool_write.execute(move || {
                while let Ok(Some(batch)) = rx.recv() {
                    if let (Ok(sequences), Ok(qualities)) =
                        (batch.sequences.lock(), batch.qualities.lock())
                    {
                        if sequences.len() < params_runtime.min_reads {
                            continue;
                        }

                        let cell_dir = output_dir.join(&batch.cb_str);
                        let _ = fs::create_dir_all(&cell_dir);
                        let reads_path = cell_dir.join("reads.fastq");

                        if let Ok(file) = File::create(&reads_path) {
                            let mut writer = BufWriter::with_capacity(256 * 1024, file);
                            for i in 0..batch.sequences.len() {
                                let _ = writeln!(
                                    writer,
                                    "@{}::{}",
                                    batch.read_names[i], batch.cb_str
                                );
                                let _ = writeln!(writer, "{}", batch.sequences[i]);
                                let _ = writeln!(writer, "+");
                                let _ = writeln!(writer, "{}", batch.qualities[i]);
                            }
                            let _ = writer.flush();

                            if params_runtime.run_spades {
                                let spades_out_dir = cell_dir.join("spades_out");
                                let _ = fs::create_dir_all(&spades_out_dir);

                                let status = Command::new("spades.py")
                                    .arg("-s") 
                                    .arg(&reads_path)
                                    .arg("--isolate")
                                    .arg("-t")
                                    .arg("1")
                                    .arg("-o")
                                    .arg(&spades_out_dir)
                                    .stdout(std::process::Stdio::null())
                                    .stderr(std::process::Stdio::null())
                                    .status()
                                    .expect("Failed to execute SPAdes");

                                if status.success() && params_runtime.cleanup {
                                    let _ = fs::remove_dir_all(&cell_dir);
                                }
                            }
                        }
                    }
                }
            });
        }
        Ok(())
    }
}
