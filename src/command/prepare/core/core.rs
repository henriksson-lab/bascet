use super::constants::CB_PATTERN;
use super::params;
use super::threading::ThreadState;
use anyhow::Result;
use crossbeam::queue::SegQueue;
use rust_htslib::bam::{record::Aux, Read};
use std::io::{Seek, Write};
use std::sync::Arc;
use zip::write::FileOptions;

struct Batch {
    pub barcode: Vec<u8>,
    pub inner: SegQueue<(String, String)>,
}

impl Batch {
    fn new(barcode: &[u8]) -> Self {
        Self {
            barcode: Vec::from(barcode),
            inner: SegQueue::new(),
        }
    }
}

pub struct BAMProcessor<'a, W>
where
    W: Seek + Write,
{
    pub params_io: Arc<params::IO>,
    pub params_runtime: Arc<params::Runtime>,
    pub params_threading: Arc<params::Threading<'a, W>>,
}

impl<'a, W> BAMProcessor<'a, W>
where
    W: Seek + Write + Send + 'static,
{
    pub fn new(
        params_io: params::IO,
        params_runtime: params::Runtime,
        params_threading: params::Threading<'a, W>,
    ) -> Self {
        Self {
            params_io: Arc::new(params_io),
            params_runtime: Arc::new(params_runtime),
            params_threading: Arc::new(params_threading),
        }
    }

    pub fn process_bam(&self) -> Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<Arc<Batch>>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for ti in 0..self.params_threading.threads_write {
            let rx = Arc::clone(&rx);
            let params_runtime = Arc::clone(&self.params_runtime);
            let thread_state = Arc::clone(&self.params_threading.thread_states[ti]);
            let zip_writer = unsafe { &mut *thread_state.zip_writer.get() };

            self.params_threading.thread_pool_write.execute(move || {
                while let Ok(Some(batch)) = rx.recv() {
                    if batch.inner.len() < params_runtime.min_reads {
                        continue;
                    }

                    let barcode_as_string = String::from_utf8_lossy(&batch.barcode).to_string();
                    let fastq_path = format!("{}/reads.fastq", &barcode_as_string);

                    let opts: FileOptions<'_, ()> =
                        FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

                    if let Ok(_) = zip_writer.start_file(&fastq_path, opts) {
                        let mut index = 0;
                        while let Some((sequence, quality)) = batch.inner.pop() {
                            index += 1;
                            let _ = writeln!(zip_writer, "@{}::{}", &barcode_as_string, index);
                            let _ = writeln!(zip_writer, "{}", sequence);
                            let _ = writeln!(zip_writer, "+");
                            let _ = writeln!(zip_writer, "{}", quality);
                        }
                    }
                }
            });
        }

        // Process BAM file
        let mut bam = rust_htslib::bam::Reader::from_path(&self.params_io.path_in)?;
        let _ = bam.set_thread_pool(self.params_threading.thread_pool_read);
        let mut record = rust_htslib::bam::Record::new();
        let mut batch = Arc::new(Batch::new(b"Invalid Barcode"));

        while bam.read(&mut record).is_some() {
            if let Ok(aux) = record.aux(b"CB") {
                if let Aux::String(cb) = aux {
                    if !cb.is_empty() && CB_PATTERN.is_match(cb) {
                        if &batch.barcode != cb.as_bytes() {
                            let _ = tx.send(Some(Arc::clone(&batch)));
                            batch = Arc::new(Batch::new(cb.as_bytes()));
                        }
                        let (seq, qual) = (record.seq(), record.qual());
                        let seq_string = String::from_utf8(seq.as_bytes())?;
                        let qual_string =
                            String::from_utf8(qual.iter().map(|q| q + 33).collect::<Vec<u8>>())?;

                        batch
                            .inner
                            .push((seq_string.to_string(), qual_string.to_string()));
                    }
                }
            }
        }

        // Send final batch if not empty
        if !batch.inner.is_empty() {
            let _ = tx.send(Some(Arc::clone(&batch)));
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
