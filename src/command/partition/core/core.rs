use crate::command::constants::RDB_FILENAME_READS;

use super::constants::CB_PATTERN;
use super::{params, state};
use anyhow::Result;
use crossbeam::queue::SegQueue;
use rust_htslib::bam::{record::Aux, Read};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use zip::write::FileOptions;

struct BamCell {
    pub barcode: Vec<u8>,
    pub inner: SegQueue<(String, String)>,
}

impl BamCell {
    fn new(barcode: &[u8]) -> Self {
        Self {
            barcode: Vec::from(barcode),
            inner: SegQueue::new(),
        }
    }
}

pub struct BAMProcessor {}
impl BAMProcessor {
    pub fn extract_cells(
        params_io: &Arc<params::IO>,
        params_runtime: &Arc<params::Runtime>,
        params_threading: &Arc<params::Threading>,

        thread_states: &Arc<Vec<state::Threading>>,
        thread_pool_read: &rust_htslib::tpool::ThreadPool,
        thread_pool_write: &threadpool::ThreadPool,
    ) -> Result<()> {
        let (tx, rx) = crossbeam::channel::bounded::<Option<Arc<BamCell>>>(64);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        for tidx in 0..params_threading.threads_work {
            let rx = Arc::clone(&rx);
            let params_runtime = Arc::clone(&params_runtime);
            let thread_states = Arc::clone(&thread_states);

            thread_pool_write.execute(move || {
                let thread_state = &thread_states[tidx];
                let mut zipwriter_rdb = thread_state.zip_writer.lock().unwrap();

                while let Ok(Some(bam_cell)) = rx.recv() {
                    if bam_cell.inner.len() < params_runtime.min_reads_per_cell {
                        continue;
                    }

                    let barcode_string = String::from_utf8_lossy(&bam_cell.barcode).to_string();
                    let path_reads = Path::new(&barcode_string).join(RDB_FILENAME_READS);

                    let opts: FileOptions<()> =
                        FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
                    if let Ok(_) = zipwriter_rdb.start_file(path_reads.to_str().unwrap(), opts) {
                        let mut index = 0;
                        while let Some((sequence, quality)) = bam_cell.inner.pop() {
                            index += 1;
                            let _ = writeln!(zipwriter_rdb, "@{}::{}", &barcode_string, index);
                            let _ = writeln!(zipwriter_rdb, "{}", sequence);
                            let _ = writeln!(zipwriter_rdb, "+");
                            let _ = writeln!(zipwriter_rdb, "{}", quality);
                        }
                    }
                }
            });
        }

        // Process BAM file
        let mut bamreader_input = rust_htslib::bam::Reader::from_path(&params_io.path_in)?;
        let _ = bamreader_input.set_thread_pool(thread_pool_read);

        let mut bam_record = rust_htslib::bam::Record::new();
        let mut bam_cell = Arc::new(BamCell::new(b"Invalid Barcode"));

        while let Some(_) = bamreader_input.read(&mut bam_record) {
            if let Ok(aux) = bam_record.aux(b"CB") {
                if let Aux::String(cb) = aux {
                    if !cb.is_empty() && CB_PATTERN.is_match(cb) {
                        if &bam_cell.barcode != cb.as_bytes() {
                            // first "finish" current cell
                            let _ = tx.send(Some(Arc::clone(&bam_cell)));
                            // start new cell
                            bam_cell = Arc::new(BamCell::new(cb.as_bytes()));
                        }

                        let (seq, qual) = (bam_record.seq(), bam_record.qual());
                        let seq_string = String::from_utf8(seq.as_bytes())
                            .expect("Could not parse sequence string");
                        let qual_string =
                            String::from_utf8(qual.iter().map(|q| q + 33).collect::<Vec<u8>>())
                                .expect("Could not parse qualities string");

                        bam_cell
                            .inner
                            .push((seq_string.to_string(), qual_string.to_string()));
                    }
                }
            }
        }

        // Send final batch if not empty
        if !bam_cell.inner.is_empty() {
            let _ = tx.send(Some(Arc::clone(&bam_cell)));
        }

        // Send termination signals
        for _ in 0..params_threading.threads_work {
            let _ = tx.send(None);
        }

        // Wait for all writer threads to complete
        thread_pool_write.join();

        Ok(())
    }
}
