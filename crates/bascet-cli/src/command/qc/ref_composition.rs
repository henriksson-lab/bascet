use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

use anyhow::Result;
use bounded_integer::BoundedU64;
use clap::Args;
use clio::{InputPath, OutputPath};
use rust_htslib::bam::{Read, Reader};
use rust_htslib::bam::record::Record as BamRecord;
use tracing::{info, warn};

use bascet_derive::Budget;

use crate::bounded_parser;

#[derive(Args)]
pub struct QcRefCompositionCMD {
    #[arg(
        short = 'i',
        long = "in",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of BAM input files (comma-separated). Assumed sorted by cell (qname)."
    )]
    pub paths_in: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output file path"
    )]
    pub path_out: OutputPath,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to available parallelism)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-work",
        help = "Number of worker threads",
        value_name = "1.. (total - 1)",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_work: Option<BoundedU64<1, { u64::MAX }>>,
}

#[derive(Budget, Debug)]
struct RefCompositionBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[threads(TWork)]
    numof_threads_work: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite)]
    numof_threads_write: BoundedU64<1, 1>,
}

struct CellRow {
    id: Vec<u8>,
    entries: Vec<(String, u64)>,
}

impl QcRefCompositionCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        info!("Running QC ref-composition");

        let total_threads = self.total_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or_else(|e| {
                    warn!(error = %e, "Failed to determine available parallelism, using 2 threads");
                    2
                })
                .try_into()
                .unwrap_or_else(|e| {
                    warn!(error = %e, "Failed to convert parallelism to valid thread count, using 2 threads");
                    2.try_into().unwrap()
                })
        });

        let work_threads = self.numof_threads_work.unwrap_or_else(|| {
            BoundedU64::new(total_threads.get().saturating_sub(1).max(1)).unwrap()
        });

        let budget = RefCompositionBudget::builder()
            .threads(total_threads)
            .numof_threads_work(work_threads)
            .numof_threads_write(BoundedU64::new(1).unwrap())
            .build();

        budget.log();

        info!(input_files = self.paths_in.len(), "Starting ref-composition");

        ////////////////////////////////////////////////////////////////////
        // Create thread for writing output
        let output_file = self.path_out.clone().create()?;
        let (write_tx, write_rx) = crossbeam::channel::unbounded::<CellRow>();

        let thread_writer = budget.spawn::<TWrite, _, _>(0, move || {
            let mut bufwriter = BufWriter::new(output_file);
            bufwriter.write_all(b"id\treference\tcountof_reads\n").unwrap();

            while let Ok(row) = write_rx.recv() {
                for (ref_name, count) in &row.entries {
                    bufwriter.write_all(&row.id).unwrap();
                    bufwriter.write_all(b"\t").unwrap();
                    bufwriter.write_all(ref_name.as_bytes()).unwrap();
                    bufwriter.write_all(b"\t").unwrap();
                    bufwriter.write_all(count.to_string().as_bytes()).unwrap();
                    bufwriter.write_all(b"\n").unwrap();
                }
            }

            bufwriter.flush().unwrap();
        });

        ////////////////////////////////////////////////////////////////////
        // Distribute input files across worker threads via a queue
        let (file_tx, file_rx) = crossbeam::channel::unbounded::<PathBuf>();
        for input in &self.paths_in {
            let _ = file_tx.send(input.path().to_path_buf());
        }
        drop(file_tx);

        let arc_countof_cells_processed = Arc::new(AtomicU64::new(0));

        let numof_threads_work = (*budget.threads::<TWork>()).get();
        let mut vec_worker_handles = Vec::with_capacity(numof_threads_work as usize);

        for thread_idx in 0..numof_threads_work {
            let thread_file_rx = file_rx.clone();
            let thread_write_tx = write_tx.clone();
            let thread_countof_cells_processed = Arc::clone(&arc_countof_cells_processed);

            vec_worker_handles.push(budget.spawn::<TWork, _, _>(thread_idx, move || {
                while let Ok(path) = thread_file_rx.recv() {
                    info!(path = ?path, "Processing BAM");
                    if let Err(e) = process_file(&path, &thread_write_tx, &thread_countof_cells_processed) {
                        warn!(path = ?path, error = %e, "Failed to process BAM");
                    }
                }
            }));
        }

        drop(write_tx);
        for handle in vec_worker_handles {
            handle.join().unwrap();
        }
        thread_writer.join().unwrap();

        Ok(())
    }
}

fn process_file(
    path: &std::path::Path,
    write_tx: &crossbeam::channel::Sender<CellRow>,
    arc_countof_cells_processed: &Arc<AtomicU64>,
) -> Result<()> {
    let mut reader = Reader::from_path(path)?;
    let header = reader.header().clone();

    let ref_names: Vec<String> = header
        .target_names()
        .iter()
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .collect();

    let mut cell_counts: HashMap<usize, u64> = HashMap::new();
    let mut record_id_last: Vec<u8> = Vec::new();

    let mut record = BamRecord::new();
    while let Some(Ok(_)) = reader.read(&mut record) {
        if record.is_unmapped() {
            continue;
        }

        let record_qname = record.qname();
        let record_id = match memchr::memmem::find(record_qname, b"::") {
            Some(pos) => &record_qname[..pos],
            None => record_qname,
        };

        if record_id != record_id_last.as_slice() {
            if !record_id_last.is_empty() {
                flush_cell(&record_id_last, &ref_names, &cell_counts, write_tx);
                let n = arc_countof_cells_processed.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 100 == 0 {
                    info!(
                        countof_cells_processed = n,
                        current_cell = ?String::from_utf8_lossy(&record_id_last),
                        "Progress"
                    );
                }
                cell_counts.clear();
            }

            assert!(
                record_id_last.is_empty() || record_id > record_id_last.as_slice(),
                "BAM not sorted by cell: {:?} after {:?}",
                String::from_utf8_lossy(record_id),
                String::from_utf8_lossy(&record_id_last),
            );

            record_id_last = record_id.to_vec();
        }

        let tid = record.tid() as usize;
        *cell_counts.entry(tid).or_insert(0) += 1;
    }

    if !record_id_last.is_empty() {
        flush_cell(&record_id_last, &ref_names, &cell_counts, write_tx);
        arc_countof_cells_processed.fetch_add(1, Ordering::Relaxed);
    }

    info!(path = ?path, "Finished BAM");
    Ok(())
}

fn flush_cell(
    id: &[u8],
    reference_names: &[String],
    cell_counts: &HashMap<usize, u64>,
    write_tx: &crossbeam::channel::Sender<CellRow>,
) {
    let mut entries: Vec<(usize, u64)> = cell_counts
        .iter()
        .map(|(&tid, &count)| (tid, count))
        .collect();
    entries.sort_unstable_by_key(|&(tid, _)| tid);

    let _ = write_tx.send(CellRow {
        id: id.to_vec(),
        entries: entries
            .into_iter()
            .map(|(tid, count)| (reference_names[tid].clone(), count))
            .collect(),
    });
}
