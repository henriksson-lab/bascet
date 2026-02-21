use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::sync::{
    self,
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Result;
use bounded_integer::BoundedU64;
use clap::Args;
use clio::{InputPath, OutputPath};
use crossbeam::channel::TryRecvError;
use rust_htslib::bam::{Read, Reader};
use rust_htslib::bam::record::Record as BamRecord;
use tracing::{debug, info, warn};

use bascet_core::{
    threading::spinpark_loop::{self, SpinPark, SPINPARK_COUNTOF_PARKS_BEFORE_WARN},
    SendPtr,
};
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

        info!(
            input_files = self.paths_in.len(),
            "Starting ref-composition"
        );

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

        let numof_threads_work = (*budget.threads::<TWork>()).get();

        for (input_idx, input) in self.paths_in.iter().enumerate() {
            info!(path = %input, "Processing BAM");

            let mut reader = Reader::from_path(&**input.path())?;
            let header = reader.header().clone();

            let ref_names: Vec<String> = header
                .target_names()
                .iter()
                .map(|name| String::from_utf8_lossy(name).into_owned())
                .collect();

            let mut worker_counts: Vec<HashMap<usize, u64>> = (0..numof_threads_work)
                .map(|_| HashMap::new())
                .collect();

            let arc_flag_synchronize = Arc::new(AtomicBool::new(false));
            let arc_barrier = Arc::new(sync::Barrier::new((numof_threads_work + 1) as usize));

            let mut vec_worker_handles = Vec::with_capacity(numof_threads_work as usize);
            let (work_tx, work_rx) = crossbeam::channel::unbounded::<usize>();

            for thread_idx in 0..numof_threads_work {
                let thread_work_rx = work_rx.clone();
                let mut counts_ptr = unsafe {
                    SendPtr::new_unchecked(
                        &mut worker_counts[thread_idx as usize] as *mut HashMap<usize, u64>,
                    )
                };
                let thread_flag_synchronize = Arc::clone(&arc_flag_synchronize);
                let thread_barrier = Arc::clone(&arc_barrier);

                vec_worker_handles.push(budget.spawn::<TWork, _, _>(thread_idx as u64, move || {
                    let thread = std::thread::current();
                    let thread_name = thread.name().unwrap_or("unknown thread");
                    debug!(thread = thread_name, "Starting worker");

                    let mut thread_spinpark_counter = 0;
                    loop {
                        let tid = match thread_work_rx.try_recv() {
                            Ok(tid) => tid,
                            Err(TryRecvError::Empty) => {
                                if thread_flag_synchronize.load(Ordering::Relaxed) {
                                    thread_barrier.wait();
                                    thread_barrier.wait();
                                }
                                match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(&mut thread_spinpark_counter) {
                                    SpinPark::Warn => warn!(source = "RefComposition::worker", "channel empty, producer slow"),
                                    _ => {}
                                }
                                continue;
                            }
                            Err(TryRecvError::Disconnected) => break,
                        };
                        thread_spinpark_counter = 0;

                        // SAFETY: Each worker has exclusive access to its own counts via raw pointer.
                        // Barriers ensure no concurrent access during sync.
                        unsafe {
                            *counts_ptr.as_mut().entry(tid).or_insert(0) += 1;
                        }
                    }
                }));
            }

            let mut record_id_last: Vec<u8> = Vec::new();
            let mut countof_cells_processed = 0u64;
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
                        flush_cell(
                            &record_id_last,
                            &mut worker_counts,
                            &ref_names,
                            &arc_flag_synchronize,
                            &arc_barrier,
                            &write_tx,
                        );

                        countof_cells_processed += 1;
                        if countof_cells_processed % 100 == 0 {
                            info!(countof_cells_processed = countof_cells_processed, current_cell = ?String::from_utf8_lossy(&record_id_last), "Progress");
                        }
                    }

                    assert!(
                        record_id_last.is_empty() || record_id > record_id_last.as_slice(),
                        "BAM not sorted by cell: {:?} after {:?}",
                        String::from_utf8_lossy(record_id),
                        String::from_utf8_lossy(&record_id_last),
                    );

                    record_id_last = record_id.to_vec();
                }

                let _ = work_tx.send(record.tid() as usize);
            }

            if !record_id_last.is_empty() {
                flush_cell(
                    &record_id_last,
                    &mut worker_counts,
                    &ref_names,
                    &arc_flag_synchronize,
                    &arc_barrier,
                    &write_tx,
                );
                countof_cells_processed += 1;
            }

            drop(work_tx);
            for handle in vec_worker_handles {
                handle.join().unwrap();
            }

            info!(countof_cells_processed = countof_cells_processed, input_idx = input_idx, path = %input, "Finished BAM");
        }

        drop(write_tx);
        thread_writer.join().unwrap();

        Ok(())
    }
}

fn flush_cell(
    id: &[u8],
    worker_counts: &mut Vec<HashMap<usize, u64>>,
    ref_names: &[String],
    arc_flag_synchronize: &Arc<AtomicBool>,
    arc_barrier: &Arc<sync::Barrier>,
    write_tx: &crossbeam::channel::Sender<CellRow>,
) {
    arc_flag_synchronize.store(true, Ordering::Relaxed);
    arc_barrier.wait();

    // SAFETY: Workers are blocked at barrier, coordinator has exclusive access
    let mut merged: HashMap<usize, u64> = HashMap::new();
    for counts in worker_counts.iter_mut() {
        for (&tid, &count) in counts.iter() {
            *merged.entry(tid).or_insert(0) += count;
        }
        counts.clear();
    }

    let mut entries: Vec<(usize, u64)> = merged.into_iter().collect();
    entries.sort_unstable_by_key(|&(tid, _)| tid);

    let _ = write_tx.send(CellRow {
        id: id.to_vec(),
        entries: entries
            .into_iter()
            .map(|(tid, count)| (ref_names[tid].clone(), count))
            .collect(),
    });

    arc_flag_synchronize.store(false, Ordering::Relaxed);
    arc_barrier.wait();
}
