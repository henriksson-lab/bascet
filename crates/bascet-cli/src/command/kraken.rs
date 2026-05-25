use crate::{
    bounded_parser,
    utils::{atomic_temp_path, publish_atomic_output},
};

use bascet_core::{
    attr::{meta::*, quality::*, sequence::*},
    *,
};
use bascet_derive::Budget;

use anyhow::Result;
use bascet_io::{codec, parse, tirp};
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use kraken2_pure_rs::{
    classify::{ClassifyDb, ClassifyOptions, classify_sequence},
    minimizer::MinimizerScanner,
    readcounts::TaxonCounters,
    types::{Sequence, SequenceFormat},
};
use rayon::prelude::*;
use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    thread::JoinHandle,
    time::Instant,
};
use tracing::{info, warn};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;

pub const DEFAULT_PATH_TEMP: &str = "temp";
const KRAKEN_CLASSIFY_BATCH_SIZE: usize = 10_000;
const KRAKEN_OUTPUT_FLUSH_INTERVAL: u64 = 1_000_000;
const KRAKEN_MIN_STREAM_BUFFER: ByteSize = ByteSize::mib(256);
const KRAKEN_MEMORY_HEADROOM: ByteSize = ByteSize::mib(512);

use crate::fileformat::new_anndata::SparseMatrixAnnDataBuilder;

struct KrakenReadPair {
    cell_id: Arc<[u8]>,
    header: Option<String>,
    r1: String,
    r2: String,
}

type KrakenBatch = Vec<KrakenReadPair>;

struct KrakenClassifyScratch {
    scanner: MinimizerScanner,
    taxa: Vec<kraken2_pure_rs::types::TaxId>,
    hit_counts: ahash::AHashMap<kraken2_pure_rs::types::TaxId, u32>,
    tx_frames: Vec<String>,
    taxon_counters: TaxonCounters,
    output_buf: String,
    r1: Sequence,
    r2: Sequence,
}

struct KrakenClassification {
    raw_line: Option<String>,
    external_taxid: Option<u32>,
}

#[derive(Default)]
struct KrakenCellCounts {
    taxid_counter: BTreeMap<u32, u32>,
    unclassified_counter: u32,
}

#[derive(Default)]
struct KrakenMatrixAccumulator {
    cell_counts: BTreeMap<Arc<[u8]>, KrakenCellCounts>,
}

impl KrakenMatrixAccumulator {
    fn add_call(&mut self, cell_id: &Arc<[u8]>, external_taxid: Option<u32>) {
        let cell_counts = self.cell_counts.entry(Arc::clone(cell_id)).or_default();
        if let Some(taxid) = external_taxid {
            *cell_counts.taxid_counter.entry(taxid + 1).or_insert(0) += 1;
        } else {
            cell_counts.unclassified_counter += 1;
        }
    }

    fn add(&mut self, pair: &KrakenReadPair, classification: &KrakenClassification) {
        self.add_call(&pair.cell_id, classification.external_taxid);
    }

    fn merge(&mut self, other: KrakenMatrixAccumulator) {
        for (cell_id, other_counts) in other.cell_counts {
            let counts = self.cell_counts.entry(cell_id).or_default();
            counts.unclassified_counter += other_counts.unclassified_counter;
            for (taxid, count) in other_counts.taxid_counter {
                *counts.taxid_counter.entry(taxid).or_insert(0) += count;
            }
        }
    }

    fn into_anndata_builder(self) -> Result<SparseMatrixAnnDataBuilder> {
        let mut matrix = SparseMatrixAnnDataBuilder::new();

        for (cell_id, mut counts) in self.cell_counts {
            let cell_index = matrix.get_or_create_cell(cell_id.as_ref());
            matrix.add_cell_counts_per_feature_index(cell_index, &mut counts.taxid_counter);
            matrix.add_unclassified(cell_index, counts.unclassified_counter);
        }

        matrix.compress_feature_column("taxid_")?;
        Ok(matrix)
    }
}

#[derive(Args)]
pub struct KrakenCMD {
    #[arg(
        short = 'i',
        long = "in",
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(long = "out-raw", help = "Raw KRAKEN2 output file")]
    pub path_out_raw: Option<PathBuf>,

    #[arg(
        long = "enable-raw-output",
        help = "Write raw KRAKEN2 output in addition to the count matrix"
    )]
    pub enable_raw_output: bool,

    #[arg(long = "out-matrix", help = "Output count matrix")]
    pub path_out_matrix: PathBuf,

    #[arg(long = "temp", help = "Temp directory; must exist already")]
    pub path_temp: PathBuf,

    #[arg(short = 'd', long = "db", help = "KRAKEN2 index to use")]
    pub path_db: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to std::threads::available parallelism)",
        value_name = "4..",
        value_parser = bounded_parser!(BoundedU64<4, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<4, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gb(30),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
        value_name = "100%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,
}

#[derive(Budget, Debug)]
struct KrakenBudget {
    #[threads(Total)]
    threads: BoundedU64<4, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

#[derive(Debug, Clone, Copy)]
struct KrakenThreadAllocation {
    read_threads: BoundedU64<1, { u64::MAX }>,
    classify_threads: usize,
}

impl KrakenCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        //Validate that a KRAKEN2 db has been given
        if self.path_db.is_dir() {
            let file_taxo = self.path_db.join("taxo.k2d");
            if !file_taxo.is_file() {
                anyhow::bail!(
                    "Specified database path is not a KRAKEN2 database (directory misses files, e.g., taxo.k2d)"
                );
            }
        } else {
            anyhow::bail!("Specified database path is not a KRAKEN2 database (not a directory)");
        }

        let budget = KrakenBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to determine available parallelism, using 4 threads");
                        4
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to convert parallelism to valid thread count, using 4 threads");
                        4.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();
        let thread_allocation = Self::allocate_threads(budget.threads.get() as usize);

        info!(
            using = %budget,
            read_threads = thread_allocation.read_threads.get(),
            classify_threads = thread_allocation.classify_threads,
            input_path = ?self.path_in,
            path_out_raw = ?self.path_out_raw,
            "Starting KRAKEN2"
        );

        let path_out_raw_tmp = if self.enable_raw_output {
            let path_out_raw = self
                .path_out_raw
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--enable-raw-output requires --out-raw"))?;
            Some(atomic_temp_path(path_out_raw))
        } else {
            None
        };

        /////////////////////////////////////////////////////////////////////////////////////
        // Stream read pairs directly into the Rust Kraken library.
        let matrix = Self::write_tirp_to_kraken(
            self.path_in.path().path(),
            &self.path_db,
            path_out_raw_tmp.as_deref(),
            thread_allocation.classify_threads,
            thread_allocation.read_threads,
            self.sizeof_stream_arena,
            budget.sizeof_stream_buffer,
        )?;

        if let Some(path_out_raw_tmp) = path_out_raw_tmp {
            let path_out_raw = self
                .path_out_raw
                .as_ref()
                .expect("--enable-raw-output path checked earlier");
            publish_atomic_output(path_out_raw_tmp, path_out_raw)?;
        }

        info!("Storing count table to {}", self.path_out_matrix.display());
        let path_matrix_tmp = atomic_temp_path(&self.path_out_matrix);
        matrix
            .into_anndata_builder()?
            .save_to_anndata(&path_matrix_tmp)
            .expect("Failed to save to HDF5 file");
        publish_atomic_output(path_matrix_tmp, &self.path_out_matrix)?;

        info!("All KRAKEN2 steps complete");

        //Move temp files to their right positions

        Ok(())
    }

    fn allocate_threads(total_threads: usize) -> KrakenThreadAllocation {
        let read_threads = (total_threads / 4).max(2);
        KrakenThreadAllocation {
            read_threads: bounded_integer::BoundedU64::new(read_threads as u64)
                .expect("read thread allocation is at least one"),
            classify_threads: total_threads,
        }
    }

    ///
    /// Get a TIRP, stream read pairs directly to Kraken.
    ///
    fn write_tirp_to_kraken(
        path_in: impl AsRef<Path>,
        path_db: impl AsRef<Path>,
        path_out_raw: Option<&Path>,
        classify_threads: usize,
        read_threads: BoundedU64<1, { u64::MAX }>,
        sizeof_stream_arena: ByteSize,
        sizeof_stream_buffer: ByteSize,
    ) -> Result<KrakenMatrixAccumulator> {
        let db = Self::load_kraken_db(path_db)?;
        let sizeof_stream_buffer = Self::kraken_stream_buffer_after_db_load(sizeof_stream_buffer);
        let classify_opts = ClassifyOptions {
            paired_end_processing: true,
            single_file_pairs: true,
            use_translated_search: !db.idx_opts.dna_db,
            ..ClassifyOptions::default()
        };
        let classify_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(classify_threads)
            .thread_name(|idx| format!("KrakenClassify@{idx}"))
            .build()?;
        let write_raw_output = path_out_raw.is_some();
        let mut writer = path_out_raw
            .map(std::fs::File::create)
            .transpose()?
            .map(BufWriter::new);

        let (batch_rx, reader_handle) = Self::spawn_kraken_batch_reader(
            path_in.as_ref().to_path_buf(),
            read_threads,
            sizeof_stream_arena,
            sizeof_stream_buffer,
            write_raw_output,
        )?;

        info!("Classifying read pairs");
        let mut num_read: u64 = 0;
        let mut matrix = KrakenMatrixAccumulator::default();
        let mut total_read_time = std::time::Duration::ZERO;
        let mut total_classify_time = std::time::Duration::ZERO;
        let mut total_accumulate_time = std::time::Duration::ZERO;
        while let Ok(batch_result) = batch_rx.recv() {
            let (batch, read_time) = batch_result?;
            total_read_time += read_time;
            num_read += batch.len() as u64;

            let classify_started = Instant::now();
            let lines = if write_raw_output {
                Some(classify_pool.install(|| {
                    batch
                        .par_iter()
                        .map_init(
                            || Self::kraken_classify_scratch(&db),
                            |scratch, pair| {
                                Self::classify_kraken_pair(&db, &classify_opts, pair, scratch, true)
                            },
                        )
                        .collect::<Vec<_>>()
                }))
            } else {
                let batch_matrix = classify_pool.install(|| {
                    batch
                        .par_iter()
                        .fold(
                            || {
                                (
                                    Self::kraken_classify_scratch(&db),
                                    KrakenMatrixAccumulator::default(),
                                )
                            },
                            |(mut scratch, mut local_matrix), pair| {
                                let classification = Self::classify_kraken_pair(
                                    &db,
                                    &classify_opts,
                                    pair,
                                    &mut scratch,
                                    false,
                                );
                                local_matrix.add(pair, &classification);
                                (scratch, local_matrix)
                            },
                        )
                        .map(|(_scratch, local_matrix)| local_matrix)
                        .reduce(KrakenMatrixAccumulator::default, |mut left, right| {
                            left.merge(right);
                            left
                        })
                });
                matrix.merge(batch_matrix);
                None
            };
            total_classify_time += classify_started.elapsed();

            let accumulate_started = Instant::now();
            if let Some(lines) = lines {
                for (pair, classification) in batch.iter().zip(lines.iter()) {
                    if let (Some(writer), Some(raw_line)) =
                        (writer.as_mut(), classification.raw_line.as_ref())
                    {
                        writer.write_all(raw_line.as_bytes())?;
                    }
                    matrix.add(pair, classification);
                }
            }
            total_accumulate_time += accumulate_started.elapsed();
            if num_read % KRAKEN_OUTPUT_FLUSH_INTERVAL == 0 {
                if let Some(writer) = writer.as_mut() {
                    writer.flush()?;
                }
                info!(
                    read_pairs = num_read,
                    read_time = ?total_read_time,
                    classify_time = ?total_classify_time,
                    accumulate_time = ?total_accumulate_time,
                    "Classified read pairs"
                );
            }
        }
        info!(
            read_time = ?total_read_time,
            classify_time = ?total_classify_time,
            accumulate_time = ?total_accumulate_time,
            "All readpairs classified"
        );
        reader_handle
            .join()
            .map_err(|_| anyhow::anyhow!("KRAKEN reader thread panicked"))??;

        if let Some(writer) = writer.as_mut() {
            writer.flush()?;
            info!("All KRAKEN2 output flushed");
        }

        Ok(matrix)
    }

    fn spawn_kraken_batch_reader(
        path_in: PathBuf,
        read_threads: BoundedU64<1, { u64::MAX }>,
        sizeof_stream_arena: ByteSize,
        sizeof_stream_buffer: ByteSize,
        write_raw_output: bool,
    ) -> Result<(
        crossbeam::channel::Receiver<Result<(KrakenBatch, std::time::Duration)>>,
        JoinHandle<Result<()>>,
    )> {
        let (batch_tx, batch_rx) = crossbeam::channel::bounded(3);
        let reader_handle = std::thread::Builder::new()
            .name("KrakenRead@0".to_string())
            .spawn(move || -> Result<()> {
                let decoder = codec::BBGZDecoder::builder()
                    .with_path(path_in)
                    .countof_threads(read_threads)
                    .build();
                let parser = parse::Tirp::builder().build();

                let mut stream = Stream::builder()
                    .with_decoder(decoder)
                    .with_parser(parser)
                    .sizeof_decode_arena(sizeof_stream_arena)
                    .sizeof_decode_buffer(sizeof_stream_buffer)
                    .build();

                let mut query = stream.query::<tirp::Record>();
                let mut num_read = 0_u64;

                loop {
                    let read_started = Instant::now();
                    let mut batch = Vec::with_capacity(KRAKEN_CLASSIFY_BATCH_SIZE);
                    while batch.len() < KRAKEN_CLASSIFY_BATCH_SIZE {
                        match query.next_into::<tirp::Record>() {
                            Ok(Some(record)) => {
                                batch.push(Self::kraken_read_pair_from_record(
                                    &record,
                                    num_read,
                                    write_raw_output,
                                ));
                                num_read += 1;
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let _ = batch_tx.send(Err(e));
                                return Ok(());
                            }
                        }
                    }

                    if batch.is_empty() {
                        break;
                    }

                    let read_time = read_started.elapsed();
                    if batch_tx.send(Ok((batch, read_time))).is_err() {
                        break;
                    }
                }

                Ok(())
            })?;

        Ok((batch_rx, reader_handle))
    }

    pub fn write_tirp_to_interleaved_fq<P>(
        path_in: P,
        path_out: P,
        num_threads: BoundedU64<1, { u64::MAX }>,
        sizeof_stream_arena: ByteSize,
        sizeof_stream_buffer: ByteSize,
    ) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let decoder = codec::BBGZDecoder::builder()
            .with_path(path_in)
            .countof_threads(num_threads)
            .build();
        let parser = parse::Tirp::builder().build();

        let mut stream = Stream::builder()
            .with_decoder(decoder)
            .with_parser(parser)
            .sizeof_decode_arena(sizeof_stream_arena)
            .sizeof_decode_buffer(sizeof_stream_buffer)
            .build();

        let mut query = stream.query::<tirp::Record>();
        let mut writer = BufWriter::new(std::fs::File::create(&path_out)?);
        let mut num_read: u64 = 0;

        loop {
            match query.next_into::<tirp::Record>() {
                Ok(Some(record)) => {
                    Self::write_fastq_record(
                        &mut writer,
                        *record.get_ref::<Id>(),
                        *record.get_ref::<R1>(),
                        *record.get_ref::<Q1>(),
                        *record.get_ref::<Umi>(),
                        num_read,
                    )?;
                    Self::write_fastq_record(
                        &mut writer,
                        *record.get_ref::<Id>(),
                        *record.get_ref::<R2>(),
                        *record.get_ref::<Q2>(),
                        *record.get_ref::<Umi>(),
                        num_read,
                    )?;
                    num_read += 1;
                }
                Ok(None) => break,
                Err(e) => panic!("{:?}", e),
            }
        }

        writer.flush()?;
        Ok(())
    }

    fn write_fastq_record<W>(
        writer: &mut W,
        record_id: &[u8],
        record_read: &[u8],
        record_qual: &[u8],
        record_umi: &[u8],
        num_read: u64,
    ) -> Result<()>
    where
        W: Write,
    {
        writer.write_all(b"@")?;
        writer.write_all(record_id)?;
        writer.write_all(b":")?;
        writer.write_all(record_umi)?;
        writer.write_all(b":")?;
        write!(writer, "{}", num_read)?;
        writer.write_all(b"\n")?;
        writer.write_all(record_read)?;
        writer.write_all(b"\n+\n")?;
        writer.write_all(record_qual)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn kraken_read_pair_from_record(
        record: &tirp::Record,
        num_read: u64,
        write_raw_output: bool,
    ) -> KrakenReadPair {
        let record_id = *record.get_ref::<Id>();
        let header = if write_raw_output {
            let record_umi = *record.get_ref::<Umi>();
            Some(format!(
                "{}:{}:{}",
                String::from_utf8_lossy(record_id),
                String::from_utf8_lossy(record_umi),
                num_read
            ))
        } else {
            None
        };

        KrakenReadPair {
            cell_id: Arc::from(record_id),
            header,
            r1: Self::record_sequence_string(*record.get_ref::<R1>()),
            r2: Self::record_sequence_string(*record.get_ref::<R2>()),
        }
    }

    fn record_sequence_string(seq: &[u8]) -> String {
        debug_assert!(std::str::from_utf8(seq).is_ok());
        // TIRP read sequences are ASCII nucleotide strings. Kraken's Sequence type
        // stores sequence data as String, so keep the ownership transfer cheap.
        unsafe { String::from_utf8_unchecked(seq.to_vec()) }
    }

    fn kraken_classify_scratch(db: &ClassifyDb) -> KrakenClassifyScratch {
        KrakenClassifyScratch {
            scanner: MinimizerScanner::new(
                db.idx_opts.k as isize,
                db.idx_opts.l as isize,
                db.idx_opts.spaced_seed_mask,
                db.idx_opts.dna_db,
                db.idx_opts.toggle_mask,
                db.idx_opts.revcom_version,
            ),
            taxa: Vec::new(),
            hit_counts: Default::default(),
            tx_frames: Vec::new(),
            taxon_counters: TaxonCounters::new(),
            output_buf: String::with_capacity(512),
            r1: Sequence {
                format: SequenceFormat::Fastq,
                ..Sequence::default()
            },
            r2: Sequence {
                format: SequenceFormat::Fastq,
                ..Sequence::default()
            },
        }
    }

    fn classify_kraken_pair(
        db: &ClassifyDb,
        classify_opts: &ClassifyOptions,
        pair: &KrakenReadPair,
        scratch: &mut KrakenClassifyScratch,
        write_raw_output: bool,
    ) -> KrakenClassification {
        Self::fill_kraken_sequence(&mut scratch.r1, pair, true);
        Self::fill_kraken_sequence(&mut scratch.r2, pair, false);

        let call = classify_sequence(
            &scratch.r1,
            Some(&scratch.r2),
            &db.hash,
            &db.taxonomy,
            &db.idx_opts,
            classify_opts,
            &mut scratch.scanner,
            &mut scratch.taxa,
            &mut scratch.hit_counts,
            &mut scratch.tx_frames,
            &mut scratch.taxon_counters,
            &mut scratch.output_buf,
        );

        let external_taxid = if call == 0 {
            None
        } else {
            Some(db.taxonomy.node(call).external_id as u32)
        };

        KrakenClassification {
            raw_line: write_raw_output.then(|| scratch.output_buf.clone()),
            external_taxid,
        }
    }

    fn fill_kraken_sequence(seq: &mut Sequence, pair: &KrakenReadPair, first_mate: bool) {
        seq.format = SequenceFormat::Fastq;
        seq.header.clear();
        if let Some(header) = &pair.header {
            seq.header.push_str(header);
        }
        seq.comment.clear();
        seq.seq.clear();
        seq.seq
            .push_str(if first_mate { &pair.r1 } else { &pair.r2 });
        seq.quals.clear();
    }

    fn load_kraken_db(path_db: impl AsRef<Path>) -> Result<ClassifyDb> {
        let path_db = path_db.as_ref();
        let hash_path = path_db.join("hash.k2d");
        let taxonomy_path = path_db.join("taxo.k2d");
        let opts_path = path_db.join("opts.k2d");

        for required_file in [&hash_path, &taxonomy_path, &opts_path] {
            if !required_file.is_file() {
                anyhow::bail!(
                    "Specified database path is not a KRAKEN2 database (missing {})",
                    required_file.display()
                );
            }
        }

        let hash_path = hash_path.to_string_lossy().into_owned();
        let taxonomy_path = taxonomy_path.to_string_lossy().into_owned();
        let opts_path = opts_path.to_string_lossy().into_owned();

        info!(
            db = %path_db.display(),
            hash = %hash_path,
            taxonomy = %taxonomy_path,
            opts = %opts_path,
            "Loading KRAKEN2 database"
        );
        let started = Instant::now();
        let rss_before_load = Self::current_rss();
        let db = ClassifyDb::from_files(&hash_path, &taxonomy_path, &opts_path, false)?;
        let rss_after_load = Self::current_rss();
        let rss_delta = match (rss_before_load, rss_after_load) {
            (Some(before), Some(after)) => {
                Some(ByteSize(after.as_u64().saturating_sub(before.as_u64())))
            }
            _ => None,
        };
        match (rss_after_load, rss_delta) {
            (Some(rss_after_load), Some(rss_delta)) => {
                info!(
                    db = %path_db.display(),
                    elapsed = ?started.elapsed(),
                    rss_after_load = %rss_after_load,
                    rss_delta = %rss_delta,
                    "KRAKEN2 database loaded"
                );
            }
            (Some(rss_after_load), None) => {
                info!(
                    db = %path_db.display(),
                    elapsed = ?started.elapsed(),
                    rss_after_load = %rss_after_load,
                    "KRAKEN2 database loaded"
                );
            }
            (None, _) => {
                info!(
                    db = %path_db.display(),
                    elapsed = ?started.elapsed(),
                    "KRAKEN2 database loaded"
                );
                warn!("Could not read current RSS after KRAKEN2 database load");
            }
        }

        Ok(db)
    }

    fn current_rss() -> Option<ByteSize> {
        memory_stats::memory_stats().map(|memory| ByteSize(memory.physical_mem as u64))
    }

    fn kraken_stream_buffer_after_db_load(requested_stream_buffer: ByteSize) -> ByteSize {
        let Some(memory) = memory_stats::memory_stats() else {
            warn!(
                requested_stream_buffer = %requested_stream_buffer,
                "Could not read current memory usage; using requested KRAKEN stream buffer"
            );
            return requested_stream_buffer;
        };

        let current_usage = ByteSize(memory.physical_mem as u64);
        let remaining = requested_stream_buffer
            .as_u64()
            .saturating_sub(current_usage.as_u64())
            .saturating_sub(KRAKEN_MEMORY_HEADROOM.as_u64());
        let adjusted = ByteSize(remaining.max(KRAKEN_MIN_STREAM_BUFFER.as_u64()));
        let adjusted = ByteSize(adjusted.as_u64().min(requested_stream_buffer.as_u64()));

        info!(
            current_physical_memory = %current_usage,
            requested_stream_buffer = %requested_stream_buffer,
            memory_headroom = %KRAKEN_MEMORY_HEADROOM,
            adjusted_stream_buffer = %adjusted,
            "Adjusted KRAKEN stream buffer after database load"
        );

        adjusted
    }
}

#[derive(Args)]
pub struct KrakenMatrixCMD {
    // Input bascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
}
impl KrakenMatrixCMD {
    /// Run the commandline option.
    /// This one takes a KRAKEN output-file, and outputs a taxonomy count matrix
    pub fn try_execute(&mut self) -> Result<()> {
        let params = KrakenMatrix {
            path_tmp: self.path_tmp.clone(),
            path_input: self.path_in.clone(),
            path_output: self.path_out.clone(),
        };

        let _ = KrakenMatrix::run(&Arc::new(params));

        info!("Kraken has finished succesfully");
        Ok(())
    }
}

///
/// KRAKEN count matrix constructor.
///
pub struct KrakenMatrix {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
}
impl KrakenMatrix {
    /// Run the algorithm
    pub fn run(params: &Arc<KrakenMatrix>) -> anyhow::Result<()> {
        //Prepare matrix that we will store into
        let mut mm = SparseMatrixAnnDataBuilder::new();

        //Open input file
        let file_in = File::open(&params.path_input).unwrap();
        let bufreader = BufReader::new(&file_in);

        //Counter for how many times each taxid has been seen for one cell
        let mut taxid_counter = BTreeMap::new();
        //let mut map_unclassified_counter= BTreeMap::new();
        let mut unclassified_counter = 0;

        //Loop through all reads; group by cell
        let mut last_cellid = None;
        for (_index, rline) in bufreader.lines().enumerate() {
            //////////// should be a plain list of features
            if let Ok(line) = rline {
                ////// when is this false??

                //Divide the row
                let mut splitter_line = line.split("\t");
                let is_categorized = splitter_line.next().unwrap();

                //Figure out what cell this is
                let readname = splitter_line.next().unwrap();
                let mut splitter_cellid = readname.split(":");
                let cellid = Some(splitter_cellid.next().unwrap().to_string());

                //If this is a new cell, then store everything we have so far in the count matrix
                if last_cellid != cellid {
                    //Store if there is a previous cell. Could skip this "if", if we read first line before starting. TODO
                    if let Some(last_cellid_s) = last_cellid {
                        let cell_index = mm.get_or_create_cell(last_cellid_s.as_bytes());

                        //Add taxid counts for last cell
                        mm.add_cell_counts_per_feature_index(cell_index, &mut taxid_counter);
                        //mm.add_feature_counts(cell_index, &mut taxid_counter);
                        //map_unclassified_counter.insert(last_cellid_s.clone(), unclassified_counter);
                        mm.add_unclassified(cell_index, unclassified_counter);

                        //Reset counters
                        taxid_counter.clear();
                        unclassified_counter = 0;
                    }
                    //Move to track the next cell
                    last_cellid = cellid;
                }

                if is_categorized == "C" {
                    //Classified read
                    let taxid_s = splitter_line.next().unwrap();
                    let taxid: u32 = taxid_s
                        .parse()
                        .expect(format! {"Failed to parse taxon id: -{}-", line}.as_str());

                    //Count this taxon id. Note, we count to taxonomyID+1 as 0 is also in use (top level)
                    let values = taxid_counter.entry(taxid + 1).or_insert(0);
                    *values += 1;
                } else if is_categorized == "U" {
                    //Unclassified read. Keep track of how many
                    unclassified_counter += 1;
                }
            } else {
                anyhow::bail!("Failed to read one line of input");
            }
        }

        //Need to also add counts for the last cell
        if let Some(last_cellid_s) = last_cellid {
            let cell_index = mm.get_or_create_cell(last_cellid_s.as_bytes());
            mm.add_cell_counts_per_feature_index(cell_index, &mut taxid_counter);

            mm.add_unclassified(cell_index, unclassified_counter);
        }

        //        C       D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129

        //Compress KRAKEN taxonomy to generate normal column names etc; this makes the output more compatible
        //with regular count matrices
        mm.compress_feature_column("taxid_")?;

        //Save the final count matrix
        info!("Storing count table to {}", params.path_output.display());
        let path_tmp = atomic_temp_path(&params.path_output);
        mm.save_to_anndata(&path_tmp)
            .expect("Failed to save to HDF5 file");
        publish_atomic_output(path_tmp, &params.path_output)?;

        Ok(())
    }
}

/*

 Note: column 1 = taxid 0
 rust sprs counts from 0

*/

/*

Example data

C       D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129
C       D2_F5_H7_C10::902        28384   257     0:56 1:11 0:14 28384:9 0:4 A:129
C       D2_F5_H7_C10::902        1783272 257     0:11 2:3 1:26 2:10 1783272:6 0:16 9606:3 0:19 A:129
C       D2_F5_H7_C10::903        2026187 257     0:29 2026187:8 86661:30 2026187:23 86661:4 A:129
C       D2_F5_H7_C10::903        2026187 257     86661:33 2026187:4 86661:5 2026187:23 86661:29 A:129
C       D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       D2_F5_H7_C10::905        1386    257     1386:75 0:19 A:129
C       D2_F5_H7_C10::905        1386    257     0:3 1386:76 0:15 A:129

https://software.cqls.oregonstate.edu/updates/docs/kraken2/MANUAL.html#standard-kraken-output-format

1. "C"/"U": a one letter code indicating that the sequence was either classified or unclassified.
2. The sequence ID, obtained from the FASTA/FASTQ header.
3. The taxonomy ID Kraken 2 used to label the sequence; this is 0 if the sequence is unclassified.
4. The length of the sequence in bp. In the case of paired read data, this will be a string containing the lengths of the two sequences in bp, separated by a pipe character, e.g. "98|94".
5. A space-delimited list indicating the LCA mapping of each k-mer in the sequence(s). For example, "562:13 561:4 A:31 0:1 562:3" would indicate that:

the first 13 k-mers mapped to taxonomy ID #562
the next 4 k-mers mapped to taxonomy ID #561
the next 31 k-mers contained an ambiguous nucleotide
the next k-mer was not in the database
the last 3 k-mers mapped to taxonomy ID #562

Note that paired read data will contain a "|:|" token in this list to indicate the end of one read and the beginning of another.

*/
