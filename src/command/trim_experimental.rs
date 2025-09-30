use std::io::BufRead;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use std::{io::Cursor, path::PathBuf};

use anyhow::Result;
use clap::{Args, Subcommand};
use crossbeam::channel::Receiver;
use itertools::{izip, Itertools};

use crate::common::ReadPair;
use crate::log_info;
use crate::{
    barcode::{atrandi_wgs_barcode::AtrandiWGSChemistry, combinatorial_barcode_8bp, Chemistry},
    common,
    io::traits::{BascetCell, BascetCellBuilder, BascetStream},
    log_critical, log_warning, support_which_stream, support_which_writer,
    threading::{self, PeekableReceiver},
};

support_which_stream! {
    TrimExperimentalInput => TrimExperimentalStream<T: BascetCell>
    for formats [fastq_gz]
}
support_which_writer! {
    TrimExperimentalOutput => TrimExperimentalWriter<W: std::io::Write>
    for formats [tirp_bgzf]
}

#[derive(Args)]
pub struct TrimExperimentalCMD {
    #[command(subcommand)]
    pub chemistry: TrimChemistry,

    // Input R1 files
    #[arg(short = '1', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub paths_r1: Vec<PathBuf>,

    // Input R2 files
    #[arg(short = '2', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub paths_r2: Vec<PathBuf>,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = 16)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = 8)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = 8)]
    threads_work: usize,

    // Stream buffer configuration
    #[arg(long = "buffer-size", value_parser = clap::value_parser!(usize), default_value_t = 8096)]
    pub buffer_size_mb: usize,
    #[arg(long = "page-size", value_parser = clap::value_parser!(usize), default_value_t = 8)]
    pub page_size_mb: usize,
}

#[derive(Clone, Copy, Subcommand)]
pub enum TrimChemistry {
    Atrandi(AtrandiArgs),
}

#[derive(Clone, Copy, Args)]
pub struct AtrandiArgs {}

#[derive(Clone)]
pub struct AtrandiExpChemistry {
    barcode: combinatorial_barcode_8bp::CombinatorialBarcode8bp,
}
impl AtrandiExpChemistry {
    fn detect_barcode_and_trim(
        &mut self,
        r1_seq: &'static [u8],
        r1_qual: &'static [u8],
        r2_seq: &'static [u8],
        r2_qual: &'static [u8],
    ) -> (bool, String, ReadPair) {
        //Detect barcode, which for parse is in R2
        let total_distance_cutoff = 4;
        let part_distance_cutoff = 1;

        let (isok, bc, _match_score) =
            self.barcode
                .detect_barcode(r2_seq, true, total_distance_cutoff, part_distance_cutoff);

        if isok {
            //R2 need to have the first part with barcodes removed. Figure out total size!
            //TODO search for the truseq adapter that may appear toward the end
            let r2_from = self.barcode.trim_bcread_len;
            let r2_to = r2_seq.len();

            //Get UMI position
            let umi_from = self.barcode.umi_from;
            let umi_to = self.barcode.umi_to;

            (
                true,
                bc,
                ReadPair {
                    r1: &r1_seq,
                    r2: &r2_seq[r2_from..r2_to],
                    q1: &r1_qual,
                    q2: &r2_qual[r2_from..r2_to],
                    umi: &r2_seq[umi_from..umi_to],
                },
            )
        } else {
            //Just return the sequence as-is
            (
                false,
                "".to_string(),
                ReadPair {
                    r1: &r1_seq,
                    r2: &r2_seq,
                    q1: &r1_qual,
                    q2: &r2_qual,
                    umi: &[],
                },
            )
        }
    }
}
impl TrimExperimentalCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let paths_r1 = &self.paths_r1;
        let paths_r2 = &self.paths_r2;
        let buffer_size_bytes = self.buffer_size_mb * 1024 * 1024;
        let page_size_bytes = self.page_size_mb * 1024 * 1024;
        let threads_stream = self.threads_read / 2;
        let num_pages = buffer_size_bytes / page_size_bytes;

        for (path_r1, path_r2) in izip!(paths_r1, paths_r2) {
            let mut chemistry = AtrandiExpChemistry {
                barcode: combinatorial_barcode_8bp::CombinatorialBarcode8bp::new(),
            };

            let reader = Cursor::new(include_bytes!("../barcode/atrandi_barcodes.tsv"));
            for (index, line) in reader.lines().enumerate() {
                if index == 0 {
                    continue;
                }

                let line = line?;
                let parts: Vec<&str> = line.split('\t').collect();
                chemistry.barcode.add_bc(parts[1], parts[0], parts[2]);
            }
            chemistry.barcode.pools[3].quick_testpos = (8 + 4) * 0;
            chemistry.barcode.pools[3].all_test_pos = vec![0, 1];

            chemistry.barcode.pools[2].quick_testpos = (8 + 4) * 1;
            chemistry.barcode.pools[2].all_test_pos = vec![0, 1];

            chemistry.barcode.pools[1].quick_testpos = (8 + 4) * 2;
            chemistry.barcode.pools[1].all_test_pos = vec![0, 1];

            chemistry.barcode.pools[0].quick_testpos = (8 + 4) * 3;
            chemistry.barcode.pools[0].all_test_pos = vec![0, 1];

            // prepare chemistry using r2
            let input = TrimExperimentalInput::try_from_path(&path_r2).unwrap();
            let mut stream =
                TrimExperimentalStream::<TrimExperimentalCell>::try_from_input(input).unwrap();
            stream.set_reader_threads(threads_stream);
            stream.set_pagebuffer_config(num_pages, page_size_bytes);

            let mut buffer = Vec::with_capacity(1000);
            for token in stream {
                let token = token.unwrap();
                buffer.push(token.read.to_vec());

                if buffer.len() >= 1000 {
                    break;
                }
            }

            let (r1_tx, r1_rx) = crossbeam::channel::unbounded();
            let path_r1 = path_r1.clone();
            let r1_handle = std::thread::spawn(move || {
                let input = TrimExperimentalInput::try_from_path(&path_r1).unwrap();
                let mut stream =
                    TrimExperimentalStream::<TrimExperimentalCell>::try_from_input(input).unwrap();
                stream.set_reader_threads(threads_stream);
                stream.set_pagebuffer_config(num_pages, page_size_bytes);

                for token in stream {
                    let token = token.unwrap();
                    let _ = r1_tx.send(token);
                }
            });

            let (r2_tx, r2_rx) = crossbeam::channel::unbounded();
            let path_r2 = path_r2.clone();
            let r2_handle = std::thread::spawn(move || {
                let input = TrimExperimentalInput::try_from_path(&path_r2).unwrap();
                let mut stream =
                    TrimExperimentalStream::<TrimExperimentalCell>::try_from_input(input).unwrap();
                stream.set_reader_threads(threads_stream);
                stream.set_pagebuffer_config(num_pages, page_size_bytes);

                for token in stream {
                    let token = token.unwrap();
                    let _ = r2_tx.send(token);
                }
            });

            let (rp_tx, rp_rx) = crossbeam::channel::unbounded();
            let success_counter = Arc::new(AtomicUsize::new(0));
            let total_counter = Arc::new(AtomicUsize::new(0));

            for _worker_thread_index in 0..self.threads_work {
                let rp_rx: Receiver<(TrimExperimentalCell, TrimExperimentalCell)> = rp_rx.clone();
                let mut chemistry = chemistry.clone();
                let success_counter = Arc::clone(&success_counter);
                let total_counter = Arc::clone(&total_counter);

                let _worker_handle = std::thread::spawn(move || {
                    while let Ok((r1, r2)) = rp_rx.recv() {
                        let (ok, id, _rp) = chemistry
                            .detect_barcode_and_trim(r1.read, r1.quality, r2.read, r2.quality);

                        let total = total_counter.fetch_add(1, Ordering::Relaxed);

                        if ok {
                            let success = success_counter.fetch_add(1, Ordering::Relaxed);
                            if success % 1_000_000 == 0 {
                                log_info!(
                                    "{:.2}M/{:.2}M reads successfully debarcoded: current BC: {:?}",
                                    success as f64 / 1_000_000.0,
                                    total as f64 / 1_000_000.0,
                                    id
                                );
                            }
                        }
                    }
                });
            }
            
            while let (Ok(r1), Ok(r2)) = (r1_rx.recv(), r2_rx.recv()) {
                let _ = rp_tx.send((r1, r2));
            }
        }

        Ok(())
    }
}

struct TrimExperimentalCell {
    cell: &'static [u8],
    read: &'static [u8],
    quality: &'static [u8],

    _page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 2]>,
    _owned: Vec<Vec<u8>>,
}

impl std::fmt::Debug for TrimExperimentalCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrimExperimentalCell")
            .field("cell", &String::from_utf8_lossy(self.cell))
            .field("read", &String::from_utf8_lossy(self.read))
            .field("quality", &String::from_utf8_lossy(self.quality))
            .field("_page_refs", &format!("{} refs", self._page_refs.len()))
            .field("_owned", &format!("{} owned", self._owned.len()))
            .finish()
    }
}

impl Drop for TrimExperimentalCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for page_ptr in &self._page_refs {
                (***page_ptr).dec_ref();
            }
        }
    }
}

impl BascetCell for TrimExperimentalCell {
    type Builder = TrimExperimentalCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}
struct TrimExperimentalCellBuilder {
    cell: Option<&'static [u8]>,
    read: Option<&'static [u8]>,
    quality: Option<&'static [u8]>,

    page_refs: smallvec::SmallVec<[threading::UnsafePtr<common::PageBuffer<u8>>; 2]>,
    owned: Vec<Vec<u8>>,
}

impl TrimExperimentalCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            read: None,
            quality: None,

            page_refs: smallvec::SmallVec::new(),
            owned: Vec::new(),
        }
    }
}

impl BascetCellBuilder for TrimExperimentalCellBuilder {
    type Token = TrimExperimentalCell;

    #[inline(always)]
    fn add_page_ref(mut self, page_ptr: threading::UnsafePtr<common::PageBuffer<u8>>) -> Self {
        unsafe {
            (**page_ptr).inc_ref();
        }
        self.page_refs.push(page_ptr);
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by Arcs. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        if self.cell.is_some() {
            log_warning!("Cell ID already set, overwriting");
        }
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        if self.read.is_some() {
            log_warning!("Sequence already set, overwriting");
        }
        self.read = Some(slice);
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &'static [u8]) -> Self {
        if self.quality.is_some() {
            log_warning!("Quality already set, overwriting");
        }
        self.quality = Some(slice);
        self
    }

    #[inline(always)]
    fn build(self) -> TrimExperimentalCell {
        TrimExperimentalCell {
            cell: self.cell.expect("cell is required"),
            read: self.read.expect("read is required"),
            quality: self.quality.expect("quality is required"),

            _page_refs: self.page_refs,
            _owned: self.owned,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for TrimExperimentalStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
