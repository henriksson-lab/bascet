use anyhow::{anyhow, bail, Result};
use bgzip::{write::BGZFMultiThreadWriter, Compression};
use clap::Args;
use crossbeam::channel::{self, Receiver};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    num::NonZero,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    io::traits::*, log_critical, log_info, log_warning, support_which_stream, support_which_writer,
};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;

pub const DEFAULT_THREADS_READ: usize = 10;
pub const DEFAULT_THREADS_WORK: usize = 2;
pub const DEFAULT_THREADS_TOTAL: usize = 12;

support_which_stream! {
    ShardifyInput => ShardifyStream<T: BascetCell>
    for formats [tirp]
}
support_which_writer! {
    ShardifyOutput => ShardifyWriter<W: std::io::Write>
    for formats [tirp]
}
/// Commandline option: Take parsed reads and organize them as shards
#[derive(Args)]
pub struct ShardifyCMD {
    // Input bascets (comma separated; ok with PathBuf???)
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    // File with a list of cells to include
    #[arg(long = "include")]
    pub path_include: Option<PathBuf>,

    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_TOTAL)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_READ)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = DEFAULT_THREADS_WORK)]
    threads_work: usize,
}

impl ShardifyCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        let filter = setup_filter(self.path_include.as_deref());
        let rx = setup_streams(
            self.path_in.iter().map(|p| p.as_path()).collect_vec(),
            (self.threads_read / self.path_in.len()).max(1),
            filter,
        );
        Ok(())
    }
}

#[derive(Debug)]
struct ShardifyCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    _underlying: Vec<Arc<Vec<u8>>>,
}

impl BascetCell for ShardifyCell {
    type Builder = ShardifyCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }

    fn get_cell(&self) -> Option<&[u8]> {
        Some(self.cell)
    }

    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.reads)
    }

    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        Some(&self.qualities)
    }

    fn get_umis(&self) -> Option<&[&[u8]]> {
        Some(&self.umis)
    }
}
struct ShardifyCellBuilder {
    cell: Option<&'static [u8]>,
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,
    underlying: Vec<Arc<Vec<u8>>>,
}

impl ShardifyCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            qualities: Vec::new(),
            umis: Vec::new(),
            underlying: Vec::new(),
        }
    }
}

impl BascetCellBuilder for ShardifyCellBuilder {
    type Token = ShardifyCell;

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    #[inline(always)]
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        let aid = Arc::new(id);
        self = self.add_underlying(aid.clone()).add_cell_id_slice(&aid);
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        let aseq = Arc::new(seq);
        self = self.add_underlying(aseq.clone()).add_cell_id_slice(&aseq);
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by Arcs. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(static_slice);
        self
    }

    #[inline(always)]
    fn add_rp_slice(mut self, r1: &[u8], r2: &[u8]) -> Self {
        let r1_static_slice: &'static [u8] = unsafe { std::mem::transmute(r1) };
        let r2_static_slice: &'static [u8] = unsafe { std::mem::transmute(r2) };
        self.reads.push((r1_static_slice, r2_static_slice));
        self
    }
    #[inline(always)]
    fn add_qp_slice(mut self, q1: &[u8], q2: &[u8]) -> Self {
        let q1_static_slice: &'static [u8] = unsafe { std::mem::transmute(q1) };
        let q2_static_slice: &'static [u8] = unsafe { std::mem::transmute(q2) };
        self.qualities.push((q1_static_slice, q2_static_slice));
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push((static_slice, &[]));
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.qualities.push((static_slice, &[]));
        self
    }

    fn add_umi_slice(mut self, umi: &[u8]) -> Self {
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(umi) };
        self.umis.push(static_slice);
        self
    }

    #[inline(always)]
    fn add_underlying(mut self, buffer: Arc<Vec<u8>>) -> Self {
        self.underlying.push(buffer);
        self
    }

    #[inline(always)]
    fn build(self) -> ShardifyCell {
        ShardifyCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            qualities: self.qualities,
            umis: self.umis,
            _underlying: self.underlying,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for ShardifyStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}

type ShardifyFilter = Arc<gxhash::HashSet<Vec<u8>>>;
fn setup_filter(input: Option<&Path>) -> ShardifyFilter {
    if input == None {
        log_critical!(
            "Empty cell list detected! This configuration may consume massive amounts of computer memory (potentially hundreds of GiB of RAM) and will DUPLICATE the input datasets. This is almost certainly an error. Verify input parameters or provide an explicitly empty collection only if this behavior is understood and intended."
        );
    }
    let input = input.unwrap();
    // GOOD FIRST ISSUE:
    // implement cell list reader around the support macros!
    let file = File::open(input).unwrap();
    let reader = BufReader::new(file);
    let filter = reader
        .lines()
        .map(|l| l.unwrap().into_bytes())
        .collect::<gxhash::HashSet<Vec<u8>>>();

    if filter.is_empty() {
        log_warning!(
            "Empty cell list detected! This configuration may consume massive amounts of computer memory (potentially hundreds of GiB of RAM) and will DUPLICATE the input datasets."
        );
    }
    return Arc::new(filter);
}

const STREAM_COORDINATOR_N_QX: usize = 32;
struct StreamCoordinator {
    rx: crossbeam::channel::Receiver<Option<usize>>,
    qx: smallvec::SmallVec<[rtrb::Consumer<ShardifyCell>; STREAM_COORDINATOR_N_QX]>,
}
impl StreamCoordinator {
    
}
fn spawn_streams_coordinated(
    input: Vec<&Path>,
    threads_per_reader: usize,
    filter: ShardifyFilter,
) -> anyhow::Result<StreamCoordinator> {
    let (tx, rx) = channel::unbounded::<Option<usize>>();
    let queues: smallvec::SmallVec<[rtrb::Consumer<ShardifyCell>; STREAM_COORDINATOR_N_QX]> = smallvec::SmallVec::new();
    for path in input {}

    anyhow::bail!("aa")
}
