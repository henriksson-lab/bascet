use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use bgzip::{write::BGZFMultiThreadWriter, Compression};
use clap::Args;
use crossbeam::channel::{self, Receiver};
use derive_builder::Builder;
use itertools::{izip, Itertools};
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    num::NonZero,
    path::{Path, PathBuf},
    sync::{atomic::AtomicPtr, Arc, LazyLock, Mutex, RwLock},
};

use crate::{
    io::traits::*, log_critical, log_debug, log_info, log_warning, support_which_stream,
    support_which_writer,
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
    pub fn try_execute(&mut self) -> Result<()> {
        let filter = read_filter(self.path_include.as_deref());
        let count_streams = self.path_in.len();
        let count_writers = self.path_out.len();
        let count_threads_per_stream = (self.threads_read / self.path_in.len()).max(1);

        let (vec_coordinator_producers, vec_coordinator_consumers): (
            &'static mut Vec<rtrb::Producer<ShardifyCell>>,
            &'static mut Vec<RwLock<UnsafeSyncConsumer>>,
        ) = {
            let mut producers = Vec::with_capacity(self.path_in.len());
            let consumers: Vec<RwLock<UnsafeSyncConsumer>> = (0..self.path_in.len())
                .map(|_| {
                    let (px, cx) = rtrb::RingBuffer::new(8);
                    producers.push(px);
                    RwLock::new(UnsafeSyncConsumer(cx))
                })
                .collect();

            (
                Box::leak(Box::new(producers)),
                Box::leak(Box::new(consumers)),
            )
        };
        let vec_consumers_ptr: &'static UnsafeSyncConsumerPtr =
            Box::leak(Box::new(UnsafeSyncConsumerPtr {
                ptr: vec_coordinator_consumers.as_mut_ptr(),
                len: vec_coordinator_consumers.len(),
            }));

        let vec_consumers_states = Arc::new(RwLock::new(Vec::with_capacity(count_streams)));
        let mut vec_reader_handles = Vec::with_capacity(count_streams);
        let mut vec_worker_handles = Vec::with_capacity(self.threads_work);
        let mut vec_writer_handles = Vec::with_capacity(count_writers);

        let (stream_tx, stream_rx) = channel::unbounded::<Option<&[u8]>>();

        for (thread_idx, (thread_input, thread_px)) in
            izip!(self.path_in.clone(), vec_coordinator_producers).enumerate()
        {
            let thread_filter = Arc::clone(&filter);
            let thread_expired = Arc::clone(&vec_consumers_states);
            let thread_tx = stream_tx.clone();

            let thread_handle = thread::spawn(move || {
                let thread_input = ShardifyInput::try_from_path(thread_input).unwrap();
                let thread_stream: ShardifyStream<ShardifyCell> =
                    ShardifyStream::try_from_input(thread_input)
                        .unwrap()
                        .set_reader_threads(count_threads_per_stream);
                let thread_px = thread_px;

                for token_cell_result in thread_stream {
                    let token_cell = match token_cell_result {
                        Ok(token_cell) => token_cell,
                        Err(_) => todo!(),
                    };

                    // SAFETY: this is known to be safe for as long as the token itself is valid!
                    // a better rust programmer could probably annotate the lifetimes properly.
                    let cell_id: &'static [u8] = unsafe {
                        std::mem::transmute::<&[u8], &'static [u8]>(token_cell.get_cell().unwrap())
                    };

                    if !thread_filter.contains(cell_id) {
                        drop(token_cell);
                        continue;
                    }

                    let mut token_cell = token_cell;
                    let mut count_spins = 0;
                    loop {
                        match thread_px.push(token_cell) {
                            Ok(()) => break,
                            Err(rtrb::PushError::Full(ret)) => {
                                token_cell = ret;
                                spin_or_park(&mut count_spins, 100);
                            }
                        }
                    }
                    let _ = thread_tx.send(Some(cell_id));
                }
                thread_expired.write().unwrap().push(thread_idx);
                log_info!("Stream finished!");
            });
            vec_reader_handles.push(thread_handle);
        }

        let (write_tx, write_rx) = channel::unbounded::<Option<Vec<ShardifyCell>>>();
        for _ in 0..self.threads_work {
            let thread_tx = write_tx.clone();
            let thread_rx = stream_rx.clone();
            let thread_reemit_tx = stream_tx.clone();
            let thread_expired = Arc::clone(&vec_consumers_states);

            let mut thread_concat = Vec::with_capacity(count_streams);

            let thread_handle = thread::spawn(move || {
                while let Ok(Some(pending_id)) = thread_rx.recv() {
                    log_info!("Recieved token"; "pending" => %String::from_utf8_lossy(pending_id));
                    thread_concat.clear();

                    consumers_exchange_min(pending_id);
                    unsafe {
                        for sweep_idx in 0..vec_consumers_ptr.len {
                            let sweep_consumer = vec_consumers_ptr.get(sweep_idx);
                            let sweep_rlock = sweep_consumer.read().unwrap();
                            match sweep_rlock.peek() {
                                Ok(sweep_token) => match sweep_token.cell.cmp(pending_id) {
                                    std::cmp::Ordering::Greater => {
                                        // some other thread is going to concat instead
                                        // thread_concat.clear();
                                        continue;
                                    }
                                    std::cmp::Ordering::Equal => {
                                        thread_concat.push(sweep_idx);
                                        
                                    }
                                    std::cmp::Ordering::Less => {
                                        // println!("{} < {}", String::from_utf8_lossy(sweep_token.cell), String::from_utf8_lossy(pending_id));
                                        // trigger re-sweep somehow??
                                        // let min = CONSUMERS_MIN_TOKEN.load();
                                        // let slice = min.as_slice();
                                        // let slice: &'static [u8] = unsafe {
                                        //     std::mem::transmute::<&[u8], &'static [u8]>(slice)
                                        // };
                                        // let _ = thread_reemit_tx.send(Some(slice));
                                        thread_concat.clear();
                                        break;
                                    }
                                },
                                // NOTE: if one is empty, not all readers have read a cell yet
                                Err(rtrb::PeekError::Empty) => {
                                    if thread_expired.read().unwrap().contains(&sweep_idx) {
                                        // we expect this to be empty in this case
                                        continue;
                                    } else {
                                        // wait for this channel to fill instead
                                        // log_info!("Waiting for channels to fill");
                                        thread_concat.clear();
                                        break;
                                    }
                                }
                            };
                        }

                        if thread_concat.is_empty() {
                            continue;
                        }

                        let mut thread_concat_cell = Vec::with_capacity(thread_concat.len());
                        for pop_idx in &thread_concat {
                            let pop_consumer = vec_consumers_ptr.get(*pop_idx);
                            let mut pop_wlock = pop_consumer.write().unwrap();
                            let cell = pop_wlock.pop().unwrap();
                            thread_concat_cell.push(cell);

                            // reset min
                            consumers_exchange_min(&[]);
                            drop(pop_wlock);
                        }
                        let _ = thread_tx.send(Some(thread_concat_cell));
                    }
                }
                log_info!("Worker finished!");
            });
            vec_worker_handles.push(thread_handle);
        }

        for thread_output in self.path_out.clone() {
            let thread_rx = write_rx.clone();
            let thread_output = ShardifyOutput::try_from_path(&thread_output).unwrap();
            let thread_file = std::fs::File::create(thread_output.path()).unwrap();
            let thread_buf_writer = BufWriter::new(thread_file);
            let thread_bgzf_writer =
                BGZFMultiThreadWriter::new(thread_buf_writer, Compression::fast());

            let mut thread_shardify_writer: ShardifyWriter<BGZFMultiThreadWriter<BufWriter<File>>> =
                ShardifyWriter::try_from_output(thread_output)
                    .unwrap()
                    .set_writer(thread_bgzf_writer);

            let thread_handle = thread::spawn(move || {
                while let Ok(Some(vec_cells)) = thread_rx.recv() {
                    log_info!("Writing"; "cell" => %String::from_utf8_lossy(vec_cells[0].cell), "open" => thread_rx.len());
                    for cell in &vec_cells {
                        let _ = thread_shardify_writer.write_cell(cell);
                    }
                }
                log_info!("Writer finished!");
                let _ = thread_shardify_writer.get_writer().unwrap().flush();
            });
            vec_writer_handles.push(thread_handle);
        }

        for handle in vec_reader_handles {
            handle.join().expect("Stream thread panicked");
        }
        for _ in 0..self.threads_work {
            let _ = stream_tx.send(None);
        }
        for handle in vec_worker_handles {
            handle.join().expect("Worker thread panicked");
        }
        for _ in 0..self.path_out.len() {
            let _ = write_tx.send(None);
        }
        for handle in vec_writer_handles {
            handle.join().expect("Writer thread panicked");
        }
        Ok(())
    }
}

static CONSUMERS_MIN_TOKEN: LazyLock<ArcSwap<Vec<u8>>> =
    LazyLock::new(|| ArcSwap::from_pointee(Vec::new()));

#[inline(always)]
fn consumers_exchange_min(token_incoming: &[u8]) {
    let mut count_spins = 0;
    loop {
        let current = CONSUMERS_MIN_TOKEN.load();

        let update =
            token_incoming < current.as_slice() || current.is_empty() || token_incoming.is_empty();
        if !update {
            return;
        }

        let new = Arc::new(token_incoming.to_vec());
        let old = CONSUMERS_MIN_TOKEN.compare_and_swap(&current, new);
        if Arc::ptr_eq(&old, &current) {
            return;
        }

        spin_or_park(&mut count_spins, 100);
    }
}
#[inline(always)]
pub fn spin_or_park(spin_counter: &mut usize, max_spins: usize) {
    if *spin_counter < max_spins {
        *spin_counter += 1;
        std::hint::spin_loop();
    } else {
        // yield CPU for a few us
        thread::park_timeout(std::time::Duration::from_micros(50));
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
fn read_filter(input: Option<&Path>) -> ShardifyFilter {
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

struct UnsafeSyncConsumer(rtrb::Consumer<ShardifyCell>);
unsafe impl Sync for UnsafeSyncConsumer {}
unsafe impl Send for UnsafeSyncConsumer {}

impl UnsafeSyncConsumer {
    unsafe fn peek(&self) -> Result<&ShardifyCell, rtrb::PeekError> {
        self.0.peek()
    }

    unsafe fn pop(&mut self) -> Result<ShardifyCell, rtrb::PopError> {
        self.0.pop()
    }
}

#[derive(Clone)]
struct UnsafeSyncConsumerPtr {
    ptr: *mut RwLock<UnsafeSyncConsumer>,
    len: usize,
}

unsafe impl Send for UnsafeSyncConsumerPtr {}
unsafe impl Sync for UnsafeSyncConsumerPtr {}

impl UnsafeSyncConsumerPtr {
    unsafe fn get(&self, index: usize) -> &RwLock<UnsafeSyncConsumer> {
        debug_assert!(index < self.len);
        &*self.ptr.add(index)
    }
}
