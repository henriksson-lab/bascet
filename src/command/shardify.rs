use anyhow::Result;
use bgzip::{write::BGZFMultiThreadWriter, Compression};
use clap::Args;
use crossbeam::channel;
use itertools::izip;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use crate::{
    common::{self, spin_or_park},
    io::traits::*,
    log_critical, log_info, log_warning, support_which_stream, support_which_writer,
};

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
        let count_threads_per_stream = (self.threads_read / count_streams).max(1);

        let (vec_coordinator_producers, vec_coordinator_consumers): (
            &'static mut Vec<rtrb::Producer<ShardifyCell>>,
            &'static mut Vec<UnsafeSyncConsumer>,
        ) = {
            let mut producers = Vec::with_capacity(count_streams);
            let consumers: Vec<UnsafeSyncConsumer> = (0..count_streams)
                .map(|_| {
                    let (px, cx) = rtrb::RingBuffer::new(32);
                    producers.push(px);
                    UnsafeSyncConsumer(cx)
                })
                .collect();

            (
                Box::leak(Box::new(producers)),
                Box::leak(Box::new(consumers)),
            )
        };
        let vec_consumers_states = Arc::new(RwLock::new(Vec::with_capacity(count_streams)));
        let mut vec_reader_handles = Vec::with_capacity(count_streams);
        // let mut vec_worker_handles = Vec::with_capacity(self.threads_work);
        let mut vec_writer_handles = Vec::with_capacity(count_writers);

        // bounds given by rtrb, this is only a notifier
        let (stream_tx, stream_rx) = channel::unbounded::<Option<()>>();
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
                    // log_info!("Filtering"; "cell" => %String::from_utf8_lossy(token_cell.cell));
                    if !thread_filter.contains(token_cell.cell) {
                        drop(token_cell);
                        continue;
                    }
                    // NOTE: this gets a valid cell but between here and pop/peeking from rtrb,
                    // data corruption occurs
                    // println!("");
                    // log_info!("Passed Filter!"; "cell" => %String::from_utf8_lossy(token_cell.cell));

                    let mut token_cell = token_cell;
                    let mut rtrb_count_spins = 0;
                    loop {
                        match thread_px.push(token_cell) {
                            Ok(()) => break,
                            Err(rtrb::PushError::Full(ret)) => {
                                token_cell = ret;
                                spin_or_park(&mut rtrb_count_spins, 1000);
                            }
                        }
                    }
                    let _ = thread_tx.send(Some(()));
                }
                thread_expired.write().unwrap().push(thread_idx);
                let _ = thread_tx.send(None);
                log_info!("Stream finished!");
            });
            vec_reader_handles.push(thread_handle);
        }

        let (write_tx, write_rx) = channel::bounded::<Option<Arc<RwLock<Vec<ShardifyCell>>>>>(16);
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
                    for cell in &*vec_cells.read().unwrap() {
                        log_info!("Writing"; "cell" => %String::from_utf8_lossy(cell.cell));
                        let _ = thread_shardify_writer.write_cell(cell);
                    }
                }
                log_info!("Writer finished!");
                let _ = thread_shardify_writer.get_writer().unwrap().flush();
            });
            vec_writer_handles.push(thread_handle);
        }

        let mut coordinator_count_streams_finished = 0;
        let mut coordinator_count_spins = 0;
        let mut coordinator_all_ready;
        let mut coordinator_min_cell: Option<&[u8]>;
        let mut coordinator_vec_take: Vec<usize> = Vec::with_capacity(count_streams);
        let mut coordinator_vec_send: Vec<ShardifyCell> = Vec::with_capacity(count_streams); // Local vec

        loop {
            match stream_rx.try_recv() {
                Ok(Some(())) => {}
                Ok(None) => {
                    coordinator_count_streams_finished += 1;
                    println!("incr {coordinator_count_streams_finished}/{count_streams}");
                    if coordinator_count_streams_finished == count_streams {
                        println!("Closing reciever");
                        break;
                    }
                }
                Err(channel::TryRecvError::Empty) => {
                    spin_or_park(&mut coordinator_count_spins, 100);
                    continue;
                }
                Err(_) => break,
            };

            // log_info!("Received token"; "pending" => %String::from_utf8_lossy(&pending_token), "open" => %stream_rx.len());
            coordinator_count_spins = 0;
            coordinator_all_ready = true;
            coordinator_min_cell = None;
            coordinator_vec_take.clear();
            coordinator_vec_send.clear();

            for (sweep_consumer_idx, sweep_consumer) in vec_coordinator_consumers.iter().enumerate()
            {
                // Skip streams marked done/expired
                if vec_consumers_states
                    .read()
                    .unwrap()
                    .contains(&sweep_consumer_idx)
                {
                    continue;
                }

                let sweep_token = match unsafe { sweep_consumer.peek() } {
                    Ok(token) => token,
                    Err(rtrb::PeekError::Empty) => {
                        // Stream not ready
                        coordinator_all_ready = false;
                        break;
                    }
                };

                match coordinator_min_cell {
                    None => {
                        coordinator_min_cell = Some(sweep_token.cell);
                        coordinator_vec_take.clear();
                        coordinator_vec_take.push(sweep_consumer_idx);
                    }
                    Some(cmc) if sweep_token.cell < cmc => {
                        coordinator_min_cell = Some(sweep_token.cell);
                        coordinator_vec_take.clear();
                        coordinator_vec_take.push(sweep_consumer_idx);
                    }
                    Some(cmc) if sweep_token.cell == cmc => {
                        coordinator_vec_take.push(sweep_consumer_idx);
                    }
                    _ => {}
                }
            }

            if !coordinator_all_ready {
                continue;
            }

            for take_idx in &coordinator_vec_take {
                let take_consumer = &mut vec_coordinator_consumers[*take_idx];
                match unsafe { take_consumer.pop() } {
                    Ok(take_cell) => coordinator_vec_send.push(take_cell),
                    Err(_) => unreachable!("Token disappeared between peek and pop"),
                }
            }

            let _ = write_tx.send(Some(Arc::new(RwLock::new(std::mem::take(
                &mut coordinator_vec_send,
            )))));
        }

        for handle in vec_reader_handles {
            handle.join().expect("Stream thread panicked");
        }
        log_info!("Stream handles closed");
        for _ in 0..count_writers {
            let _ = write_tx.send(None);
        }
        for handle in vec_writer_handles {
            handle.join().expect("Writer thread panicked");
        }
        log_info!("Write handles closed");
        Ok(())
    }
}

struct ShardifyCell {
    cell: &'static [u8],
    reads: Vec<(&'static [u8], &'static [u8])>,
    qualities: Vec<(&'static [u8], &'static [u8])>,
    umis: Vec<&'static [u8]>,

    _page_refs: smallvec::SmallVec<[common::UnsafeMutPtr<common::PageBuffer>; 2]>,
    _owned: Vec<Vec<u8>>,
}
impl Drop for ShardifyCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for page_ptr in &self._page_refs {
                (*page_ptr.mut_ptr()).dec_ref();
            }
        }
    }
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

    page_refs: smallvec::SmallVec<[common::UnsafeMutPtr<common::PageBuffer>; 2]>,
    owned: Vec<Vec<u8>>,
}

impl ShardifyCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            reads: Vec::new(),
            qualities: Vec::new(),
            umis: Vec::new(),

            page_refs: smallvec::SmallVec::new(),
            owned: Vec::new(),
        }
    }
}

impl BascetCellBuilder for ShardifyCellBuilder {
    type Token = ShardifyCell;

    #[inline(always)]
    fn add_page_ref(mut self, page_ptr: common::UnsafeMutPtr<common::PageBuffer>) -> Self {
        unsafe {
            (*page_ptr.mut_ptr()).inc_ref();
        }
        self.page_refs.push(page_ptr);
        self
    }

    // HACK: these are hacks since this type of stream token uses slices. so we take the underlying owned vec
    // and treat it like an otherwise Arc'd underlying vec and then pretend it is a slice.
    fn add_cell_id_owned(mut self, id: Vec<u8>) -> Self {
        self.owned.push(id);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        // and the CountsketchCell holds the _owned field to maintain this invariant
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.cell = Some(slice_with_lifetime);
        self
    }

    #[inline(always)]
    fn add_sequence_owned(mut self, seq: Vec<u8>) -> Self {
        self.owned.push(seq);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.reads.push((slice_with_lifetime, &[]));
        self
    }

    #[inline(always)]
    fn add_quality_owned(mut self, qual: Vec<u8>) -> Self {
        self.owned.push(qual);
        let slice = self.owned.last().unwrap().as_slice();
        // SAFETY: The slice is valid for the static lifetime as long as self.owned keeps the Vec alive
        let slice_with_lifetime: &'static [u8] = unsafe { std::mem::transmute(slice) };
        self.qualities.push((slice_with_lifetime, &[]));
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by Arcs. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_rp_slice(mut self, r1: &'static [u8], r2: &'static [u8]) -> Self {
        self.reads.push((r1, r2));
        self
    }
    #[inline(always)]
    fn add_qp_slice(mut self, q1: &'static [u8], q2: &'static [u8]) -> Self {
        self.qualities.push((q1, q2));
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        self.reads.push((slice, &[]));
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &'static [u8]) -> Self {
        self.qualities.push((slice, &[]));
        self
    }

    fn add_umi_slice(mut self, umi: &'static [u8]) -> Self {
        self.umis.push(umi);
        self
    }

    #[inline(always)]
    fn build(self) -> ShardifyCell {
        ShardifyCell {
            cell: self.cell.expect("cell is required"),
            reads: self.reads,
            qualities: self.qualities,
            umis: self.umis,

            _page_refs: self.page_refs,
            _owned: self.owned,
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
