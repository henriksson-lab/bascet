use fs2::FileExt;
use memmap2::MmapOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::{
    cmp::min,
    fs::File,
    sync::Arc,
    usize,
};

use super::kmer_codec::KMERandCount;
use super::{BoundedHeap, BoundedMinHeap, KMERCodec};

use threadpool::ThreadPool;


// \t[0-9]{10}\n
// (4 294 967 296) is max value for kmer counts, thats 10 digits :)
pub const KMC_COUNTER_MAX_DIGITS: usize = 12;
pub const HUGE_PAGE_SIZE: usize = 2048 * 1024;




pub struct KmerCounterParams {
    pub path_kmcdump: std::path::PathBuf,
    pub kmer_size: usize,
    pub features_nmin: usize,
}


pub struct KmerCounter {}
impl KmerCounter {

    pub fn extract_kmcdump_parallel(
        params: &KmerCounterParams,
        n_workers: usize
    ) -> anyhow::Result<BoundedMinHeap<KMERandCount>> {
        //Spinning up workers for every new file can be pricey... could put this in params or something, to hide it. future work

        let params= Arc::new(params);


        //Create all thread states
        let threads_buffer_size = (HUGE_PAGE_SIZE / n_workers) - (params.kmer_size + KMC_COUNTER_MAX_DIGITS);

        //Decide on KMER encoding
        let codec = KMERCodec::new(params.kmer_size);

        //Set up memory-mapped reading of file
        let file = File::open(&params.path_kmcdump).unwrap();
        let lock = file.lock_exclusive();
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.unwrap());

        //Set up a channel to send regions for reading to worker threads
        let (tx, rx) = crossbeam::channel::bounded(n_workers*3);
        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

        //Set up a channel to gather minheaps at end
        let (tx_minheap, rx_minheap) = crossbeam::channel::bounded(n_workers);
        let (tx_minheap, rx_minheap) = (Arc::new(tx_minheap), Arc::new(rx_minheap));
        
        //Start all workers
        let thread_pool = ThreadPool::new(n_workers);
        for _tidx in 0..n_workers {
            let rx = Arc::clone(&rx);
            let tx_minheap = Arc::clone(&tx_minheap);
            let mmap = Arc::clone(&mmap);
            let ovlp_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
            let kmer_size = params.kmer_size;
            let features_nmin = params.features_nmin;
            thread_pool.execute(move || {
                let mut min_heap = BoundedMinHeap::with_capacity(features_nmin);
                while let Ok(Some((start, end))) = rx.recv() {
                    let chunk = &mmap[start..end];
                    process_chunk_to_minheap(
                        &chunk,
                        &mut min_heap,
                        codec,
                        kmer_size,
                        ovlp_size,
                    );
                }
                tx_minheap.send(Arc::new(min_heap)).unwrap();
            });
        }

        //In main thread, instruct workers where to read
        let overlap_window_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
        let n_chunks = (mmap.len() + threads_buffer_size - 1) / threads_buffer_size;
        for i in 0..n_chunks {
            let raw_start = i * threads_buffer_size;
            let raw_end = min(
                raw_start + threads_buffer_size + overlap_window_size,
                mmap.len(),
            );
            let valid_start = find_chunk_start(&mmap[raw_start..], raw_start, overlap_window_size);
            let valid_end = find_chunk_end(&mmap[..raw_end], raw_end, overlap_window_size);
            tx.send(Some((valid_start, valid_end))).unwrap();
        }

        //Shut down all workers and wait for them to finish
        for _ in 0..n_workers {
            tx.send(None).unwrap();
        }
        thread_pool.join();

        //Merge all minheaps
        let mut final_min_heap = BoundedMinHeap::with_capacity(params.features_nmin);
        for _s in 0..n_workers {
            let mh = rx_minheap.recv().unwrap();
            for d in mh.iter() {
                _ = final_min_heap.push(d.clone());
            }
        }

        //Explicitly dropping file lock because i am paranoid it will not unlock otherwise
        drop(lock);

        Ok(final_min_heap)
    }






    pub fn detect_kmcdump_kmer_size(p: &PathBuf) -> anyhow::Result<usize> {
        let f = File::open(p).expect("Could not open file");
        let mut reader = BufReader::new(f);
        let mut buf = Vec::new();
        reader.read_until(b'\t', &mut buf).expect("Could not parse first KMER from KMC dump file"); ////// Hopefully ok!
        Ok(buf.len())
    }




    pub fn extract_kmcdump_single_thread (
        params: &Arc<KmerCounterParams>
    ) -> anyhow::Result<BoundedMinHeap<KMERandCount>> {

        let features_nmin = params.features_nmin;
        let n_workers = 1;
        let ovlp_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
        let kmer_size = params.kmer_size;

        //Create all thread states
        let threads_buffer_size = (HUGE_PAGE_SIZE / n_workers) - (params.kmer_size + KMC_COUNTER_MAX_DIGITS);

        //Decide on KMER encoding
        let codec = KMERCodec::new(params.kmer_size);

        //Set up memory-mapped reading of file
        let file = File::open(&params.path_kmcdump).unwrap();
        let lock = file.lock_exclusive();
        let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.unwrap());

        let mut min_heap = BoundedMinHeap::with_capacity(features_nmin);

        //In main thread, instruct workers where to read
        let overlap_window_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
        let n_chunks = (mmap.len() + threads_buffer_size - 1) / threads_buffer_size;
        for i in 0..n_chunks {
            let raw_start = i * threads_buffer_size;
            let raw_end = min(
                raw_start + threads_buffer_size + overlap_window_size,
                mmap.len(),
            );
            let valid_start = find_chunk_start(&mmap[raw_start..], raw_start, overlap_window_size);
            let valid_end = find_chunk_end(&mmap[..raw_end], raw_end, overlap_window_size);

            let chunk = &mmap[valid_start..valid_end];
            process_chunk_to_minheap(
                &chunk,
                &mut min_heap,
                codec,
                kmer_size,
                ovlp_size,
            );
        }


        //Explicitly dropping file lock because i am paranoid it will not unlock otherwise
        drop(lock);

        Ok(min_heap)
    }




    
    pub fn store_minhash(
        minhash: &BoundedMinHeap<KMERandCount>,
        p: &PathBuf
    ){
        //Open file for writing
        let f=File::create(p).expect("Could not open file for writing");
        let mut bw=BufWriter::new(f);

        //Write kmer & count. Hopefully this iterates from min to max
        for h in minhash.iter() {
            writeln!(bw, "{}\t{}", &h.kmer, &h.count).unwrap();
        }
    }

}
















#[inline(always)]
fn find_chunk_start(chunk: &[u8], raw_start: usize, ovlp_size: usize) -> usize {
    for i in 0..min(ovlp_size, chunk.len()) {
        if chunk[i] == b'\n' {
            return raw_start + i + 1;
        }
    }
    raw_start
}

#[inline(always)]
fn find_chunk_end(chunk: &[u8], raw_end: usize, ovlp_size: usize) -> usize {
    let window_size = min(ovlp_size, chunk.len());
    for i in (chunk.len() - window_size..chunk.len()).rev() {
        if chunk[i] == b'\n' {
            return min(i + 1, raw_end);
        }
    }
    raw_end
}



///////// Parse the count of kmers from KMC database. Counting has already been done
#[inline(always)]
unsafe fn parse_count_u32(bytes: &[u8]) -> u32 {
    // Fast path for single digit, most common case
    if bytes.len() == 1 {
        return (bytes[0] - b'0') as u32;
    }

    // Fast path for two digits, second most common case
    if bytes.len() == 2 {
        return ((bytes[0] - b'0') * 10 + (bytes[1] - b'0')) as u32;
    }

    // LUT for two-digit numbers
    const LOOKUP: [u32; 100] = {
        let mut table = [0u32; 100];
        let mut i = 0;
        while i < 100 {
            table[i] = (i / 10 * 10 + i % 10) as u32;
            i += 1;
        }
        table
    };

    let chunks = bytes.chunks_exact(2);
    let remainder = chunks.remainder();

    let mut result = 0u32;
    for chunk in chunks {
        let idx = ((chunk[0] - b'0') * 10 + (chunk[1] - b'0')) as usize;
        result = result.wrapping_mul(100) + LOOKUP[idx];
    }

    // Handle last digit if present
    if let Some(&d) = remainder.first() {
        result = result.wrapping_mul(10) + (d - b'0') as u32;
    }

    result
}


////////// For a chunk of data, extract KMERs
#[inline(always)]
fn process_chunk_to_minheap(
    chunk: &[u8],
    min_heap: &mut BoundedMinHeap<KMERandCount>,
    codec: KMERCodec,
    kmer_size: usize,
    ovlp_size: usize,
) {
    let chunk_length = chunk.len();
    let min_read_size = kmer_size + 2; // K + 2 is minimum size for a kmer + count (\t\d)
    let n_max_panes = chunk_length / min_read_size;
    let mut cursor = 0;

    for _ in 0..n_max_panes {
        if cursor >= chunk_length {
            break;
        }

        let pane_start = cursor;
        let remaining = chunk_length - pane_start;

        if remaining < min_read_size {
            break;
        }

        // Find the length of the current pane (up to next newline)
        let mut pane_length = min_read_size;
        for i in pane_length..min(ovlp_size, remaining) {
            if chunk[pane_start + i] == b'\n' {
                pane_length = i;
                break;
            }
        }

        // Extract and encode kmer with its count
        let kmer_end = pane_start + kmer_size;
        let count = unsafe { parse_count_u32(&chunk[kmer_end + 1..pane_start + pane_length]) };

        let encoded = unsafe {
            codec.encode(&chunk[pane_start..kmer_end], count)
        };

        let _ = min_heap.push(encoded);

        cursor += pane_length + 1; // +1 for newline
    }
}
