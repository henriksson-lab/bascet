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
// (4 294 967 296) is max value for kmer counts, thats 10 digits :)
pub const KMC_COUNTER_MAX_DIGITS: usize = 12;
pub const HUGE_PAGE_SIZE: usize = 2048 * 1024;
const PLUSMIN_LOOKUP: [i64; 2] = [1, -1];


pub struct CountSketch {
    pub sketch: Vec<i64>,
    pub total: i64
}
impl CountSketch {
    pub fn new(size: usize) -> CountSketch {
        CountSketch {
            sketch: vec![0; size],
            total: 0
        }
    }

    #[inline(always)]
    fn to_plusmin_one(kmer: u32) -> i32 {
        1 - ((kmer & 1) << 1) as i32
    }
    
    
    pub fn add(&mut self, kmer: KMERandCount) {

        // https://wangshusen.github.io/code/countsketch.html inspo
        // in R: https://www.rdocumentation.org/packages/aroma.light/versions/3.2.0/topics/wpca
        // https://docs.rs/streaming_algorithms/latest/streaming_algorithms/  mincounthash code
        // python: https://pdsa.readthedocs.io/en/latest/frequency/count_sketch.html
        // MurmurHash3

        let h = KMERCodec::h_hash_for_kmer(kmer.kmer);
        let g = KMERCodec::g_hash_for_kmer(kmer.kmer);

        let sgn = CountSketch::to_plusmin_one(g) as i64;  //using last bit of hash only. sped up a little now :)
        //println!("{} {}", h, sgn);

        let pos = (h as usize) % self.sketch.len();
        self.sketch[pos] += sgn;

        self.total += 1;
    }

} 


pub struct KmerCounter {
    pub path_kmcdump: std::path::PathBuf,
    pub kmer_size: usize,
    pub features_nmin: usize,
}
impl KmerCounter {



    pub fn get_countsketch_fq(
        path_read_r1: std::path::PathBuf,
        path_read_r2: std::path::PathBuf,
        kmer_size: usize,
        sketch_size: usize,
        max_reads: usize
    ) -> anyhow::Result<CountSketch> {

        //Decide on KMER encoding
        let codec = KMERCodec::new(kmer_size);

        let mut sketch=CountSketch::new(sketch_size);

        //Process both R1 and R2
        KmerCounter::get_countsketch_fq_one(path_read_r1, &mut sketch, &codec, max_reads)?;
        KmerCounter::get_countsketch_fq_one(path_read_r2, &mut sketch, &codec, max_reads)?;

        //TODO We can speed up the process by directly getting the readpairs without writing to disk. this needs a new API

        Ok(sketch)
    }




    pub fn get_countsketch_fq_one(
        path_read: std::path::PathBuf,
        sketch: &mut CountSketch,
        codec: &KMERCodec,
        max_reads: usize
    ) -> anyhow::Result<()> {

        //Read FASTQ file
        let mut cur_reads = 0;
        let file = File::open(&path_read).unwrap();
        let reader = BufReader::new(file);
        let mut readit = reader.lines();
        while cur_reads < max_reads {
            if let Some(_line1) = readit.next() {
                let seq= readit.next().unwrap()?;
                let _line3= readit.next().unwrap()?;
                let _line4= readit.next().unwrap()?;

                for kmer in seq.as_bytes().windows(codec.kmer_size) {
                    let encoded = unsafe {
                        codec.encode(kmer, 1) ///////////////////////////////// TODO there is a big loop inside here to compress bytes using lookup. should get a kmer iterator instead
                    };    
                    sketch.add(encoded); //can reduce to one addition per read for speed
                }

            } else {
                break;
            }
            cur_reads += 1;
        }

        Ok(())
    }






    pub fn get_minhash_fq(
        path_read_r1: std::path::PathBuf,
        path_read_r2: std::path::PathBuf,
        kmer_size: usize,
        features_nmin: usize,
        max_reads: usize
    ) -> anyhow::Result<BoundedMinHeap<KMERandCount>> {

        let mut min_heap = BoundedMinHeap::with_capacity(features_nmin);

        //Decide on KMER encoding
        let codec = KMERCodec::new(kmer_size);

        //Process both R1 and R2
        KmerCounter::get_minhash_fq_one(path_read_r1, &mut min_heap, &codec, max_reads)?;
        KmerCounter::get_minhash_fq_one(path_read_r2, &mut min_heap, &codec, max_reads)?;

        //TODO We can speed up the process by directly getting the readpairs without writing to disk. this needs a new API

        Ok(min_heap)
    }

    pub fn get_minhash_fq_one(
        path_read: std::path::PathBuf,
        min_heap: &mut BoundedMinHeap<KMERandCount>,
        codec: &KMERCodec,
        max_reads: usize
    ) -> anyhow::Result<()> {

        //Read FASTQ file
        let mut cur_reads = 0;
        let file = File::open(&path_read).unwrap();
        let reader = BufReader::new(file);
        let mut readit = reader.lines();
        while cur_reads < max_reads {
            if let Some(_line1) = readit.next() {
                let seq= readit.next().unwrap()?;
                let _line3= readit.next().unwrap()?;
                let _line4= readit.next().unwrap()?;

                for kmer in seq.as_bytes().windows(codec.kmer_size) {
                    let encoded = unsafe {
                        codec.encode(kmer, 1) ///////////////////////////////// TODO there is a big loop inside here to compress bytes using lookup. should get a kmer iterator instead
                    };    
                    let _ = min_heap.push(encoded);
                }

            } else {
                break;
            }
            cur_reads += 1;
        }

        Ok(())
    }






    pub fn get_minhash_kmcdump_parallel(
        params: &KmerCounter,
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
        reader.read_until(b'\t', &mut buf).expect("Could not parse first KMER from KMC dump file"); 
        Ok(buf.len()-1)  // Subtract -1 because \t is included in the string
    }




    pub fn extract_kmcdump_single_thread (
        params: &KmerCounter
    ) -> anyhow::Result<BoundedMinHeap<KMERandCount>> {

        let mut min_heap = BoundedMinHeap::with_capacity(params.features_nmin);

        //Decide on KMER encoding
        let codec = KMERCodec::new(params.kmer_size);

        //Set up regular reading of file
        let file = File::open(&params.path_kmcdump).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line=line.unwrap();
            let mut splitter = line.split('\t');
            let kmer = splitter.next().unwrap();
            let count: u32 = splitter.next().unwrap().parse().unwrap();

            let encoded = unsafe {
                codec.encode(kmer.as_bytes(), count)
            };    
            let _ = min_heap.push(encoded);
        }

        Ok(min_heap)
    }





    
    pub fn store_countsketch_seq(
        _kmer_size: usize, 
        sketch: &CountSketch,
        p: &PathBuf
    ){
        //Open file for writing
        let f=File::create(p).expect("Could not open file for writing");
        let mut bw=BufWriter::new(f);

        //Write total number of counts
        writeln!(bw, "{}", sketch.total).unwrap();    

        //Write each sketch entry
        for e in &sketch.sketch {
            writeln!(bw, "{}", e).unwrap();    
        }
    }



    
    pub fn store_minhash_seq(
        kmer_size: usize, 
        minhash: &mut BoundedMinHeap<KMERandCount>,
        p: &PathBuf
    ){
        //Open file for writing
        let f=File::create(p).expect("Could not open file for writing");
        let mut bw=BufWriter::new(f);


        //Write just kmer sequences. First get them and presort them.
        //By sorting them, compression will be more efficient, and merging them across cells will be much faster
        let codec = KMERCodec::new(kmer_size);
        let mut list_string:Vec<String> = Vec::with_capacity(minhash.len());
        while let Some(h) = minhash.pop_min() {
            unsafe {
                let kmer_string = codec.decode(&h);
                list_string.push(kmer_string);
            }
        }

        //Sort and write them out
        list_string.sort();
        for kmer_string in list_string {
            writeln!(bw, "{}", &kmer_string).unwrap();    
        }

    }



    pub fn store_minhash_all(
        kmer_size: usize, 
        minhash: &mut BoundedMinHeap<KMERandCount>,
        p: &PathBuf
    ){
        //Open file for writing
        let f=File::create(p).expect("Could not open file for writing");
        let mut bw=BufWriter::new(f);


        //Write kmer & count. Hopefully this iterates from min to max
        let codec = KMERCodec::new(kmer_size);

        while let Some(h) = minhash.pop_min() {
            unsafe {
                let kmer_string = codec.decode(&h);
                writeln!(bw, "{}\t{}\t{}\t{}", &kmer_string, &h.count,   &h.kmer, &h.hash).unwrap();    
            }
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
