

use blart::AsBytes;
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::{fs::File, usize};

use crate::kmer::{BoundedHeap, BoundedMinHeap};




static BYTE_LUT: [u8; 128] = {
    let mut lut = [0u8; 128];
    lut[b'a' as usize] = 0b00;
    lut[b't' as usize] = 0b10;
    lut[b'u' as usize] = 0b10;
    lut[b'c' as usize] = 0b01;
    lut[b'g' as usize] = 0b11;
    lut[b'A' as usize] = 0b00;
    lut[b'T' as usize] = 0b10;
    lut[b'U' as usize] = 0b10;
    lut[b'C' as usize] = 0b01;
    lut[b'G' as usize] = 0b11;
    lut
};

static BITS_LUT: [u8; 4] = {
    let mut lut = [0u8; 4];
    lut[0b00] = b'A';
    lut[0b10] = b'T';
    lut[0b01] = b'C';
    lut[0b11] = b'G';
    lut
};



///
/// KMER encoder, for a given KMER-size
/// 
#[derive(Clone, Copy)]
pub struct MinhashCodec {
    pub kmer_size: usize,
}
impl MinhashCodec {
    pub const ENCODE: [u8; 256] = {
        let mut table = [0u8; 256];
        table[b'A' as usize] = 0b00;
        table[b'C' as usize] = 0b01;
        table[b'G' as usize] = 0b10;
        table[b'T' as usize] = 0b11;
        table
    };

    pub const fn new(kmer_size: usize) -> Self {
        Self {
            kmer_size: kmer_size,
        }
    }






    /// from https://github.com/Daniel-Liu-c0deb0t/cute-nucleotides/blob/master/src/n_to_bits.rs
    /// 
    /// replace with portable assembly in the future
    /// 
    /// Encode `{A, T/U, C, G}` from the byte string into pairs of bits (`{00, 10, 01, 11}`) packed into 64-bit integers,
    /// by using a naive scalar method.
    pub fn n_to_bits_lut(n: &[u8]) -> Vec<u64> {
        let mut res = vec![0u64; (n.len() >> 5) + if n.len() & 31 == 0 {0} else {1}];

        unsafe {
            for i in 0..n.len() {
                let offset = i >> 5;
                let shift = (i & 31) << 1;
                *res.get_unchecked_mut(offset) = *res.get_unchecked(offset)
                    | ((*BYTE_LUT.get_unchecked(*n.get_unchecked(i) as usize) as u64) << shift);
            }
        }

        res
    }

    /// from https://github.com/Daniel-Liu-c0deb0t/cute-nucleotides/blob/master/src/n_to_bits.rs
    /// 
    /// replace with portable assembly in the future
    /// 
    /// Decode pairs of bits from packed 64-bit integers to get a byte string of `{A, T/U, C, G}`, by using a naive scalar method.
    pub fn bits_to_n_lut(bits: &[u64], len: usize) -> Vec<u8> {
        if len > (bits.len() << 5) {
            panic!("The length is greater than the number of nucleotides!");
        }

        unsafe {
            let layout = std::alloc::Layout::from_size_align_unchecked(len, 1);
            let res_ptr = std::alloc::alloc(layout);

            for i in 0..len {
                let offset = i >> 5;
                let shift = (i & 31) << 1;
                let curr = *bits.get_unchecked(offset);
                *res_ptr.offset(i as isize) = *BITS_LUT.get_unchecked(((curr >> shift) & 0b11) as usize);
            }

            Vec::from_raw_parts(res_ptr, len, len)
        }
    }


    #[inline(always)]
    pub fn h_hash_for_packed_kmer(kmer: u64) -> u64 { //&[u8]
        gxhash::gxhash64(&kmer.to_le_bytes(), 0x00)
    }

    pub fn pack_kmer(n: &[u8]) -> u64 {
        let v = Self::n_to_bits_lut(n);
        v[0]
    }

    pub fn unpack_kmer(&self, bits: u64) -> Vec<u8> {
        let v=vec![bits];
        Self::bits_to_n_lut(&v, self.kmer_size)
    }

}







///
/// Store KMERs in 2bit form
/// 
/// Should adapt this code in the future:
/// https://github.com/Daniel-Liu-c0deb0t/cute-nucleotides
/// 
/// might be faster to SIMD, and just bitwise the result
/// 
/// 
#[derive(Clone, Copy)]
pub struct MinhashKMER {
    pub kmer: u64, 
    pub hash: u64,
}
impl MinhashKMER {

    pub fn new(
        kmer: &[u8]
    ) -> MinhashKMER {
        let kmer_8 = kmer[0..8].try_into().unwrap();
        let pack_kmer = MinhashCodec::pack_kmer(kmer_8);
        Self {
           kmer: pack_kmer,
           hash: MinhashCodec::h_hash_for_packed_kmer(pack_kmer),
       }
    }

    pub fn decode(&self, codec: &MinhashCodec) -> Vec<u8>{
        codec.unpack_kmer(self.kmer)
        //KMERCodec::unpack_kmer(&self, bits)
        //codec.kmer_size
    }

}

impl PartialOrd for MinhashKMER {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.hash.cmp(&other.hash))
    }
}

impl Ord for MinhashKMER {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialEq for MinhashKMER {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for MinhashKMER { }


pub struct MinHash {}

impl MinHash {
    


    pub fn get_minhash_fq(
        path_read_r1: std::path::PathBuf,
        path_read_r2: std::path::PathBuf,
        kmer_size: usize,
        features_nmin: usize,
        max_reads: usize
    ) -> anyhow::Result<BoundedMinHeap<MinhashKMER>> {

        let mut min_heap = BoundedMinHeap::with_capacity(features_nmin);

        //Decide on KMER encoding
        let codec = MinhashCodec::new(kmer_size);

        //Process both R1 and R2
        Self::get_minhash_fq_one(path_read_r1, &mut min_heap, &codec, max_reads)?;
        Self::get_minhash_fq_one(path_read_r2, &mut min_heap, &codec, max_reads)?;

        //TODO We can speed up the process by directly getting the readpairs without writing to disk. this needs a new API
        Ok(min_heap)
    }

    pub fn get_minhash_fq_one(
        path_read: std::path::PathBuf,
        min_heap: &mut BoundedMinHeap<MinhashKMER>,
        codec: &MinhashCodec,
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

                let seq_bytes: Vec<u8> = seq.as_bytes().iter().map(|&b| MinhashCodec::ENCODE[b as usize]).collect();
                 for encoded in seq_bytes.windows(codec.kmer_size) {
                    let _ = min_heap.push(
                        MinhashKMER::new(encoded)
                    );
                }

            } else {
                break;
            }
            cur_reads += 1;
        }

        Ok(())
    }    



    pub fn store_minhash_seq(
        kmer_size: usize,
        minhash: &mut BoundedMinHeap<MinhashKMER>,//&[u8]>,
        p: &PathBuf
    ){
        //Open file for writing
        let f=File::create(p).expect("Could not open file for writing");
        let mut bw=BufWriter::new(f);

        //Write just kmer sequences. First get them and presort them.
        //By sorting them, compression will be more efficient, and merging them across cells will be much faster
        let codec = MinhashCodec::new(kmer_size);
        let mut list_string:Vec<Vec<u8>> = Vec::with_capacity(minhash.len());
        while let Some(h) = minhash.pop_min() {
            let kmer_bytes = h.decode(&codec);
            list_string.push(kmer_bytes);
        }

        //Sort and write them out
        list_string.sort();
        for kmer_string in list_string {
            bw.write_all(kmer_string.as_bytes()).unwrap();
            writeln!(bw, "").unwrap();
        }

    }



}

/*



    pub fn get_minhash_kmcdump_parallel(
        params: &KmerCounter,
        n_workers: usize
    ) -> anyhow::Result<BoundedMinHeap<&[u8]>> {
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
        Ok(buf.len()-1)  Subtract -1 because \t is included in the string
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

    pub fn store_countsketch_seq(sketch: &CountSketch, p: &PathBuf) {
        //Open file for writing
        let f = File::create(p).expect("Could not open file for writing");
        let mut bw = BufWriter::new(f);

        //Write total number of counts
        writeln!(bw, "{}", sketch.total).unwrap();

        //Write each sketch entry
        for e in &sketch.sketch {
            writeln!(bw, "{}", e).unwrap();
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

 */