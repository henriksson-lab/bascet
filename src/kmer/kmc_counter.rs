use gxhash::GxHasher;
use std::hash::Hasher;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::{fs::File, usize};


// use super::kmer_codec::KMERandCount;
use super::KMERCodec;

// \t[0-9]{10}\n
// (4 294 967 296) is max value for kmer counts, thats 10 digits :)
pub const KMC_COUNTER_MAX_DIGITS: usize = 12;
pub const HUGE_PAGE_SIZE: usize = 2048 * 1024;
const PLUSMIN_LOOKUP: [i64; 2] = [1, -1];

#[derive(Clone, Debug)]
pub struct CountSketch {
    // NOTE: using smallvec is slower. also no clue why
    pub sketch: Vec<i64>,
    pub total: i64,
    // NOTE: for some reason having one hasher is a lot more expensive than creating a new one for each add????
    // pub hasher: GxHasher,
}
impl CountSketch {
    pub fn new(size: usize) -> CountSketch {
        CountSketch {
            sketch: vec![0; size],
            total: 0,
            // hasher: GxHasher::default(),
        }
    }

    #[inline(always)]
    pub fn add_kmer(&mut self, kmer: &[u8]) {
        // https://wangshusen.github.io/code/countsketch.html inspo
        // in R: https://www.rdocumentation.org/packages/aroma.light/versions/3.2.0/topics/wpca
        // https://docs.rs/streaming_algorithms/latest/streaming_algorithms/  mincounthash code
        // python: https://pdsa.readthedocs.io/en/latest/frequency/count_sketch.html

        // let hasher = &mut self.hasher;
        // NOTE: gxhasher might not be very compatible. Alternatively Rapidhash (https://github.com/Nicoshev/rapidhash) could be useful?
        // at some point a rolling hash would likely also be worth investigating?
        let mut hasher = GxHasher::default();
        hasher.write(kmer);
        let h = hasher.finish();
        hasher.write_u8(0);
        let g = hasher.finish();
        let s = PLUSMIN_LOOKUP[(g & 0b0000_0001) as usize]; // last bit

        let pos = (h as usize) % self.sketch.len();
        self.sketch[pos] += s;
        self.total += 1;
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.sketch.fill(0);
        self.total = 0;
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
        max_reads: usize,
    ) -> anyhow::Result<CountSketch> {
        //Decide on KMER encoding
        let codec = KMERCodec::new(kmer_size);

        let mut sketch = CountSketch::new(sketch_size);

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
        max_reads: usize,
    ) -> anyhow::Result<()> {
        // let start = std::time::Instant::now();

        // Pre-allocate buffers
        let file = File::open(&path_read)?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
        let mut line_buf = Vec::with_capacity(2 ^ 16);
        let mut encoded_buf = Vec::with_capacity(2 ^ 16);

        let mut cur_reads = 0;
        while cur_reads < max_reads {
            // Skip header (line 1)
            if reader.read_until(b'\n', &mut line_buf)? == 0 {
                break;
            }
            line_buf.clear();

            // Read sequence (line 2)
            if reader.read_until(b'\n', &mut line_buf)? == 0 {
                break;
            }
            // Remove newline
            if line_buf.ends_with(&[b'\n']) {
                line_buf.pop();
            }

            // Process sequence
            encoded_buf.clear();
            encoded_buf.extend(line_buf.iter().map(|&b| KMERCodec::ENCODE[b as usize]));
            for window in encoded_buf.windows(codec.kmer_size) {
                sketch.add_kmer(window);
            }
            line_buf.clear();

            // Skip quality lines (3 and 4)
            reader.read_until(b'\n', &mut line_buf)?;
            line_buf.clear();
            reader.read_until(b'\n', &mut line_buf)?;
            line_buf.clear();

            cur_reads += 1;
        }

        // let duration = start.elapsed();
        // log::info!("Processed {} reads in {:?}", cur_reads, duration);

        Ok(())
    }



}

// #[inline(always)]
// fn find_chunk_start(chunk: &[u8], raw_start: usize, ovlp_size: usize) -> usize {
//     for i in 0..min(ovlp_size, chunk.len()) {
//         if chunk[i] == b'\n' {
//             return raw_start + i + 1;
//         }
//     }
//     raw_start
// }

// #[inline(always)]
// fn find_chunk_end(chunk: &[u8], raw_end: usize, ovlp_size: usize) -> usize {
//     let window_size = min(ovlp_size, chunk.len());
//     for i in (chunk.len() - window_size..chunk.len()).rev() {
//         if chunk[i] == b'\n' {
//             return min(i + 1, raw_end);
//         }
//     }
//     raw_end
// }

// ///////// Parse the count of kmers from KMC database. Counting has already been done
// #[inline(always)]
// unsafe fn parse_count_u32(bytes: &[u8]) -> u32 {
//     // Fast path for single digit, most common case
//     if bytes.len() == 1 {
//         return (bytes[0] - b'0') as u32;
//     }

//     // Fast path for two digits, second most common case
//     if bytes.len() == 2 {
//         return ((bytes[0] - b'0') * 10 + (bytes[1] - b'0')) as u32;
//     }

//     // LUT for two-digit numbers
//     const LOOKUP: [u32; 100] = {
//         let mut table = [0u32; 100];
//         let mut i = 0;
//         while i < 100 {
//             table[i] = (i / 10 * 10 + i % 10) as u32;
//             i += 1;
//         }
//         table
//     };

//     let chunks = bytes.chunks_exact(2);
//     let remainder = chunks.remainder();

//     let mut result = 0u32;
//     for chunk in chunks {
//         let idx = ((chunk[0] - b'0') * 10 + (chunk[1] - b'0')) as usize;
//         result = result.wrapping_mul(100) + LOOKUP[idx];
//     }

//     // Handle last digit if present
//     if let Some(&d) = remainder.first() {
//         result = result.wrapping_mul(10) + (d - b'0') as u32;
//     }

//     result
// }

// ////////// For a chunk of data, extract KMERs
// #[inline(always)]
// fn process_chunk_to_minheap(
//     chunk: &[u8],
//     min_heap: &mut BoundedMinHeap<KMERandCount>,
//     codec: KMERCodec,
//     kmer_size: usize,
//     ovlp_size: usize,
// ) {
//     let chunk_length = chunk.len();
//     let min_read_size = kmer_size + 2; // K + 2 is minimum size for a kmer + count (\t\d)
//     let n_max_panes = chunk_length / min_read_size;
//     let mut cursor = 0;

//     for _ in 0..n_max_panes {
//         if cursor >= chunk_length {
//             break;
//         }

//         let pane_start = cursor;
//         let remaining = chunk_length - pane_start;

//         if remaining < min_read_size {
//             break;
//         }

//         // Find the length of the current pane (up to next newline)
//         let mut pane_length = min_read_size;
//         for i in pane_length..min(ovlp_size, remaining) {
//             if chunk[pane_start + i] == b'\n' {
//                 pane_length = i;
//                 break;
//             }
//         }

//         // Extract and encode kmer with its count
//         let kmer_end = pane_start + kmer_size;
//         let count = unsafe { parse_count_u32(&chunk[kmer_end + 1..pane_start + pane_length]) };

//         let encoded = unsafe {
//             codec.encode(&chunk[pane_start..kmer_end], count)
//         };

//         let _ = min_heap.push(encoded);

//         cursor += pane_length + 1; // +1 for newline
//     }
// }
