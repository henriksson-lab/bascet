use gxhash::GxHasher;
use std::hash::Hasher;
use std::io::{BufRead, BufReader};
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
