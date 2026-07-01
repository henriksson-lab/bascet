use gxhash::gxhash64;
use nthash_rs::{NtHash, canonical};
use rand::Rng;
const PLUSMIN_LOOKUP: [i64; 2] = [1, -1];

/// Requires power-of-2 sketch sizes for optimal performance.
/// Uses nthash for efficient rolling hash computation on DNA sequences.
#[derive(Clone, Debug)]
pub struct CountSketch {
    pub sketch: Vec<i64>,
    pub total: i64,
    size_mask: usize,
}

impl CountSketch {
    /// Create a new CountSketch with the given size.
    ///
    /// Panics if size is not a power of 2.
    pub fn new(size: usize) -> Self {
        assert!(
            size != 0 && (size & (size - 1)) == 0,
            "size must be a power of 2"
        );

        CountSketch {
            sketch: vec![0; size],
            total: 0,
            size_mask: size - 1,
        }
    }

    /// Add all k-mers from a DNA sequence to the sketch.
    ///
    /// Uses nthash rolling hash for efficient k-mer hashing.
    /// Processes canonical k-mer hashes.
    /// Returns Err(()) if the sequence is shorter than k.
    pub fn add_sequence(&mut self, sequence: &[u8], k: u16) -> Result<(), ()> {
        let k = k as usize;
        if sequence.len() < k {
            return Err(());
        }

        let mut revcomp_buf = vec![0u8; k];

        for window in sequence.windows(k) {
            let fwd_hash = gxhash64(window, 0);

            for (i, &b) in window.iter().rev().enumerate() {
                revcomp_buf[i] = Self::complement(b);
            }
            let rev_hash = gxhash64(&revcomp_buf, 0);

            self.add_hash(canonical(fwd_hash, rev_hash));
        }

        Ok(())
    }

    #[inline]
    fn complement(b: u8) -> u8 {
        match b {
            b'A' => b'T',
            b'T' => b'A',
            b'C' => b'G',
            b'G' => b'C',
            b'a' => b't',
            b't' => b'a',
            b'c' => b'g',
            b'g' => b'c',
            _ => b'N',
        }
    }

    /// Add a single hash value to the sketch.
    #[inline(always)]
    pub fn add_hash(&mut self, hash: u64) {
        // let g = hash.rotate_right(32);
        let s = PLUSMIN_LOOKUP[rand::thread_rng().gen_range(0..2)];
        let pos = (hash as usize) & self.size_mask;

        self.sketch[pos] += s;
        self.total += 1;
    }

    /// Reset the sketch to all zeros.
    pub fn reset(&mut self) {
        self.sketch.fill(0);
        self.total = 0;
    }

    /// Get the total number of k-mers added.
    #[inline(always)]
    pub fn total(&self) -> i64 {
        self.total
    }

    /// Get the sketch values as a Vec.
    pub fn snapshot(&self) -> Vec<i64> {
        self.sketch.clone()
    }
}
