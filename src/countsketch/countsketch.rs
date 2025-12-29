use nthash_rs::{NtHash, canonical};

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
        assert!(size != 0 && (size & (size - 1)) == 0, "size must be a power of 2");

        CountSketch {
            sketch: vec![0; size],
            total: 0,
            size_mask: size - 1,
        }
    }

    /// Add all k-mers from a DNA sequence to the sketch.
    ///
    /// Uses nthash rolling hash for efficient k-mer hashing.
    /// Processes canonical k-mer hashes
    pub fn add_sequence(&mut self, sequence: &[u8], k: u16) {
        let mut hasher = NtHash::new(sequence, k, 1, 0)
            .expect("Invalid sequence or k-mer size");

        while hasher.roll() {
            let canonical_hash = canonical(hasher.forward_hash(), hasher.reverse_hash());
            self.add_hash(canonical_hash);
        }
    }

    /// Add a single hash value to the sketch.
    #[inline(always)]
    pub fn add_hash(&mut self, hash: u64) {
        let g = hash.rotate_right(32);
        let s = PLUSMIN_LOOKUP[(g & 1) as usize];

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
