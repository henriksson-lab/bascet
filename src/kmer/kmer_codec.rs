use std::{cmp::Ordering, hash::Hasher};

////////////// Lookup table for N where N is any of ATCG. Maps to 0..3
const NT1_LOOKUP: [u8; (b'T' - b'A' + 1) as usize] = {
    let mut table = [0u8; (b'T' - b'A' + 1) as usize];
    table[(b'A' - b'A') as usize] = 0b00;
    table[(b'T' - b'A') as usize] = 0b01;
    table[(b'G' - b'A') as usize] = 0b10;
    table[(b'C' - b'A') as usize] = 0b11;
    table
};


const fn generate_nt4_value(a: u8, b: u8, c: u8, d: u8) -> u8 {
    (NT1_LOOKUP[(a - b'A') as usize] << 6)
        | (NT1_LOOKUP[(b - b'A') as usize] << 4)
        | (NT1_LOOKUP[(c - b'A') as usize] << 2)
        | NT1_LOOKUP[(d - b'A') as usize]
}


const fn calculate_index(a: u8, b: u8, c: u8, d: u8) -> usize {
    const DIM: usize = (b'T' - b'A' + 1) as usize;
    ((a - b'A') as usize)
        + ((b - b'A') as usize * DIM)
        + ((c - b'A') as usize * DIM * DIM)
        + ((d - b'A') as usize * DIM * DIM * DIM)
}


////////////// Lookup table for NNNN where N is any of ATCG.
////////////// Maps compressed ATCG (usize) to 0..255 ie compresses it to a single byte
const fn generate_nt4_table() -> [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] {
    const NUCLEOTIDES: [u8; 4] = [b'A', b'T', b'G', b'C'];
    let mut table = [0u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE];

    let mut i = 0;
    while i < 256 {
        // 4^4 combinations
        let n1 = NUCLEOTIDES[(i >> 6) & 0b11];
        let n2 = NUCLEOTIDES[(i >> 4) & 0b11];
        let n3 = NUCLEOTIDES[(i >> 2) & 0b11];
        let n4 = NUCLEOTIDES[i & 0b11];

        let idx = calculate_index(n1, n2, n3, n4);
        table[idx] = generate_nt4_value(n1, n2, n3, n4);

        i += 1;
    }
    table
}
const NT4_DIMSIZE: usize = (b'T' - b'A' + 1) as usize;
const NT4_LOOKUP: [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] =  /////// Map compressed ATCG => single byte
    generate_nt4_table();

const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];






//NOTE: all of this can probably make use of SIMD operations but I do not know how that'd work

//////////// KMER encoder, for a given KMER-size
#[derive(Clone, Copy)]
pub struct KMERCodec {
    pub kmer_size: usize,
}
impl KMERCodec {
    pub const fn new(kmer_size: usize) -> Self {
        Self {
            kmer_size: kmer_size,
        }
    }

    //////////// Encode a kmer + count + random value
    #[inline(always)]
    pub unsafe fn encode(&self, bytes: &[u8], count: u32) -> KMERandCount {
        let chunk_size: usize = 4;
        let kmer_size = self.kmer_size as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let mut encoded = 0;
        let ptr = bytes.as_ptr();

        // Compress chunks of 4 nucleotides to 1-byte encoding
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u64::from(NT4_LOOKUP[idx]);
        }

        // Compress remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u64::from(NT1_LOOKUP[(bytes[start + i] - b'A') as usize]);
        }

        KMERandCount::new(encoded, count)
    }



    //////////// Encode a kmer + count + random value ............... cannot just use as_bytes + above?
    #[inline(always)]
    pub unsafe fn encode_str(&self, kmer: &str, count: u32) -> KMERandCount {
        let chunk_size: usize = 4;
        let kmer_size = self.kmer_size as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let bytes = kmer.as_bytes();
        let mut encoded = 0;
        let ptr = bytes.as_ptr();

        // Process chunks of 4 nucleotides
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u64::from(NT4_LOOKUP[idx]);
        }

        // Handle remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u64::from(NT1_LOOKUP[(bytes[start + i] - b'A') as usize]);
        }

        KMERandCount::new(encoded, count)
    }

    #[inline(always)]
    pub unsafe fn decode(&self, encoded: &KMERandCount) -> String {
        let mut sequence = Vec::with_capacity(self.kmer_size);
        let mut temp = encoded.kmer; 
        for _ in 0..self.kmer_size {
            let nuc = (temp & 0b11) as usize;
            sequence.push(NT_REVERSE[nuc]);
            temp >>= 2;
        }
        sequence.reverse();
        String::from_utf8_unchecked(sequence)
    }
}


#[inline(always)]
fn hash_for_kmer(kmer: u64) -> u32 {
    //Use a fast hash function https://docs.rs/fasthash/latest/fasthash/sea/index.html
    let mut hasher=fasthash::sea::Hasher64::new();
    hasher.write_u64(kmer);
    let f= hasher.finish();

    let hash = f ^ (f>>32); //fit hash in u32
    hash as u32
}





#[derive(Clone, Copy)]
pub struct KMERandCount {
    pub kmer: u64,
    pub hash: u32,
    pub count: u32,
}
impl KMERandCount {
    pub fn new(
        kmer: u64,
        count: u32
   ) -> KMERandCount {
       Self {
           kmer: kmer,
           hash: hash_for_kmer(kmer), 
           count: count
       }
   }
}

impl PartialOrd for KMERandCount {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.hash.cmp(&other.hash))
    }
}

impl Ord for KMERandCount {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialEq for KMERandCount {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for KMERandCount { }