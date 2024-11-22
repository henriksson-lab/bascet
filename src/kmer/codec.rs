use bitfield_struct::bitfield;
use rand::{
    distributions::{Distribution, Uniform},
    rngs::SmallRng, Rng,
};

const NT1_LOOKUP: [u8; (b'T' - b'A' + 1) as usize] = {
    let mut table = [0u8; (b'T' - b'A' + 1) as usize];
    table[(b'A' - b'A') as usize] = 0b00;
    table[(b'T' - b'A') as usize] = 0b01;
    table[(b'G' - b'A') as usize] = 0b10;
    table[(b'C' - b'A') as usize] = 0b11;
    table
};

const fn generate_nt4_value(a: u8, b: u8, c: u8, d: u8) -> u8 {
    (NT1_LOOKUP[(a - b'A') as usize] << 6) | (NT1_LOOKUP[(b - b'A') as usize] << 4) | (NT1_LOOKUP[(c - b'A') as usize] << 2) | NT1_LOOKUP[(d - b'A') as usize]
}

const fn calculate_index(a: u8, b: u8, c: u8, d: u8) -> usize {
    const DIM: usize = (b'T' - b'A' + 1) as usize;
    ((a - b'A') as usize) + 
    ((b - b'A') as usize * DIM) +
    ((c - b'A') as usize * DIM * DIM) +
    ((d - b'A') as usize * DIM * DIM * DIM)
}
const fn generate_nt4_table() -> [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] {
    const NUCLEOTIDES: [u8; 4] = [b'A', b'T', b'G', b'C'];
    let mut table = [0u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE];
    
    let mut i = 0;
    while i < 256 {  // 4^4 combinations
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
const NT4_LOOKUP: [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] = generate_nt4_table();

const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];

//NOTE: all of this can probably make use of SIMD operations but I do not know how that'd work
pub struct Codec<const K: usize>;
impl<const K: usize> Codec<K> {
    const KMER_SIZE: usize = K;

    pub const fn new() -> Self {
        Codec
    }

    #[inline(always)]
    pub unsafe fn encode(
        &self,
        bytes: &[u8],
        count: u32,
        rng: &mut impl Rng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let chunk_size: usize = 4;
        let kmer_size = Self::KMER_SIZE as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let mut encoded: u128 = 0;
        let ptr = bytes.as_ptr();

        // Process chunks of 4 nucleotides
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u128::from(NT4_LOOKUP[idx]);
        }

        // Handle remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u128::from(NT1_LOOKUP[(bytes[start + i] - b'A') as usize]);
        }

        EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count as u16)
            .with_rand(range.sample(rng))
    }

    #[inline(always)]
    pub unsafe fn encode_str(
        &self,
        kmer: &str,
        count: u16,
        rng: &mut impl Rng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let chunk_size: usize = 4;
        let kmer_size = Self::KMER_SIZE as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let bytes = kmer.as_bytes();
        let mut encoded: u128 = 0;
        let ptr = bytes.as_ptr();

        // Process chunks of 4 nucleotides
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u128::from(NT4_LOOKUP[idx]);
        }

        // Handle remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u128::from(NT1_LOOKUP[(bytes[start + i] - b'A') as usize]);
        }

        EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count)
            .with_rand(range.sample(rng))
    }

    #[inline(always)]
    pub unsafe fn decode(&self, encoded: u128) -> String {
        let mut sequence = Vec::with_capacity(Self::KMER_SIZE);
        let mut temp = EncodedKMER::from_bits(encoded).kmer();
        for _ in 0..Self::KMER_SIZE {
            let nuc = (temp & 0b11) as usize;
            sequence.push(NT_REVERSE[nuc]);
            temp >>= 2;
        }
        sequence.reverse();
        String::from_utf8_unchecked(sequence)
    }
}

#[bitfield(u128)]
pub struct EncodedKMER {
    #[bits(96)]
    pub kmer: u128,

    #[bits(16)]
    pub rand: u16,

    #[bits(16)]
    pub count: u16,
}
