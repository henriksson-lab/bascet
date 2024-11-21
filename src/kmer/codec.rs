use bitfield_struct::bitfield;
use rand::{
    distributions::{Distribution, Uniform},
    rngs::SmallRng,
};

const NT_LOOKUP: [u8; 256] = {
    let mut table = [0u8; 256];
    table[b'A' as usize] = 0b00;
    table[b'T' as usize] = 0b01;
    table[b'G' as usize] = 0b10;
    table[b'C' as usize] = 0b11;
    table
};
// TODO: Improve lookup via this scheme
// const NT_LOOKUP: [u8; (b'T' - b'A' + 1) as usize] = {
//     let mut table = [0u8; (b'T' - b'A' + 1) as usize];
//     table[(b'A' - b'A') as usize] = 0b00;
//     table[(b'T' - b'A') as usize] = 0b01;
//     table[(b'G' - b'A') as usize] = 0b10;
//     table[(b'C' - b'A') as usize] = 0b11;
//     table
// };
const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];

pub struct Codec<const K: usize>;
impl<const K: usize> Codec<K> {
    const KMER_SIZE: usize = K;

    pub const fn new() -> Self {
        Codec
    }

    #[inline(always)]
    pub unsafe fn encode(
        &self,
        kmer: &[u8],
        count: u32,
        rng: &mut impl rand::Rng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let mut encoded: u128 = 0;

        for i in 0..Self::KMER_SIZE as usize {
            encoded = (encoded << 2) | u128::from(NT_LOOKUP[kmer[i] as usize]);
        }

        return EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count as u16)
            .with_rand(range.sample(rng));
    }
    #[inline(always)]
    pub unsafe fn encode_str(
        &self,
        kmer: &str,
        count: u16,
        rng: &mut SmallRng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let mut encoded: u128 = 0;
        let bytes = kmer.as_bytes();

        for i in 0..Self::KMER_SIZE as usize {
            encoded = (encoded << 2) | u128::from(NT_LOOKUP[bytes[i] as usize]);
        }

        return EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count)
            .with_rand(range.sample(rng));
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
