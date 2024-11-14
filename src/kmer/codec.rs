use bitfield_struct::bitfield;

const NT_LOOKUP: [u8; 256] = {
    let mut table = [0u8; 256];
    table[b'A' as usize] = 0b00;
    table[b'T' as usize] = 0b01;
    table[b'G' as usize] = 0b10;
    table[b'C' as usize] = 0b11;
    table
};

pub struct Codec<const K: usize>;
impl<const K: usize> Codec<K> {
    const KMER_SIZE: usize = K;

    pub const fn new() -> Self {
        Codec
    }

    #[inline(always)]
    pub unsafe fn encode(&self, kmer: &str, count: u32) -> EncodedKMER {
        let bytes = kmer.as_bytes();
        let mut encoded: u128 = 0;

        for i in 0..Self::KMER_SIZE as usize {
            encoded = (encoded << 2) | u128::from(NT_LOOKUP[bytes[i] as usize]);
        }

        return EncodedKMER::new().with_kmer(encoded).with_count(count);
    }
}

#[bitfield(u128)]
pub struct EncodedKMER {
    #[bits(104)]
    pub kmer: u128,

    #[bits(24)]
    pub count: u32,
}
