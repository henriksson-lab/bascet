<<<<<<< HEAD
const NT4_DIMSIZE: usize = 32 as usize; //0b0011111  = 31 is largest value
=======


const NT4_DIMSIZE: usize = 32 as usize;   //0b0011111  = 31 is largest value 
>>>>>>> main

// A  65  0b1000001
// C  67  0b1000011
// G  71  0b1000111
// T  84  0b1010100

<<<<<<< HEAD
=======

>>>>>>> main
////////////////
/// Only keep the lower bits
const fn reduce_base(b: u8) -> u8 {
    b & 0b0011111
}

<<<<<<< HEAD
=======

>>>>>>> main
////////////////
///  Lookup table for N where N is any of ATCG. Maps to 0..3
const NT1_LOOKUP: [u8; NT4_DIMSIZE] = {
    let mut table = [0u8; NT4_DIMSIZE];
    table[reduce_base(b'A') as usize] = 0b00;
    table[reduce_base(b'T') as usize] = 0b01;
    table[reduce_base(b'G') as usize] = 0b10;
    table[reduce_base(b'C') as usize] = 0b11;
    table[reduce_base(b'N') as usize] = 0b00; //This is default anyway. for UMIs, we can fail gracefully if we get an N
    table
};

<<<<<<< HEAD
=======

>>>>>>> main
const fn generate_nt4_value(a: u8, b: u8, c: u8, d: u8) -> u8 {
    (NT1_LOOKUP[reduce_base(a) as usize] << 6)
        | (NT1_LOOKUP[reduce_base(b) as usize] << 4)
        | (NT1_LOOKUP[reduce_base(c) as usize] << 2)
        | NT1_LOOKUP[reduce_base(d) as usize]
}

////////////////
/// Hopefully this gets optimized into shifts!
const fn calculate_index(a: u8, b: u8, c: u8, d: u8) -> usize {
    (reduce_base(a) as usize)
        + (reduce_base(b) as usize * NT4_DIMSIZE)
        + (reduce_base(c) as usize * NT4_DIMSIZE * NT4_DIMSIZE)
        + (reduce_base(d) as usize * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE)
}

<<<<<<< HEAD
=======

>>>>>>> main
////////////////
///  Lookup table for NNNN where N is any of ATCG.
/// Maps compressed ATCG (usize) to 0..255 ie compresses it to a single byte
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

////////////////
/// Map compressed ATCG => single byte
<<<<<<< HEAD
const NT4_LOOKUP: [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] =
    generate_nt4_table();

const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];

////////////////
/// KMER encoder
///
/// 12bp UMI => 24 bits needed (u32)
/// 16bp UMI => 32 bits needed (u32)
///
pub struct KMER2bit {}
impl KMER2bit {
    ////////////////
    /// Encode a kmer as u32
    #[inline(always)]
    pub unsafe fn encode_u32(bytes: &[u8]) -> u32 {
=======
const NT4_LOOKUP: [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] = generate_nt4_table();

const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];



///
///  KMER encoder
/// 
/// 12bp UMI => 24 bits needed (u32)
/// 16bp UMI => 32 bits needed (u32)
/// 
pub struct KMER2bit {
}
impl KMER2bit {

    ///
    /// Encode a kmer as u32. This function works for any size of data
    /// 
    #[inline(always)]
    pub unsafe fn encode_u32(bytes: &[u8]) -> u32 {

>>>>>>> main
        let kmer_size = bytes.len();

        let chunk_size: usize = 4;
        let kmer_size = kmer_size as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let mut encoded = 0;
        let ptr = bytes.as_ptr();

        // Compress chunks of 4 nucleotides to 1-byte encoding
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                reduce_base(*chunk_ptr.offset(0)) as usize
                    + (reduce_base(*chunk_ptr.offset(1)) as usize * NT4_DIMSIZE)
                    + (reduce_base(*chunk_ptr.offset(2)) as usize * NT4_DIMSIZE * NT4_DIMSIZE)
<<<<<<< HEAD
                    + (reduce_base(*chunk_ptr.offset(3)) as usize
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE)
=======
                    + (reduce_base(*chunk_ptr.offset(3)) as usize * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE)
>>>>>>> main
            };
            encoded = (encoded << 8) | u32::from(NT4_LOOKUP[idx]);
        }

        // Compress remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
<<<<<<< HEAD
            encoded =
                (encoded << 2) | u32::from(NT1_LOOKUP[reduce_base(bytes[start + i]) as usize]);
=======
            encoded = (encoded << 2) | u32::from(NT1_LOOKUP[reduce_base(bytes[start + i]) as usize]);
>>>>>>> main
        }

        encoded
    }

<<<<<<< HEAD
    ////////////////
    /// Get KMER from encoded format
    #[inline(always)]
    pub unsafe fn decode_u32(&self, encoded: u32, kmer_size: usize) -> String {
        let mut sequence = Vec::with_capacity(kmer_size);
        let mut temp = encoded;
=======


    ////////////////
    /// Get KMER from encoded format
    #[inline(always)]
    pub unsafe fn decode_u32(&self, encoded: u32, kmer_size:usize) -> String {
        let mut sequence = Vec::with_capacity(kmer_size);
        let mut temp = encoded; 
>>>>>>> main
        for _ in 0..kmer_size {
            let nuc = (temp & 0b11) as usize;
            sequence.push(NT_REVERSE[nuc]);
            temp >>= 2;
        }
        sequence.reverse();
        String::from_utf8_unchecked(sequence)
    }
<<<<<<< HEAD
}

=======

}





>>>>>>> main
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_4() {
<<<<<<< HEAD
        let t = [b'A', b'A', b'A', b'A'];
        let e = unsafe { KMER2bit::encode_u32(&t) };

        println!("{}", e);

        assert_eq!(e, 0);
=======
        let t=[
            b'A', b'A', b'A', b'A',
        ];
        let e=unsafe { KMER2bit::encode_u32(&t) };

        println!("{}",e);

        assert_eq!(e, 0);

>>>>>>> main
    }

    #[test]
    fn test_encode_12() {
<<<<<<< HEAD
        let t = [
            b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A', b'A',
            b'A', b'A',
        ];
        let e = unsafe { KMER2bit::encode_u32(&t) };

        println!("{}", e);

        assert_eq!(e, 0);
    }
}
=======
        let t=[
            b'A', b'A', b'A', b'A',
            b'A', b'A', b'A', b'A',
            b'A', b'A', b'A', b'A',
            b'A', b'A', b'A', b'A',
        ];
        let e=unsafe { KMER2bit::encode_u32(&t) };

        println!("{}",e);

        assert_eq!(e, 0);

    }
}
>>>>>>> main
