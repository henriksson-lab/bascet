/*
  hex               dec
A 41     01000001   65
T 54     01010100   84
C 43     01000011   67
G 47     01000111   71
N 4e     01001110   78
         00011111  ############## possible bitmask to reduce size


*/

//could apply this to reduce the table from 65kb to 12kb. but this likely requires some unsafe casting
//const MASK_2NT: u16 = 0b0001111100011111;

///////////////////////////////
/// Lookup table for N where N is any of ATCGN. Maps to 0..16
const NT1_LOOKUP: [u8; 256 as usize] = {
    let mut table = [0u8; 256 as usize];
    table[(b'A') as usize] = 0b1000;
    table[(b'T') as usize] = 0b0100;
    table[(b'G') as usize] = 0b0010;
    table[(b'C') as usize] = 0b0001;
    table
};

///////////////////////////////
/// Lookup table for NN where N is any of ATCGN.
/// Maps compressed 2xN, where N is any of ATCGN, to 0..255.
/// ie compresses it to a single byte
const fn generate_nt2_table() -> [u8; 256 * 256] {
    const NUCLEOTIDES: [u8; 5] = [b'A', b'T', b'G', b'C', b'N'];
    let mut table = [0u8; 256 * 256];

    const fn generate_nt2_value(a: u8, b: u8) -> u8 {
        (NT1_LOOKUP[(a) as usize] << 4) | NT1_LOOKUP[(b) as usize]
    }

    let mut i = 0;
    while i < 5 * 5 {
        let n1 = NUCLEOTIDES[i / 5];
        let n2 = NUCLEOTIDES[i % 5];
        let idx_u8 = [n1, n2];
        let idx_u16 = concat_u8_u16(&idx_u8);

        //let idx = calculate_index(n1, n2);

        table[idx_u16 as usize] = generate_nt2_value(n1, n2);

        i += 1;
    }

    table
}

///////////////////////////////
/// Map compressed NN => single byte
const NT2_LOOKUP: [u8; 256 * 256] = generate_nt2_table();

///////////////////////////////
///
/// Hot-encode ATCGN as 4-bits per base
///
/// N will be encoded as 0, meaning that it has equal hamming distance to all other bases
///
#[derive(Clone, Copy)]
pub struct HotEncodeATCGN {}
impl HotEncodeATCGN {
    ///////////////////////////////
    /// Encode 8bp, in total 32 bits
    #[inline(always)]
    pub fn encode_8bp(bytes: &[u8; 8]) -> u32 {
        //This code contains no unsafe blocks but assumes that the rust compiler is able to optimize

        //Option for masking here
        //https://doc.rust-lang.org/nightly/core/simd/struct.Mask.html
        //https://towardsdatascience.com/nine-rules-for-simd-acceleration-of-your-rust-code-part-1-c16fe639ce21/
        //https://doc.rust-lang.org/std/simd/trait.SimdElement.html

        //getting array from slice, https://doc.rust-lang.org/std/primitive.array.html
        //<[u8; 2]>::try_from

        let sub1 = [bytes[0], bytes[1]];
        let sub2 = [bytes[2], bytes[3]];
        let sub3 = [bytes[4], bytes[5]];
        let sub4 = [bytes[6], bytes[7]];

        let idx1 = concat_u8_u16(&sub1);
        let idx2 = concat_u8_u16(&sub2);
        let idx3 = concat_u8_u16(&sub3);
        let idx4 = concat_u8_u16(&sub4);

        let mut lookup = [0, 0, 0, 0 as u8];
        lookup[0] = NT2_LOOKUP[idx1 as usize];
        lookup[1] = NT2_LOOKUP[idx2 as usize];
        lookup[2] = NT2_LOOKUP[idx3 as usize];
        lookup[3] = NT2_LOOKUP[idx4 as usize];

        let ret = u32::from(lookup[0])
            | (u32::from(lookup[1]) << 8)
            | (u32::from(lookup[2]) << 16)
            | (u32::from(lookup[3]) << 24);

        ret
    }

    ///////////////////////////////
    /// Encode 8bp, in total 32 bits
    #[inline(always)]
    pub fn fast_encode_8bp(bytes: &[u8; 8]) -> u16 {
        let n0 = NT1_LOOKUP[bytes[0] as usize] as u16;
        let n1 = NT1_LOOKUP[bytes[1] as usize] as u16;
        let n2 = NT1_LOOKUP[bytes[2] as usize] as u16;
        let n3 = NT1_LOOKUP[bytes[3] as usize] as u16;
        let n4 = NT1_LOOKUP[bytes[4] as usize] as u16;
        let n5 = NT1_LOOKUP[bytes[5] as usize] as u16;
        let n6 = NT1_LOOKUP[bytes[6] as usize] as u16;
        let n7 = NT1_LOOKUP[bytes[7] as usize] as u16;

        n0 | (n1 << 2) | (n2 << 4) | (n3 << 6) | (n4 << 8) | (n5 << 10) | (n6 << 12) | (n7 << 14)
    }

    ///////////////////////////////
    /// Encode 16bp, in total 64 bits
    #[inline(always)]
    pub fn encode_16bp(bytes: &[u8]) -> u64 {
        //; 16

        //This code contains no unsafe blocks but assumes that the rust compiler is able to optimize
        let idx1 = u16::from_ne_bytes([bytes[0], bytes[1]]);
        let idx2 = u16::from_ne_bytes([bytes[2], bytes[3]]);
        let idx3 = u16::from_ne_bytes([bytes[4], bytes[5]]);
        let idx4 = u16::from_ne_bytes([bytes[6], bytes[7]]);
        let idx5 = u16::from_ne_bytes([bytes[8], bytes[9]]);
        let idx6 = u16::from_ne_bytes([bytes[10], bytes[11]]);
        let idx7 = u16::from_ne_bytes([bytes[12], bytes[13]]);
        let idx8 = u16::from_ne_bytes([bytes[14], bytes[15]]);

        let mut lookup = [0, 0, 0, 0, 0, 0, 0, 0 as u8];
        lookup[0] = NT2_LOOKUP[idx1 as usize];
        lookup[1] = NT2_LOOKUP[idx2 as usize];
        lookup[2] = NT2_LOOKUP[idx3 as usize];
        lookup[3] = NT2_LOOKUP[idx4 as usize];

        lookup[4] = NT2_LOOKUP[idx5 as usize];
        lookup[5] = NT2_LOOKUP[idx6 as usize];
        lookup[6] = NT2_LOOKUP[idx7 as usize];
        lookup[7] = NT2_LOOKUP[idx8 as usize];

        //Convert 8bp into u64. not faster than below
        let ret = u64::from_ne_bytes(lookup);

        ret
    }

    pub fn closest_by_hamming_u32(query: u32, candidates: &[u32]) -> (usize, u32) {
        //Compute each hamming distance first. We need it later.
        //By doing this separate from testing, this hopefully gets vectorized
        let all_dist: Vec<u32> = candidates
            .iter()
            .map(|&x| HotEncodeATCGN::bitwise_hamming_distance_u32(query, x))
            .collect();

        //Assume that there is at least one barcode in the list to avoid Option
        let (min_index, min_dist) = all_dist
            .iter()
            .enumerate()
            .min_by_key(|(_index, &this_dist)| this_dist)
            .unwrap();
        //Note that there is SIMD for finding index of smallest entry. this requires memory alignment!
        //https://doc.rust-lang.org/beta/core/arch/x86/fn._mm_minpos_epu16.html
        //https://www.felixcloutier.com/x86/phminposuw

        (min_index, *min_dist)
    }

    pub fn fast_closest_by_hamming_u16(query: u16, candidates: &[u16]) -> (u16, u16) {
        let mut min_distance = u32::MAX;
        let mut min_index = 0;

        for (idx, &candidate) in candidates.iter().enumerate() {
            // Use your existing bitwise hamming distance for u16
            let distance = Self::bitwise_hamming_distance_u16(query, candidate);

            if distance < min_distance {
                min_distance = distance;
                min_index = idx;

                if distance == 0 {
                    break;
                }
            }
        }

        // max dist is 8 so this cannot overflow
        (min_index.try_into().unwrap(), min_distance as u16)
    }

    pub fn closest_by_hamming_u64(query: u64, candidates: &[u64]) -> (usize, u32) {
        //Compute each hamming distance first. We need it later.
        //By doing this separate from testing, this hopefully gets vectorized
        let all_dist: Vec<u32> = candidates
            .iter()
            .map(|&x| HotEncodeATCGN::bitwise_hamming_distance_u64(query, x))
            .collect();

        //Assume that there is at least one barcode in the list to avoid Option
        let (min_index, min_dist) = all_dist
            .iter()
            .enumerate()
            .min_by_key(|(_index, &this_dist)| this_dist)
            .unwrap();

        (min_index, *min_dist)
    }

    #[inline(always)]
    pub fn bitwise_hamming_distance_u16(a: u16, b: u16) -> u32 {
        let ret = 8 - (a & b).count_ones(); //Distance can be at most 8
        ret
    }

    ///////////////////////////////
    /// Computes the bitwise Hamming distance between two hot-encoded barcodes.
    /// Not XOR; we check how many bits agree, then subtract this from the maximum.
    ///
    /// Could speed up by instead returning similarity and maximizing in the caller
    #[inline(always)]
    pub fn bitwise_hamming_distance_u32(a: u32, b: u32) -> u32 {
        let ret = 8 - (a & b).count_ones(); //Distance can be at most 8
                                            /*
                                            println!("{:b}", a);
                                            println!("{:b}", b);
                                            println!("{:}", ret);
                                            panic!("ad");
                                             */

        ret
    }

    ///////////////////////////////
    /// Computes the bitwise Hamming distance between two hot-encoded barcodes.
    /// Not XOR; we check how many bits agree, then subtract this from the maximum
    #[inline(always)]
    pub fn bitwise_hamming_distance_u64(a: u64, b: u64) -> u32 {
        let ret = 16 - (a & b).count_ones(); //Distance can be at most 16
                                             /*
                                             println!("{:b}", a);
                                             println!("{:b}", b);
                                             println!("{:}", ret);
                                             panic!("ad");
                                              */

        ret
    }
}

#[inline(always)]
pub const fn concat_u8_u16(vec: &[u8; 2]) -> u16 {
    ((vec[1] as u16) << 8) | (vec[0] as u16)
}
