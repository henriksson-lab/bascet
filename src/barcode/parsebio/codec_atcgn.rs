/*
  hex               dec
A 41     01000001   65
T 54     01010100   84
C 43     01000011   67
G 47     01000111   71
N 4e     01001110   78
         00011111
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
const fn generate_nt2_table() -> [u8; 256*256] {
    const NUCLEOTIDES: [u8; 5] = [b'A', b'T', b'G', b'C', b'N'];
    let mut table = [0u8;256*256];

    const fn generate_nt2_value(a: u8, b: u8) -> u8 {
        (NT1_LOOKUP[(a) as usize] << 4) | NT1_LOOKUP[(b) as usize]
    }

    let mut i=0;
    while i<5*5 {
        let n1 = NUCLEOTIDES[i/5];
        let n2 = NUCLEOTIDES[i%5];
        let idx_u8=[n1,n2];
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
pub struct HotEncodeATCGN {
}
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
        
        let mut lookup = [0,0,0,0 as u8];
        lookup[0] = NT2_LOOKUP[idx1 as usize];
        lookup[1] = NT2_LOOKUP[idx2 as usize];
        lookup[2] = NT2_LOOKUP[idx3 as usize];
        lookup[3] = NT2_LOOKUP[idx4 as usize];

        let ret = u32::from(lookup[0]) | (u32::from(lookup[1])<<8) | (u32::from(lookup[2])<<16) | (u32::from(lookup[3])<<24);       

        ret
    }





    ///////////////////////////////
    /// Encode 16bp, in total 64 bits
    #[inline(always)]
    pub fn encode_16bp(bytes: &[u8]) -> u64 { //; 16
        
        //This code contains no unsafe blocks but assumes that the rust compiler is able to optimize 

        /* 
        this appears to have the same performance as below

        let idx1 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[0], bytes[1]])};
        let idx2 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[2], bytes[3]])};
        let idx3 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[4], bytes[5]])};
        let idx4 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[6], bytes[7]])};
        let idx5 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[8], bytes[9]])};
        let idx6 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[10], bytes[11]])};
        let idx7 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[12], bytes[13]])};
        let idx8 = unsafe {std::mem::transmute::<[u8; 2], u16>([bytes[14], bytes[15]])};
        */
        
        let sub1 = [bytes[0], bytes[1]];
        let sub2 = [bytes[2], bytes[3]];
        let sub3 = [bytes[4], bytes[5]];
        let sub4 = [bytes[6], bytes[7]];
        let sub5 = [bytes[8], bytes[9]];
        let sub6 = [bytes[10], bytes[11]];
        let sub7 = [bytes[12], bytes[13]];
        let sub8 = [bytes[14], bytes[15]];

        let idx1 = concat_u8_u16(&sub1);
        let idx2 = concat_u8_u16(&sub2);
        let idx3 = concat_u8_u16(&sub3);
        let idx4 = concat_u8_u16(&sub4);
        let idx5 = concat_u8_u16(&sub5);
        let idx6 = concat_u8_u16(&sub6);
        let idx7 = concat_u8_u16(&sub7);
        let idx8 = concat_u8_u16(&sub8);


        let mut lookup = [0,0,0,0, 0,0,0,0 as u8];
        lookup[0] = NT2_LOOKUP[idx1 as usize];
        lookup[1] = NT2_LOOKUP[idx2 as usize];
        lookup[2] = NT2_LOOKUP[idx3 as usize];
        lookup[3] = NT2_LOOKUP[idx4 as usize];

        lookup[4] = NT2_LOOKUP[idx5 as usize];
        lookup[5] = NT2_LOOKUP[idx6 as usize];
        lookup[6] = NT2_LOOKUP[idx7 as usize];
        lookup[7] = NT2_LOOKUP[idx8 as usize];


        let ret = unsafe {std::mem::transmute::<[u8; 8], u64>(lookup)};

/* 
        let ret = 
            u64::from(lookup[0]) | 
            (u64::from(lookup[1])<<8) | 
            (u64::from(lookup[2])<<16) | 
            (u64::from(lookup[3])<<24) | 
            (u64::from(lookup[4])<<32) | 
            (u64::from(lookup[5])<<40) |
            (u64::from(lookup[6])<<48) | 
            (u64::from(lookup[7])<<56);       
            */

        ret
    }








    pub fn closest_by_hamming_u32(
        query: u32, 
        candidates: &[u32]) -> (usize, u32) {

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
            .min_by_key(|(_index,&this_dist)| this_dist)
            .unwrap();
        //Note that there is SIMD for finding index of smallest entry. this requires memory alignment!
        //https://doc.rust-lang.org/beta/core/arch/x86/fn._mm_minpos_epu16.html 
        //https://www.felixcloutier.com/x86/phminposuw


        (min_index, *min_dist)
    }




    pub fn closest_by_hamming_u64(
        query: u64, 
        candidates: &[u64]) -> (usize, u32) {

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
            .min_by_key(|(_index,&this_dist)| this_dist)
            .unwrap();

        (min_index, *min_dist)
    }



    ///////////////////////////////
    /// Computes the bitwise Hamming distance between two hot-encoded barcodes.
    /// Not XOR; we check how many bits agree, then subtract this from the maximum.
    /// 
    /// Could speed up by instead returning similarity and maximizing in the caller
    #[inline(always)]
    pub fn bitwise_hamming_distance_u32(a: u32, b: u32) -> u32 {
        let ret = 8 - (a & b).count_ones();  //Distance can be at most 8
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
        let ret = 16 - (a & b).count_ones();  //Distance can be at most 16
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
    ((vec[1] as u16) <<8)  | (vec[0] as u16)
}


