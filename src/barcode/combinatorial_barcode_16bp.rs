use tracing::trace;
use std::collections::HashMap;
use std::io::Read;

use crate::barcode::parsebio::HotEncodeATCGN;
use crate::fileformat::shard::CellID;



type HalfVector = wide::u16x32;
type FullVector = wide::u32x16;
const SIMD_LENGTH: usize = HalfVector::LANES as usize;
const SIMD_FULL_LENGTH: usize = FullVector::LANES as usize;


///////////////////////////////
/// Convert string, assumed to be 16bp, to a packed barcode
pub fn str_to_barcode_16bp(sequence: &str) -> u64 {
    let bytes = sequence.as_bytes();
    HotEncodeATCGN::encode_16bp(bytes)
}

///////////////////////////////
/// A set of barcode positions and sequences, making up a total combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcode16bp {
    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String, usize>,

    //Each barcode set in the combination
    pools: Vec<CombinatorialBarcodePart16bp>,

    //How much to trim from this read
    pub trim_bcread_len: usize,

    //Location of the UMI
    pub umi_from: usize,
    pub umi_to: usize,
}
impl CombinatorialBarcode16bp {
    pub fn new() -> CombinatorialBarcode16bp {
        CombinatorialBarcode16bp {
            map_poolname_to_index: HashMap::new(),
            pools: vec![],
            trim_bcread_len: 0,
            umi_from: 0,
            umi_to: 0,
        }
    }

    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

    pub fn add_pool(&mut self, poolname: &str, pool: CombinatorialBarcodePart16bp) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index
            .insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    pub fn add_bc(&mut self, name: &str, poolname: &str, sequence: &str) {
        //Create new pool if needed
        if !(self.map_poolname_to_index.contains_key(poolname)) {
            self.add_pool(poolname, CombinatorialBarcodePart16bp::new());
        }

        let pool_index = self
            .map_poolname_to_index
            .get(poolname)
            .expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart16bp =
            self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }

    ///////////////////////////////
    /// Detect barcode only
    #[inline(always)]
    pub fn detect_barcode(
        &self,
        read_seq: &[u8],
        abort_early: bool,
        total_distance_cutoff: u32,
        part_distance_cutoff: u32,
    ) -> (bool, CellID, u32) {
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());
        let mut total_score = 0;

        //Loop across each barcode round
        for p in &self.pools {
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
            full_bc_index.push(this_bc);
            total_score = total_score + score;

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if abort_early && score > part_distance_cutoff {
                return (false, self.bcidvec_to_string(&full_bc_index), total_score);
            }
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);

        //All barcodes collected. Check if total mismatch is ok
        if total_score > total_distance_cutoff {
            //println!("Late BC abort for total score {}", total_score);
            return (false, cellid, total_score);
        } else {
            return (true, cellid, total_score);
        }
    }

    ///////////////////////////////
    /// Detect barcode only.
    ///
    /// This version only searches for exact matches, ensuring high speed. To be used to see what chemistry is present
    ///
    #[inline(always)]
    pub fn detect_exact_barcode(&self, read_seq: &[u8]) -> (bool, CellID) {
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());

        //Loop across each barcode round
        for p in &self.pools {
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
            full_bc_index.push(this_bc);

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if score > 0 {
                return (false, self.bcidvec_to_string(&full_bc_index));
            }
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);
        return (true, cellid);
    }

    ///////////////////////////////
    /// Convert list of barcode names to cellID
    fn bcidvec_to_string(&self, cell_id: &Vec<usize>) -> CellID {
        //println!("{:?}", cell_id);

        //Get name of barcode from each pool
        let parts_cellid: Vec<String> = cell_id
            .iter()
            .enumerate()
            .map(|(pooli, bc_id)| &self.pools[pooli].barcode_name_list[*bc_id])
            .cloned()
            .collect();

        //Note: : and - are not allowed in cell IDs. this because of the possible use of tabix
        //should support some type of uuencodeing
        parts_cellid.join("_")
    }

    ///////////////////////////////
    /// Read list of barcodes from a TSV file
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode16bp {
        let mut cb: CombinatorialBarcode16bp = CombinatorialBarcode16bp::new();

        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(src);
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                record.well.as_str(),
                record.pos.as_str(),
                record.seq.as_str(),
            );
        }

        if cb.num_pools() == 0 {
            println!("Warning: empty barcodes file");
        }
        cb
    }
}


pub struct DetectedBarcode {
    pub index: u32,
    pub cellid: String,
    pub within_threshold: bool,
    pub score: u32
}

///////////////////////////////
/// A set of barcode positions and sequences, making up a total combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcode16bpFast {
    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String, usize>,

    //Each barcode set in the combination
    pools: Vec<CombinatorialBarcodePart16bpFast>,

    //How much to trim from this read
    pub trim_bcread_len: usize,

    //Location of the UMI
    pub umi_from: usize,
    pub umi_to: usize,
}
impl CombinatorialBarcode16bpFast {
    pub fn new() -> Self {
        CombinatorialBarcode16bpFast {
            map_poolname_to_index: HashMap::new(),
            pools: vec![],
            trim_bcread_len: 0,
            umi_from: 0,
            umi_to: 0,
        }
    }

    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

    pub fn add_pool(&mut self, poolname: &str, pool: CombinatorialBarcodePart16bpFast) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index
            .insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    pub fn add_bc(&mut self, name: &str, poolname: &str, sequence: &str) {
        //Create new pool if needed
        if !(self.map_poolname_to_index.contains_key(poolname)) {
            self.add_pool(poolname, CombinatorialBarcodePart16bpFast::new());
        }

        let pool_index = self
            .map_poolname_to_index
            .get(poolname)
            .expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart16bpFast =
            self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }

    ///////////////////////////////
    /// Detect barcode only
    #[inline(always)]
    pub fn detect_barcode(
        &self,
        read_seq: &[u8],
        abort_early: bool,
        total_distance_cutoff: u32,
        part_distance_cutoff: u32,
    ) -> DetectedBarcode {
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());
        let mut scores: Vec<u32> = Vec::with_capacity(self.num_pools());
        let mut total_score = 0;

        //Loop across each barcode round
        for p in &self.pools {
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
            // We cast this a lot so this is just to be sure.
            assert!(this_bc < (u32::MAX-1) as usize);
            full_bc_index.push(this_bc);
            scores.push(score);
            total_score = total_score + score;

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if abort_early && score > part_distance_cutoff {
                return DetectedBarcode { index: this_bc as u32, cellid: self.bcidvec_to_string(&full_bc_index), within_threshold: false, score };
            }
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);

        //All barcodes collected. Check if total mismatch is ok
        if total_score > total_distance_cutoff {
            //println!("Late BC abort for total score {}", total_score);
            // TODO validate that we select the correct full_bc_index here.
            return DetectedBarcode { index: full_bc_index[0] as u32, cellid, within_threshold: false, score: scores[0] };
        } else {
            return DetectedBarcode { index: full_bc_index[0] as u32, cellid, within_threshold: true, score: scores[0] };
        }
        
    }

    ///////////////////////////////
    /// Detect barcode only.
    ///
    /// This version only searches for exact matches, ensuring high speed. To be used to see what chemistry is present
    ///
    #[inline(always)]
    pub fn detect_exact_barcode(&self, read_seq: &[u8]) -> (bool, CellID) {
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());

        //Loop across each barcode round
        for p in &self.pools {
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
            full_bc_index.push(this_bc);

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if score > 0 {
                return (false, self.bcidvec_to_string(&full_bc_index));
            }
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);
        return (true, cellid);
    }

    ///////////////////////////////
    /// Convert list of barcode names to cellID
    fn bcidvec_to_string(&self, cell_id: &Vec<usize>) -> CellID {
        //println!("{:?}", cell_id);

        //Get name of barcode from each pool
        let parts_cellid: Vec<String> = cell_id
            .iter()
            .enumerate()
            .map(|(pooli, bc_id)| &self.pools[pooli].barcode_name_list[*bc_id])
            .cloned()
            .collect();

        //Note: : and - are not allowed in cell IDs. this because of the possible use of tabix
        //should support some type of uuencodeing
        parts_cellid.join("_")
    }

    ///////////////////////////////
    /// Read list of barcodes from a TSV file
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode16bpFast {
        let mut cb = CombinatorialBarcode16bpFast::new();

        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(src);
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                record.well.as_str(),
                record.pos.as_str(),
                record.seq.as_str(),
            );
        }

        if cb.num_pools() == 0 {
            println!("Warning: empty barcodes file");
        }
        cb
    }
}

/// A search hit.
struct Hit {
    /// Index into first half/vec of vecs.
    primary_index: usize,
    /// Index into the full barcodes within the inner vecs
    secondary_index: usize,
    /// Hamming distance
    score: u32
}


#[derive(Clone, Debug)]
pub struct CombinatorialBarcodePart16bpFast {
    /// First u16 of each barcode, mapped to an index into full_barcodes and first_halves.
    unique_first_halves: HashMap<u16, usize>,

    /// Flat array of all unique first u16's of each barcode.
    first_halves: Vec<u16>,

    /// Array of arrays of full barcodes corresponding to each unique first half.
    full_barcodes: Vec<Vec<u32>>,

    /// Maps a primary and secondary index to an index into all_barcodes
    full_barcodes_indices: Vec<Vec<usize>>,

    /// Flat array of all unique full barcodes in arbitrairy order.
    all_barcodes: Vec<u32>,

    pub barcode_name_list: Vec<String>,

    pub quick_testpos: u32,
    pub all_test_pos: Vec<u32>
}

impl CombinatorialBarcodePart16bpFast {

    fn to_compact(barcode: &[u8]) -> u32 {
        const COMPACT_BASE_A: u8 = 0b00;
        const COMPACT_BASE_C: u8 = 0b01;
        const COMPACT_BASE_G: u8 = 0b10;
        const COMPACT_BASE_T: u8 = 0b11;

        const ASCII_A: u8 = 'A' as u8;
        const ASCII_C: u8 = 'C' as u8;
        const ASCII_G: u8 = 'G' as u8;
        const ASCII_T: u8 = 'T' as u8;
        const ASCII_N: u8 = 'N' as u8;

        
        const fn ascii_to_compact(a: u8) -> u8 {
            match a {
                ASCII_A => COMPACT_BASE_A,
                ASCII_C => COMPACT_BASE_C,
                ASCII_G => COMPACT_BASE_G,
                ASCII_T => COMPACT_BASE_T,
                _ => panic!("Not possible"),
            }
        }

        fn compact_to_char(a: u8) -> char {
            match a & 0b11 {
                COMPACT_BASE_A => 'A',
                COMPACT_BASE_C => 'C',
                COMPACT_BASE_G => 'G',
                COMPACT_BASE_T => 'T',
                _ => panic!("Invalid"),
            }
        }

        assert!(barcode.len() >= 16);

        let mut bits: u32 = 0;
        for (i, mut base) in barcode.iter().take(16).copied().enumerate() {
            if base == ASCII_N {
                base = ASCII_A;
            }
            bits |= (ascii_to_compact(base) as u32) << (i * 2);
        }

        bits
        
    }

    fn get_first_half(full: u32) -> u16 {
        (full & 0xffff) as u16
    }

    fn hamming_half(a: u16, b: u16) -> u32 {
        let matching = a ^ b;

        let odd_mask: u16 = 0x5555;
        let even_mask: u16 = 0xaaaa;

        let odd = matching & odd_mask;
        let even = (matching & even_mask) >> 1;

        let matched_symbols = odd | even;

        let number_mismatched = matched_symbols.count_ones();
        number_mismatched
    }

    fn hamming_full(a: u32, b: u32) -> u32 {
        let matching = a ^ b;

        let odd_mask = 0x55555555;
        let even_mask = 0xaaaaaaaa;

        let odd = matching & odd_mask;
        let even = (matching & even_mask) >> 1;

        let matched_symbols = odd | even;

        let number_mismatched = matched_symbols.count_ones();
        number_mismatched
    }


    fn simd_count_mismatches16(a: HalfVector, b: HalfVector) -> HalfVector {
        let matching = a ^ b;

        let odd_mask = HalfVector::splat(0x5555u16);
        let even_mask = HalfVector::splat(0xaaaau16);

        let odd = matching & odd_mask;
        let even = (matching & even_mask) >> 1;

        let matched_symbols: HalfVector = odd | even;

        let mut counts: HalfVector = HalfVector::splat(0);
        for (u, c) in matched_symbols
            .as_array()
            .iter()
            .zip(counts.as_mut_array().iter_mut())
        {
            *c = u.count_ones().try_into().unwrap();
        }

        counts
    }

    fn simd_count_mismatches32(a: FullVector, b: FullVector) -> FullVector {
        let matching = a ^ b;

        let odd_mask = FullVector::splat(0x55555555u32);
        let even_mask = FullVector::splat(0xaaaaaaaau32);

        let odd = matching & odd_mask;
        let even = (matching & even_mask) >> 1;

        let matched_symbols: FullVector = odd | even;

        let mut counts: FullVector = FullVector::splat(0);
        for (u, c) in matched_symbols
            .as_array()
            .iter()
            .zip(counts.as_mut_array().iter_mut())
        {
            *c = u.count_ones().try_into().unwrap();
        }

        counts
    }

    
    // Assumes 32 wide 16-bit vectors
    // Performs a (hopefully) branchless and faster reduction.
    fn simd_any16(vector: HalfVector) -> bool {
        let mut upper: [u16; 16] = [0; 16];
        let mut lower: [u16; 16] = [0; 16];
        upper.copy_from_slice(&vector.as_array()[0..16]);
        lower.copy_from_slice(&vector.as_array()[16..32]);
        let sum = wide::u16x16::from(upper) | wide::u16x16::from(lower);

        let mut upper: [u16; 8] = [0; 8];
        let mut lower: [u16; 8] = [0; 8];
        upper.copy_from_slice(&sum.as_array()[0..8]);
        lower.copy_from_slice(&sum.as_array()[8..16]);
        let sum = wide::u16x8::from(upper) | wide::u16x8::from(lower);

        sum.to_array().iter().copied().any(|v| v != 0)        
    }


    // Assumes 16 wide 32-bit vectors
    // Performs a (hopefully) branchless and faster reduction.
    fn simd_any32(vector: FullVector) -> bool {
        let mut upper: [u32; 8] = [0; 8];
        let mut lower: [u32; 8] = [0; 8];
        upper.copy_from_slice(&vector.as_array()[0..8]);
        lower.copy_from_slice(&vector.as_array()[8..16]);
        let sum = wide::u32x8::from(upper) | wide::u32x8::from(lower);

        // let mut upper: [u32; 4] = [0; 4];
        // let mut lower: [u32; 4] = [0; 4];
        // upper.copy_from_slice(&sum.as_array()[0..4]);
        // lower.copy_from_slice(&sum.as_array()[4..8]);
        // let sum = wide::u32x4::from(upper) | wide::u32x4::from(lower);

        sum.any()
    }

    fn simd_reduce_min16(vector: HalfVector) -> u16 {
        // let mut upper: [u16; 16] = [0; 16];
        // let mut lower: [u16; 16] = [0; 16];
        // upper.copy_from_slice(&vector.as_array()[0..16]);
        // lower.copy_from_slice(&vector.as_array()[16..32]);
        // let sum = wide::u16x16::from(upper).min(wide::u16x16::from(lower));

        // let mut upper: [u16; 8] = [0; 8];
        // let mut lower: [u16; 8] = [0; 8];
        // upper.copy_from_slice(&sum.as_array()[0..8]);
        // lower.copy_from_slice(&sum.as_array()[8..16]);
        // let sum = wide::u16x8::from(upper).min(wide::u16x8::from(lower));

        // // Unwrap should never trigger since it is from an array some value is always lowest.
        // sum.to_array().iter().copied().min().unwrap()
        vector.as_array().iter().copied().min().unwrap()
    }

    // #[inline(never)]
    // #[cold]
    /// Returns an index and score if a match is found
    #[inline(always)]
    fn scan_for_match(needle: u32, haystack: &[u32], threshold: u32) -> (usize, u32) {
        let mut best_hit = (usize::MAX, u32::MAX);
        let barcode_vector = FullVector::splat(needle);
        for (si, slice) in haystack.chunks_exact(SIMD_FULL_LENGTH).enumerate() {
            let mut vector = FullVector::splat(0);
            vector.as_mut_array().copy_from_slice(slice);

            let counts = Self::simd_count_mismatches32(vector, barcode_vector);
            let any_below = counts.simd_lt(FullVector::splat(threshold + 1)).any();
            
            if any_below {
                // TODO check if this is a vector reduction on avx512? Maybe write manually
                for (i, count) in counts.as_array().iter().copied().enumerate() {
                    if count < best_hit.1 {
                        best_hit.0 = i+si*SIMD_FULL_LENGTH;
                        best_hit.1 = count;
                    }
                }
            }
        }

        let skippable = SIMD_FULL_LENGTH*(haystack.len()/SIMD_FULL_LENGTH);

        // Handle tail
        for (i, bc) in haystack.iter().copied().enumerate().skip(skippable) {
            let distance = Self::hamming_full(bc, needle);
            if distance <= threshold {
                best_hit.0 = i;
                best_hit.1 = distance;
            }
        }

        best_hit
    }
    fn match_single_error(&self, barcode: u32) -> Hit {
        // let threshold: u32 = 1;
        let first = Self::get_first_half(barcode);
        let first_vector = HalfVector::splat(first);

        let mut best_hit = Hit {
            primary_index: usize::MAX,
            secondary_index: usize::MAX,
            score: u32::MAX-1 // Needs -1 because of code in scan_for_match, really only needs to be bigger than 16
        };

        for (ci, (slice, full_slice)) in self.first_halves.chunks_exact(SIMD_LENGTH).zip(self.full_barcodes.chunks_exact(SIMD_LENGTH)).enumerate() {
            let mut vector = HalfVector::splat(0);
            vector.as_mut_array().copy_from_slice(slice);
            
            let counts = Self::simd_count_mismatches16(vector, first_vector);

            if !Self::simd_any16(counts.simd_lt(HalfVector::splat((best_hit.score+1) as u16))) {
                continue;
            }

            // This didn't work. No idea why.
            // let closest_score = Self::simd_reduce_min16(vector);
            // if closest_score >= best_hit.score as u16 {
            //     continue;
            // }
            for (i, (result, potential_matches)) in counts.as_array().iter().copied().zip(full_slice.iter()).enumerate() {
                if result as u32 <= best_hit.score {
                    let found = Self::scan_for_match(barcode, potential_matches, best_hit.score);
                    if found.1 <  best_hit.score {
                        best_hit = Hit {
                            primary_index: ci*SIMD_LENGTH + i,
                            secondary_index: found.0,
                            score: found.1
                        };
                    }
                }
            }
        }

        let non_remainder = SIMD_LENGTH*(self.first_halves.len()/SIMD_LENGTH);

        let iter_chain = self.first_halves.iter().copied().zip(self.full_barcodes.iter()).skip(non_remainder).enumerate();

        for (i, (half, potential)) in iter_chain {
            if Self::hamming_half(first, half) <= best_hit.score {
                let found = Self::scan_for_match(barcode, potential, best_hit.score);
                if found.1 < best_hit.score {
                    best_hit = Hit {
                        primary_index: i,
                        secondary_index: found.0,
                        score: found.1
                    };
                }
            }
        }

        best_hit
    }

    pub fn new() -> Self {
        Self {
            full_barcodes: Vec::new(),
            unique_first_halves: HashMap::new(),
            first_halves: Vec::new(),
            full_barcodes_indices: Vec::new(),
            all_barcodes: Vec::new(),
            barcode_name_list: Vec::new(),
            quick_testpos: 0,
            all_test_pos: Vec::new()
        }
    }

    pub fn add_bc(&mut self, bcname: &str, sequence: &str) {


        let compact = Self::to_compact(sequence.as_bytes());
        let all_index = self.all_barcodes.len();
        self.all_barcodes.push(compact);
        self.barcode_name_list.push(bcname.to_owned());
        assert_eq!(self.all_barcodes.len(), self.barcode_name_list.len());
        
        let first_half = Self::get_first_half(compact);
        let index = self.unique_first_halves.get(&first_half);
        
        if let Some(&index) = index {
            self.full_barcodes[index].push(compact);
            self.full_barcodes_indices[index].push(all_index);
        } else {
            self.unique_first_halves.insert(first_half, self.full_barcodes.len());
            self.full_barcodes.push(vec![compact]);
            self.first_halves.push(first_half);
            self.full_barcodes_indices.push(vec![all_index]);
        }
        assert_eq!(self.full_barcodes.len(), self.first_halves.len());
        assert_eq!(self.first_halves.len(), self.full_barcodes_indices.len());
        
    }


    pub fn detect_barcode(&self, read_seq: &[u8]) -> (usize, u32) {
        let compact = Self::to_compact(read_seq);
        let hit = self.match_single_error(compact);
    
        if hit.score == u32::MAX {
            panic!("No hit found for a barcode round; ensure that there are test positions defined");
        }

        let real_index = self.full_barcodes_indices[hit.primary_index][hit.secondary_index];

        (real_index, hit.score)
    }

}

///////////////////////////////
/// One barcode position, in a combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcodePart16bp {
    pub barcode_seq_list: Vec<u64>,
    pub barcode_name_list: Vec<String>,
    pub seq2barcode: HashMap<u64, usize>, // map to BC index

    pub quick_testpos: usize,
    pub all_test_pos: Vec<usize>,
}
impl CombinatorialBarcodePart16bp {
    pub fn new() -> CombinatorialBarcodePart16bp {
        CombinatorialBarcodePart16bp {
            barcode_seq_list: vec![],
            barcode_name_list: vec![],
            seq2barcode: HashMap::new(),
            quick_testpos: 0,
            all_test_pos: vec![],
        }
    }

    ///////////////////////////////
    /// Add a barcode to this round
    pub fn add_bc(&mut self, bcname: &str, sequence: &str) {
        
        let packed_bc = str_to_barcode_16bp(sequence);
        let bc_id = self.barcode_seq_list.len();

        self.seq2barcode.insert(packed_bc, bc_id);

        self.barcode_seq_list.push(packed_bc);
        self.barcode_name_list.push(bcname.to_string());
    }

    /// Matches the barcode against the set of barcodes.
    /// Returns index of the found barcode and the hamming distance to it.
    pub fn detect_barcode(&self, read_seq: &[u8]) -> (usize, u32) {
        //barcode index, score

        let bc_length = 16;

        //perform optimistic search first!
        //Extract the barcode
        let optimistic_seq = &read_seq[self.quick_testpos..(self.quick_testpos + bc_length)];
        let optimistic_seq = HotEncodeATCGN::encode_16bp(&optimistic_seq);

        if let Some(&i) = self.seq2barcode.get(&optimistic_seq) {
            return (i, 0);
        } else {
            trace!("not a precise match {:?}", optimistic_seq);
        }

        //Find candidate hits. Scan each barcode, in all positions
        let mut all_hits: Vec<(usize, u32)> = Vec::new(); //encoded barcode index, score
        for current_pos in self.all_test_pos.iter() {
            //Extract the barcode for one position
            let optimistic_seq = &read_seq[self.quick_testpos..(current_pos + bc_length)];
            let current_seq = HotEncodeATCGN::encode_16bp(&optimistic_seq);

            //Find best matching barcode
            let (bc_index, bc_distance) = HotEncodeATCGN::closest_by_hamming_u64(
                current_seq,
                self.barcode_seq_list.as_slice(),
            );

            if bc_distance == 0 {
                //If we find a perfect hit then return early, with this barcode. Not clear if this speeds up anymore, or just adds work
                return (bc_index, bc_distance);
            } else {
                //Keep for later comparison
                all_hits.push((bc_index, bc_distance));
            }
        }

        //Return the first hit that is the best one
        let min_entry = all_hits.iter().min_by_key(|&&x| x.1).copied().expect(
            "No hit found for a barcode round; ensure that there are test positions defined",
        );

        return min_entry;
    }
}

///////////////////////////////
/// For serialization: one row in a barcode CSV definition file
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct BarcodeCsvFileRow {
    pos: String,
    well: String,
    seq: String,
}
