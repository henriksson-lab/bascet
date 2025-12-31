use gxhash::HashMapExt;
use log::debug;
use std::collections::HashMap;
use std::io::Read;

use crate::barcode::parsebio::HotEncodeATCGN;
use crate::fileformat::shard::CellID;

///////////////////////////////
/// Convert string, assumed to be 8bp, to a packed barcode
pub fn str_to_barcode_8bp(seq: &str) -> u32 {
    const BC_LEN: usize = 8;
    assert!(
        BC_LEN >= seq.len(),
        "Short read (read len < barcode pool item len) encountered (seq: {seq})"
    );

    let bytes: [u8; BC_LEN] = unsafe { std::ptr::read(seq.as_ptr() as *const [u8; BC_LEN]) };
    return HotEncodeATCGN::encode_8bp(&bytes);
}

///////////////////////////////
/// A set of barcode positions and sequences, making up a total combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcode8bp {
    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String, usize>,

    //Each barcode set in the combination
    pub pools: Vec<CombinatorialBarcodePart8bp>,

    //How much to trim from this read
    pub trim_bcread_len: usize,

    //Location of the UMI
    pub umi_from: usize,
    pub umi_to: usize,
}
// unsafe impl Send for CombinatorialBarcode8bp {}

impl CombinatorialBarcode8bp {
    pub fn new() -> CombinatorialBarcode8bp {
        CombinatorialBarcode8bp {
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

    pub fn add_pool(&mut self, poolname: &str, pool: CombinatorialBarcodePart8bp) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index
            .insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    pub fn add_bc(&mut self, name: &str, poolname: &str, sequence: &str) {
        //Create new pool if needed
        if !(self.map_poolname_to_index.contains_key(poolname)) {
            self.add_pool(poolname, CombinatorialBarcodePart8bp::new());
        }

        let pool_index = self
            .map_poolname_to_index
            .get(poolname)
            .expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart8bp =
            self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }

    ///////////////////////////////
    /// Detect barcode only
    #[inline(always)]
    pub fn _depreciated_detect_barcode(
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
            let (this_bc, score) = p._depreciated_detect_barcode(read_seq);
            full_bc_index.push(this_bc);
            total_score = total_score + score;

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if abort_early && score > part_distance_cutoff {
                //println!("Early BC abort for local score {}", score);
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

    #[inline(always)]
    pub fn detect_barcode(
        &self,
        read_seq: &[u8],
        abort_early: bool,
        total_distance_cutoff: u8,
        part_distance_cutoff: u8,
    ) -> (u32, i8) {
        // 4^8 = u16::MAX, therefore u16 is the largest necessary type
        let mut full_bc_index: u32 = 0;
        let mut total_score = 0;
        let len = self.pools.len();
        //Loop across each barcode round
        for (i, p) in self.pools.iter().enumerate() {
            //Detect this round BC
            let (bc, score) = p.detect_barcode(read_seq);
            assert!(bc <= u32::MAX as usize);

            let shift = (len - 1 - i) * 8;
            full_bc_index |= (bc as u32) << shift;
            total_score += score;

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if abort_early && score > part_distance_cutoff {
                //println!("Early BC abort for local score {}", score);
                return (full_bc_index, total_score as i8);
            }
        }
        //All barcodes collected. Check if total mismatch is ok
        if total_score > total_distance_cutoff {
            //println!("Late BC abort for total score {}", total_score);
            return (full_bc_index, -1);
        } else {
            return (full_bc_index, total_score as i8);
        }
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
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode8bp {
        let mut cb: CombinatorialBarcode8bp = CombinatorialBarcode8bp::new();

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

///////////////////////////////
/// One barcode position, in a combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcodePart8bp {
    pub barcode_seq_list: Vec<u32>,
    pub barcode_name_list: Vec<String>,
    pub seq2barcode: gxhash::HashMap<u32, usize>, // map to BC index

    pub pos_anchor: usize,
    pub pos_rel_anchor: Vec<usize>,
}
impl CombinatorialBarcodePart8bp {
    pub fn new() -> CombinatorialBarcodePart8bp {
        CombinatorialBarcodePart8bp {
            barcode_seq_list: vec![],
            barcode_name_list: vec![],
            seq2barcode: gxhash::HashMap::new(),
            pos_anchor: 0,
            pos_rel_anchor: vec![],
        }
    }

    ///////////////////////////////
    /// Add a barcode to this round
    pub fn add_bc(&mut self, bcname: &str, sequence: &str) {
        let packed_bc = str_to_barcode_8bp(sequence);
        let bc_id = self.barcode_seq_list.len();
        // assert!(
        //     bc_id <= u8::MAX as usize,
        //     "barcode ID {} exceeds u8::MAX",
        //     bc_id
        // );

        self.seq2barcode.insert(packed_bc, bc_id);
        self.barcode_seq_list.push(packed_bc);
        self.barcode_name_list.push(bcname.to_string());
    }

    pub fn _depreciated_detect_barcode(&self, read_seq: &[u8]) -> (usize, u32) {
        //barcode index, score

        let bc_length = 8;

        //perform optimistic search first!
        //Extract the barcode
        let bytes = &read_seq[self.pos_anchor..(self.pos_anchor + bc_length)];
        let optimistic_seq = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        let optimistic_seq = HotEncodeATCGN::encode_8bp(&optimistic_seq);
        if let Some(&i) = self.seq2barcode.get(&optimistic_seq) {
            return (i, 0);
        } else {
            debug!("not a precise match {:?}", optimistic_seq);
        }

        //Find candidate hits. Scan each barcode, in all positions
        let mut all_hits: Vec<(usize, u32)> = Vec::new(); //encoded barcode index, score
        for current_pos in self.pos_rel_anchor.iter() {
            //Extract the barcode for one position
            let bytes = &read_seq[*current_pos..(current_pos + bc_length)];

            let optimistic_seq = [
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ];
            let current_seq = HotEncodeATCGN::encode_8bp(&optimistic_seq);

            //Find best matching barcode
            let (bc_index, bc_distance) = HotEncodeATCGN::closest_by_hamming_u32(
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

    pub fn detect_barcode(&self, read_seq: &[u8]) -> (usize, u8) {
        //barcode index, score
        const BC_LEN: usize = 8;
        let read_len = read_seq.len();
        let pos_anchor = self.pos_anchor;
        assert!(
            read_seq.len() >= BC_LEN,
            "Short read (read len < barcode pool item len) encountered"
        );

        //perform optimistic search first!
        //Extract the barcode
        let optimistic_seq: [u8; BC_LEN] =
            unsafe { std::ptr::read(read_seq.as_ptr().add(pos_anchor) as *const [u8; BC_LEN]) };
        let optimistic_seq_encoded = HotEncodeATCGN::encode_8bp(&optimistic_seq);
        if let Some(&i) = self.seq2barcode.get(&optimistic_seq_encoded) {
            return (i, 0);
        } else {
            debug!(
                "not a precise match {:?}({:?})",
                optimistic_seq, optimistic_seq_encoded
            );
        }

        //Find candidate hits. Scan each barcode, in all positions
        let mut vec_hits: Vec<(usize, u8)> = Vec::new(); //encoded barcode index, score
        for pos_offset in self.pos_rel_anchor.iter() {
            //Extract the barcode for one position
            if pos_anchor + pos_offset + BC_LEN > read_len {
                continue;
            };
            let current_seq: [u8; BC_LEN] = unsafe {
                std::ptr::read(read_seq.as_ptr().add(pos_anchor + pos_offset) as *const [u8; BC_LEN])
            };
            let current_seq_encoded = HotEncodeATCGN::encode_8bp(&current_seq);

            //Find best matching barcode
            let (res_index, res_distance) = HotEncodeATCGN::fast_closest_by_hamming_u16(
                current_seq_encoded,
                self.barcode_seq_list.as_slice(),
            );

            if res_distance == 0 {
                //If we find a perfect hit then return early, with this barcode. Not clear if this speeds up anymore, or just adds work
                return (res_index, res_distance);
            } else {
                //Keep for later comparison
                vec_hits.push((res_index, res_distance));
            }
        }

        //Return the first hit that is the best one
        let min_entry = vec_hits.iter().min_by_key(|&&x| x.1).copied().expect(
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
