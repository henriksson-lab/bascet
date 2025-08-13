use log::debug;
use std::collections::HashMap;
use std::io::Read;

use crate::fileformat::shard::CellID;
use crate::barcode::parsebio::HotEncodeATCGN;

///////////////////////////////
/// Convert string, assumed to be 16bp, to a packed barcode
pub fn str_to_barcode_16bp(
    sequence: &str,
) -> u64 {
    let bytes = sequence.as_bytes();
    HotEncodeATCGN::encode_16bp(bytes)
}

<<<<<<< HEAD
=======

>>>>>>> main
///////////////////////////////
/// A set of barcode positions and sequences, making up a total combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcode16bp {
<<<<<<< HEAD
    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String, usize>,
=======

    //Maps name of pool to index in array (used during building only)
    map_poolname_to_index: HashMap<String,usize>,
>>>>>>> main

    //Each barcode set in the combination
    pools: Vec<CombinatorialBarcodePart16bp>,

    //How much to trim from this read
    pub trim_bcread_len: usize,

    //Location of the UMI
    pub umi_from: usize,
    pub umi_to: usize,
<<<<<<< HEAD
}
impl CombinatorialBarcode16bp {
    pub fn new() -> CombinatorialBarcode16bp {
=======

}
impl CombinatorialBarcode16bp {

    pub fn new() -> CombinatorialBarcode16bp {

>>>>>>> main
        CombinatorialBarcode16bp {
            map_poolname_to_index: HashMap::new(),
            pools: vec![],
            trim_bcread_len: 0,
            umi_from: 0,
<<<<<<< HEAD
            umi_to: 0,
=======
            umi_to: 0
>>>>>>> main
        }
    }

    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

<<<<<<< HEAD
    pub fn add_pool(&mut self, poolname: &str, pool: CombinatorialBarcodePart16bp) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index
            .insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    pub fn add_bc(&mut self, name: &str, poolname: &str, sequence: &str) {
=======

    pub fn add_pool(
        &mut self,
        poolname: &str,
        pool: CombinatorialBarcodePart16bp
    ) {
        let pool_index = self.pools.len();
        self.map_poolname_to_index.insert(poolname.to_string(), pool_index);
        self.pools.push(pool);
    }

    
    pub fn add_bc(
        &mut self,
        name: &str,
        poolname: &str,
        sequence: &str
    )  {

>>>>>>> main
        //Create new pool if needed
        if !(self.map_poolname_to_index.contains_key(poolname)) {
            self.add_pool(poolname, CombinatorialBarcodePart16bp::new());
        }

<<<<<<< HEAD
        let pool_index = self
            .map_poolname_to_index
            .get(poolname)
            .expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart16bp =
            self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }

=======
        let pool_index = self.map_poolname_to_index.get(poolname).expect("bc index fail");
        let pool: &mut CombinatorialBarcodePart16bp = self.pools.get_mut(*pool_index).expect("get pool fail");
        pool.add_bc(name, sequence);
    }


>>>>>>> main
    ///////////////////////////////
    /// Detect barcode only
    #[inline(always)]
    pub fn detect_barcode(
        &self,
        read_seq: &[u8],
        abort_early: bool,
        total_distance_cutoff: u32,
<<<<<<< HEAD
        part_distance_cutoff: u32,
=======
        part_distance_cutoff: u32
>>>>>>> main
    ) -> (bool, CellID, u32) {
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());
        let mut total_score = 0;

<<<<<<< HEAD
        //Loop across each barcode round
        for p in &self.pools {
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
=======

        //Loop across each barcode round
        for p in &self.pools {

            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(
                read_seq
            );
>>>>>>> main
            full_bc_index.push(this_bc);
            total_score = total_score + score;

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if abort_early && score > part_distance_cutoff {
                //println!("Early BC abort for local score {}", score);
                return (false, self.bcidvec_to_string(&full_bc_index), total_score);
<<<<<<< HEAD
            }
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);

=======
            }            
        }

        let cellid = self.bcidvec_to_string(&full_bc_index);
        
>>>>>>> main
        //All barcodes collected. Check if total mismatch is ok
        if total_score > total_distance_cutoff {
            //println!("Late BC abort for total score {}", total_score);
            return (false, cellid, total_score);
        } else {
            return (true, cellid, total_score);
        }
<<<<<<< HEAD
    }

    ///////////////////////////////
    /// Detect barcode only.
    ///
    /// This version only searches for exact matches, ensuring high speed. To be used to see what chemistry is present
    ///
    #[inline(always)]
    pub fn detect_exact_barcode(&self, read_seq: &[u8]) -> (bool, CellID) {
=======

    }



    ///////////////////////////////
    /// Detect barcode only.
    /// 
    /// This version only searches for exact matches, ensuring high speed. To be used to see what chemistry is present
    /// 
    #[inline(always)]
    pub fn detect_exact_barcode(
        &self,
        read_seq: &[u8],
    ) -> (bool, CellID) {
>>>>>>> main
        let mut full_bc_index: Vec<usize> = Vec::with_capacity(self.num_pools());

        //Loop across each barcode round
        for p in &self.pools {
<<<<<<< HEAD
            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(read_seq);
=======

            //Detect this round BC
            let (this_bc, score) = p.detect_barcode(
                read_seq
            );
>>>>>>> main
            full_bc_index.push(this_bc);

            //If we cannot decode a barcode, abort early. This saves a good % of time
            if score > 0 {
                return (false, self.bcidvec_to_string(&full_bc_index));
<<<<<<< HEAD
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
=======
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
        let parts_cellid:Vec<String> = cell_id.iter().enumerate().map(|(pooli, bc_id)| &self.pools[pooli].barcode_name_list[*bc_id]).cloned().collect();
>>>>>>> main

        //Note: : and - are not allowed in cell IDs. this because of the possible use of tabix
        //should support some type of uuencodeing
        parts_cellid.join("_")
    }

<<<<<<< HEAD
    ///////////////////////////////
    /// Read list of barcodes from a TSV file
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode16bp {
        let mut cb: CombinatorialBarcode16bp = CombinatorialBarcode16bp::new();

        let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').from_reader(src);
=======
    
    ///////////////////////////////
    /// Read list of barcodes from a TSV file
    pub fn read_barcodes(src: impl Read) -> CombinatorialBarcode16bp {

        let mut cb: CombinatorialBarcode16bp = CombinatorialBarcode16bp::new();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(src);
>>>>>>> main
        for result in reader.deserialize() {
            let record: BarcodeCsvFileRow = result.unwrap();

            cb.add_bc(
                record.well.as_str(),
                record.pos.as_str(),
<<<<<<< HEAD
                record.seq.as_str(),
            );
        }

        if cb.num_pools() == 0 {
=======
                record.seq.as_str()
            );
        }

        if cb.num_pools()==0 {
>>>>>>> main
            println!("Warning: empty barcodes file");
        }
        cb
    }
<<<<<<< HEAD
}

=======

}


>>>>>>> main
///////////////////////////////
/// One barcode position, in a combinatorial barcode
#[derive(Clone, Debug)]
pub struct CombinatorialBarcodePart16bp {
<<<<<<< HEAD
    pub barcode_seq_list: Vec<u64>,
    pub barcode_name_list: Vec<String>,
    pub seq2barcode: HashMap<u64, usize>, // map to BC index

    pub quick_testpos: usize,
    pub all_test_pos: Vec<usize>,
}
impl CombinatorialBarcodePart16bp {
=======

    pub barcode_seq_list: Vec<u64>,
    pub barcode_name_list: Vec<String>,
    pub seq2barcode: HashMap<u64,usize>, // map to BC index

    pub quick_testpos: usize,
    pub all_test_pos:Vec<usize>,
}
impl CombinatorialBarcodePart16bp {

>>>>>>> main
    pub fn new() -> CombinatorialBarcodePart16bp {
        CombinatorialBarcodePart16bp {
            barcode_seq_list: vec![],
            barcode_name_list: vec![],
            seq2barcode: HashMap::new(),
            quick_testpos: 0,
<<<<<<< HEAD
            all_test_pos: vec![],
=======
            all_test_pos: vec![]
>>>>>>> main
        }
    }

    ///////////////////////////////
    /// Add a barcode to this round
<<<<<<< HEAD
    pub fn add_bc(&mut self, bcname: &str, sequence: &str) {
=======
    pub fn add_bc(
        &mut self,
        bcname: &str,
        sequence: &str
    ){

>>>>>>> main
        let packed_bc = str_to_barcode_16bp(sequence);
        let bc_id = self.barcode_seq_list.len();

        self.seq2barcode.insert(packed_bc, bc_id);

        self.barcode_seq_list.push(packed_bc);
        self.barcode_name_list.push(bcname.to_string());
    }
<<<<<<< HEAD

    pub fn detect_barcode(&self, read_seq: &[u8]) -> (usize, u32) {
        //barcode index, score
=======
    


    pub fn detect_barcode(
        &self,
        read_seq: &[u8],
    ) -> (usize, u32) { //barcode index, score
>>>>>>> main

        let bc_length = 16;

        //perform optimistic search first!
        //Extract the barcode
        let optimistic_seq = &read_seq[self.quick_testpos..(self.quick_testpos + bc_length)];
        let optimistic_seq = HotEncodeATCGN::encode_16bp(&optimistic_seq);

        if let Some(&i) = self.seq2barcode.get(&optimistic_seq) {
<<<<<<< HEAD
            return (i, 0);
        } else {
            debug!("not a precise match {:?}", optimistic_seq);
        }

        //Find candidate hits. Scan each barcode, in all positions
        let mut all_hits: Vec<(usize, u32)> = Vec::new(); //encoded barcode index, score
        for current_pos in self.all_test_pos.iter() {
=======
            return (i,0);
        } else {
            debug!("not a precise match {:?}",optimistic_seq);
        }

        //Find candidate hits. Scan each barcode, in all positions 
        let mut all_hits: Vec<(usize, u32)> = Vec::new();  //encoded barcode index, score
        for current_pos in self.all_test_pos.iter() {

>>>>>>> main
            //Extract the barcode for one position
            let optimistic_seq = &read_seq[self.quick_testpos..(current_pos + bc_length)];
            let current_seq = HotEncodeATCGN::encode_16bp(&optimistic_seq);

            //Find best matching barcode
<<<<<<< HEAD
            let (bc_index, bc_distance) = HotEncodeATCGN::closest_by_hamming_u64(
                current_seq,
                self.barcode_seq_list.as_slice(),
            );

            if bc_distance == 0 {
=======
            let (bc_index, bc_distance) = HotEncodeATCGN::closest_by_hamming_u64(current_seq, self.barcode_seq_list.as_slice());

            if bc_distance==0 {
>>>>>>> main
                //If we find a perfect hit then return early, with this barcode. Not clear if this speeds up anymore, or just adds work
                return (bc_index, bc_distance);
            } else {
                //Keep for later comparison
                all_hits.push((bc_index, bc_distance));
            }
<<<<<<< HEAD
        }

        //Return the first hit that is the best one
        let min_entry = all_hits.iter().min_by_key(|&&x| x.1).copied().expect(
            "No hit found for a barcode round; ensure that there are test positions defined",
        );

        return min_entry;
    }
}

=======

        }

        //Return the first hit that is the best one
        let min_entry = all_hits
            .iter()
            .min_by_key(|&&x| x.1)
            .copied().expect("No hit found for a barcode round; ensure that there are test positions defined");

        return min_entry;
    }

}




>>>>>>> main
///////////////////////////////
/// For serialization: one row in a barcode CSV definition file
#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct BarcodeCsvFileRow {
    pos: String,
    well: String,
    seq: String,
<<<<<<< HEAD
}
=======
}
>>>>>>> main
