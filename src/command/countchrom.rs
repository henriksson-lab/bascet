use std::collections::BTreeMap;

use hdf5::types::VarLenUnicode;
use rust_htslib::bam::Read;
use rust_htslib::bam::record::Record as BamRecord;
use rust_htslib::htslib::uint;


use std::path::PathBuf;
use hdf5::File as H5File;


pub struct CountGenomeParams {

//    pub include_cells: Option<Vec<CellID>>,

    pub path_in: std::path::PathBuf,
    //pub path_tmp: std::path::PathBuf,
    pub path_out: std::path::PathBuf,

    pub num_threads: usize

}


type Cellid = Vec<u8>;


pub struct CountChrom { 
}
impl CountChrom {


    pub fn run(
        params: &CountGenomeParams
    ) -> anyhow::Result<()> {


        let mut cnt_mat = SparseCountMatrix::new();

        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads.
        //cannot be TIRF; if we divide up reads we risk double counting
        let mut bam = rust_htslib::bam::Reader::from_path(&params.path_in)?;

        //Activate multithreaded reading
        bam.set_threads(params.num_threads).unwrap();

        //Keep track of last chromosome seen (assuming that file is sorted)
        let mut last_chr:Vec<u8> = Vec::new();        

        //Map cellid -> count. Note that we do not have a list of cellid's at start; we need to harmonize this later
        let mut map_cell_count: BTreeMap<Cellid, uint> = BTreeMap::new();

        let mut num_reads=0;

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf


            //Only keep mapping reads
            let flags = record.flags();
            if flags & 0x4 ==0 {

                let header = bam.header();
                let chr = header.tid2name(record.tid() as u32);

                //Check if we now work on a new chromosome
                if chr!=last_chr {
    
                    //Store counts for this cell
                    if !map_cell_count.is_empty() { //Only empty the first loop

                        if !last_chr.is_empty() { //Do not store empty feature
                            let feature_index = cnt_mat.get_or_create_feature(&last_chr.to_vec());
                            cnt_mat.add_cell_counts(
                                feature_index,
                                 &mut map_cell_count
                            );
                        }
    
                        //println!("{:?}", map_cell_count);
    
                        //Clear buffers, move to the next cell
                        map_cell_count.clear();
                        last_chr=chr.to_vec();
                    }
                } 

                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name = record.qname();
                let mut splitter = read_name.split(|b| *b == b':'); 
                let cell_id = splitter.next().expect("Could not parse cellID from read name");

                //Count this read
                let values = map_cell_count.entry(cell_id.to_vec()).or_insert(0);
                *values += 1;

                //Keep track of where we are
                num_reads+=1;
                if num_reads%1000000 == 0 {
                    println!("Processed {} reads", num_reads);
                }
            }
        }

        //Store counts for this cell
        //Need to check this at the end as well
        if !map_cell_count.is_empty() { //Only empty the first loop
            let feature_index = cnt_mat.get_or_create_feature(&last_chr.to_vec());
            cnt_mat.add_cell_counts(
                feature_index,
                 &mut map_cell_count
            );
        }

        //Save count matrix
        cnt_mat.save_to_anndata(&params.path_out).unwrap();

        Ok(())
    }



    
}

















/**
 * General sparse count matrix
 */
pub struct SparseCountMatrix {

    entries: Vec<(u32,u32,u32)>,   //row, col, count = feature, cell, count

    cell_to_index: BTreeMap<Cellid, uint>,  /// this could easily be a hashset instead TODO
    feature_to_index: BTreeMap<Cellid, uint>,  /// this could easily be a hashset instead TODO
    
    cur_num_cell: u32,
    cur_num_feature: u32,
}
impl SparseCountMatrix {

    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            cell_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            feature_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            cur_num_cell: 0,
            cur_num_feature: 0
        }
    }

    /**
     * Features may have been added before. Try to recover index of cell, or create it
     */
    fn get_or_create_feature(&mut self, id: &Vec<u8>) -> u32 {
        if let Some(i) = self.feature_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_feature;

            //println!("create feature {}",i);
            

            self.feature_to_index.insert(id.clone(), i);
            self.cur_num_feature+=1;
            i
        }
    }


    /**
     * Cells may have been added before. Try to recover index of cell, or create it
     */
    fn get_or_create_cell(&mut self, id: &Vec<u8>) -> u32 {
        if let Some(i) = self.cell_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_cell;
            self.cell_to_index.insert(id.clone(), i);
            self.cur_num_cell+=1;
            i
        }
    }


    /*
     * For a given feature, add counts for a set of cells
     */
    pub fn add_cell_counts(
        &mut self, 
        feature_index: u32, //feature: &String,
        counter: &mut BTreeMap<Cellid, u32>
    ) {
        //let feature_index = self.add_feature(&feature);
        for (cellid, cnt) in counter {
            let cellid_int = self.get_or_create_cell(cellid);
            self.entries.push((feature_index as u32, cellid_int as u32, *cnt)); //feature index should be the "row" - changing the least. cellid changes the most here

        }

    }


    pub fn save_to_anndata(&mut self, p: &PathBuf) -> anyhow::Result<()> {

        log::info!("Sorting counts");
        self.entries.sort(); //Tuples have a sort order defined

        log::info!("Saving count matrix");

        //Delete output file if it exists already; HDF5 library complains otherwise
        if p.exists() {
            std::fs::remove_file(&p).expect("Failed to delete previous output file");
        }
        
        let file = H5File::create(p)?; // open for writing

        //Extract separate vectors
        let csr_data: Vec<u32> = self.entries.iter().map(|(_row,_col,data)| *data).collect();
        let csr_cols: Vec<u32> = self.entries.iter().map(|(_row,col,_data)| *col).collect(); 
        let csr_rows: Vec<u32> = self.entries.iter().map(|(row,_col,_data)| *row).collect(); //must be compressed

        //Figure out where rows start in this list        ////////// This assumes that we added counts, feature by feature //. otherwise sort the array before!!
        let mut ind_ptr:Vec<u32> = Vec::new(); 
        ind_ptr.push(0);
        for i in 1..(csr_rows.len()) {
            if csr_rows[i] != csr_rows[i-1] {
                ind_ptr.push(i as u32);
            }
        }
        //For some reason, also need an entry representing the length of data
        ind_ptr.push((csr_data.len()) as u32);



        //Store the sparse matrix here
        let group = file.create_group("X")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&csr_data.as_slice()).create("data")?;    //Data
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&csr_cols.as_slice()).create("indices")?; // Columns
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&ind_ptr.as_slice()).create("indptr")?;  // Rows

        //Store the matrix size
        let n_rows = self.cur_num_feature;
        let n_cols = self.cur_num_cell;
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&[n_rows,n_cols].as_slice()).create("shape")?;    


        //Store the names of the features
        let list_feature_names = gather_map_to_index(&self.feature_to_index, self.cur_num_feature as usize);
        let group = file.create_group("obs")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.
            with_data(list_feature_names.as_slice()).
            create("_index")?;

        println!("Features {:?}", list_feature_names);

        //Store the names of the cells. Map to an array first
        let list_cell_names = gather_map_to_index(&self.cell_to_index, self.cur_num_cell as usize);
        let group = file.create_group("var")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.
            with_data(list_cell_names.as_slice()).
            create("_index")?;


        Ok(())

    }

}


fn gather_map_to_index(
    map_to_index: &BTreeMap<Cellid, uint>,
    len: usize
) -> Vec<hdf5::types::VarLenUnicode>{
    let mut list_cell_names: Vec<hdf5::types::VarLenUnicode> = vec![VarLenUnicode::new(); len]; // Vec::w();
    for (cellid, cellid_int) in map_to_index {
        list_cell_names[*cellid_int as usize] = listu8_to_h5_string(cellid);
    }
    list_cell_names
}


fn listu8_to_h5_string(list: &Vec<u8>) -> hdf5::types::VarLenUnicode {
    let f=String::from_utf8(list.to_vec()).unwrap();
    f.parse().unwrap()
}
