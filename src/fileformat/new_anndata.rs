
use std::collections::BTreeMap;
use std::path::PathBuf;

use rust_htslib::htslib::uint;

use hdf5::types::VarLenUnicode;
use hdf5::File as H5File;

type Cellid = Vec<u8>;
type Featureid = Vec<u8>;

use sprs::{CsMat, TriMat};


/**
 * Sparse count matrix, aimed at storing as anndata
 * 
 * https://anndata.readthedocs.io/en/stable/
 * 
 * file format
 * https://anndata.readthedocs.io/en/latest/fileformat-prose.html
 * 
 * 
 * note obsm
 * store["obsm/X_pca"]
 * can store countsketch this way
 * 
 * 
 * dataframe: each column is a separate array
 * https://cran.r-project.org/web/packages/anndata/readme/README.html
 * row is one cell
 * 
 * 
 */
pub struct SparseMatrixAnnDataWriter {

    entries: Vec<(u32,u32,u32)>,   //feature, cell, count

    cell_to_index: BTreeMap<Cellid, uint>,  /// this could easily be a hashset instead TODO
    feature_to_index: BTreeMap<Featureid, uint>,  /// this could easily be a hashset instead TODO

    map_cell_unclassified_count: BTreeMap<u32, uint>,  /// this could easily be a hashset instead TODO

    cur_num_cell: u32,
    cur_num_feature: u32,
}
impl SparseMatrixAnnDataWriter {

    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            cell_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            feature_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            map_cell_unclassified_count: BTreeMap::new(),
            cur_num_cell: 0,
            cur_num_feature: 0
        }
    }

    /**
     * Features may have been added before. Try to recover index of cell, or create it
     */
    pub fn get_or_create_feature(
        &mut self, 
        id: &[u8]
    ) -> u32 {
        if let Some(i) = self.feature_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_feature;

            //println!("create feature {}",i);
            

            self.feature_to_index.insert(id.to_vec(), i);
            self.cur_num_feature+=1;
            i
        }
    }


    /**
     * Cells may have been added before. Try to recover index of cell, or create it
     */
    pub fn get_or_create_cell(
        &mut self, 
        id: &[u8]
    ) -> u32 {
        if let Some(i) = self.cell_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_cell;
            self.cell_to_index.insert(id.to_vec(), i);
            self.cur_num_cell+=1;
            i
        }
    }


    /**
     * For a given feature, add counts for a set of cells
     */
    pub fn add_cell_counts_per_cell_name(
        &mut self, 
        feature_index: u32, 
        counter: &mut BTreeMap<Cellid, u32>
    ) {
        //let feature_index = self.add_feature(&feature);
        for (cellid, cnt) in counter {
            let cellid_int = self.get_or_create_cell(cellid);
            self.entries.push((feature_index as u32, cellid_int as u32, *cnt)); 
            
        }
    }

    /**
     * For a given feature, add counts for a set of cells
     */
    pub fn add_cell_counts_per_cell_index(
        &mut self, 
        feature_index: u32, 
        counter: &mut BTreeMap<u32, u32> //map cell_index -> count
    ) {
        //let feature_index = self.add_feature(&feature);
        for (cellid_int, cnt) in counter {
            self.entries.push((feature_index, *cellid_int, *cnt)); 
            
        }
    }



    
    /**
     * For a given cell, add counts for a set of cells
     */
    pub fn add_cell_counts_per_feature_name(
        &mut self, 
        cell_index: u32,
        counter: &mut BTreeMap<Featureid, u32>
    ) {
        for (featureid, cnt) in counter {
            let feature_index = self.get_or_create_feature(featureid);
            self.entries.push((feature_index as u32, cell_index as u32, *cnt)); //feature index should be the "row" - changing the least. cellid changes the most here

        }
    }

    /**
     * For a given cell, add counts for a set of cells
     */
    pub fn add_cell_counts_per_feature_index(
        &mut self, 
        cell_index: u32, 
        counter: &mut BTreeMap<u32, u32> //map feature_index -> count
    ) {
        for (feature_index, cnt) in counter {
            self.entries.push((*feature_index as u32, cell_index as u32, *cnt)); //feature index should be the "row" - changing the least. cellid changes the most here

        }
    }


    /**
     * For a given cell, add unclassified counts
     */
    pub fn add_unclassified(
        &mut self, 
        cell_index: u32,
        counter: u32
    ) {
        self.map_cell_unclassified_count.insert(cell_index, counter);
    }


    /**
     * Sort content and store as andata object
     */
    pub fn save_to_anndata(
        &mut self, 
        p: &PathBuf,
        has_feature_names: bool
    ) -> anyhow::Result<()> {

        /*
         * rows: cells (observations)
         * cols: features (gene, chroms, taix)
         */

        //Figure out the dimensions of the matrix. This is needed for KRAKEN matrices in particular, as no names of the features are given
        let n_rows = self.cur_num_cell;
        let mut n_cols = self.cur_num_feature;
        if n_cols==0 {
            for (feature,_cellid,_cnt) in &self.entries {
                if *feature >= n_cols {
                    n_cols = *feature + 1;
                }
            }
        }

        //Add all entries to this matrix
        let mut trimat = TriMat::new((n_rows as usize, n_cols as usize));
        for (feature,cell,cnt) in &self.entries {
            trimat.add_triplet(
                *cell as usize, 
                *feature as usize, 
                *cnt
            );
        }

        //Convert matrix to Csr format
        let csr_mat: CsMat<_> = trimat.to_csr();



        log::info!("Saving count matrix");

        //Delete output file if it exists already; HDF5 library complains otherwise
        if p.exists() {
            std::fs::remove_file(&p).expect("Failed to delete previous output file");
        }
        
        let file = H5File::create(p)?; // open for writing

        //Extract separate vectors to store
        let mat_indices = csr_mat.indices();
        let mat_data = csr_mat.data();
        let mat_indptr = csr_mat.indptr();
        let mat_indptr = mat_indptr.as_slice().unwrap();

        //Store the sparse matrix here
        let group = file.create_group("X")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_data).create("data")?;    //Data
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indices).create("indices")?; // Columns
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indptr).create("indptr")?;  // Rows

        //Store the matrix size
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&[
            n_rows, //num cells
            n_cols  //num features
        ].as_slice()).create("shape")?;    


        //Store the names of the features, if present
        if has_feature_names {
            let list_feature_names = gather_map_to_index(&self.feature_to_index, self.cur_num_feature as usize);
            let group = file.create_group("var")?; 
            let builder = group.new_dataset_builder();
            let _ = builder.
                with_data(list_feature_names.as_slice()).
                create("_index")?;

            println!("Features {:?}", list_feature_names);
        } 

        //Store the names of the cells. Map to an array first
        let list_cell_names = gather_map_to_index(&self.cell_to_index, self.cur_num_cell as usize);
        let group = file.create_group("obs")?; 
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

/**
 * Helper: convert string to HDF5 variable length unicode
 */
fn listu8_to_h5_string(list: &Vec<u8>) -> hdf5::types::VarLenUnicode {
    let f=String::from_utf8(list.to_vec()).unwrap();
    f.parse().unwrap()
}
