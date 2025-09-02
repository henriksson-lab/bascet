use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use rust_htslib::htslib::uint;

use hdf5::types::VarLenUnicode;
use hdf5::File as H5File;

type Cellid = Vec<u8>;
type Featureid = Vec<u8>;

use sprs::{CsMat, TriMat};

///
/// Builder of AnnData objects
/// 
/// https://anndata.readthedocs.io/en/stable/
///
/// file format
/// https://anndata.readthedocs.io/en/latest/fileformat-prose.html
///
///
/// note obsm
/// store["obsm/X_pca"]
/// can use this to store countsketch
///
/// dataframe: each column is a separate array
/// https://cran.r-project.org/web/packages/anndata/readme/README.html
/// row is one cell
///
pub struct SparseMatrixAnnDataBuilder {
    entries: Vec<(u32, u32, u32)>, //feature, cell, count

    cell_to_index: BTreeMap<Cellid, uint>,
    /// this could easily be a hashset instead TODO
    feature_to_index: BTreeMap<Featureid, uint>,
    /// this could easily be a hashset instead TODO
    map_cell_unclassified_count: BTreeMap<u32, uint>,
    /// this could easily be a hashset instead TODO
    cur_num_cell: u32,
    cur_num_feature: u32,
}
impl SparseMatrixAnnDataBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            cell_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            feature_to_index: BTreeMap::new(), // this could easily be a hashset instead TODO
            map_cell_unclassified_count: BTreeMap::new(),
            cur_num_cell: 0,
            cur_num_feature: 0,
        }
    }

    ///
    /// Features may have been added before. Try to recover index of cell, or create it
    ///
    pub fn get_or_create_feature(&mut self, id: &[u8]) -> u32 {
        if let Some(i) = self.feature_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_feature;
            self.feature_to_index.insert(id.to_vec(), i);
            self.cur_num_feature += 1;
            i
        }
    }

    ///
    /// Cells may have been added before. Try to recover index of cell, or create it
    ///
    pub fn get_or_create_cell(&mut self, id: &[u8]) -> u32 {
        if let Some(i) = self.cell_to_index.get(id) {
            *i
        } else {
            let i = self.cur_num_cell;
            self.cell_to_index.insert(id.to_vec(), i);
            self.cur_num_cell += 1;
            i
        }
    }

    ///
    /// For a given feature, add counts for a set of cells
    ///
    pub fn add_cell_counts_per_cell_name(
        &mut self,
        feature_index: u32,
        counter: &mut BTreeMap<Cellid, u32>,
    ) {
        //let feature_index = self.add_feature(&feature);
        for (cellid, cnt) in counter {
            let cellid_int = self.get_or_create_cell(cellid);
            self.entries
                .push((feature_index as u32, cellid_int as u32, *cnt));
        }
    }

    ///
    /// For a given feature, add counts for a set of cells
    ///
    pub fn add_cell_counts_per_cell_index(
        &mut self,
        feature_index: u32,
        counter: &mut BTreeMap<u32, u32>, //map cell_index -> count
    ) {
        //let feature_index = self.add_feature(&feature);
        for (cellid_int, cnt) in counter {
            self.entries.push((feature_index, *cellid_int, *cnt));
        }
    }

    ///
    /// For a given cell, add counts for a set of cells
    ///
    pub fn add_cell_counts_per_feature_name(
        &mut self,
        cell_index: u32,
        counter: &mut BTreeMap<Featureid, u32>,
    ) {
        for (featureid, cnt) in counter {
            let feature_index = self.get_or_create_feature(featureid);
            self.entries
                .push((feature_index as u32, cell_index as u32, *cnt)); //feature index should be the "row" - changing the least. cellid changes the most here
        }
    }

    ///
    /// For a given cell, add counts for a set of cells
    ///
    pub fn add_cell_counts_per_feature_index(
        &mut self,
        cell_index: u32,
        counter: &mut BTreeMap<u32, u32>, //map feature_index -> count
    ) {
        for (feature_index, cnt) in counter {
            self.entries
                .push((*feature_index as u32, cell_index as u32, *cnt)); //feature index should be the "row" - changing the least. cellid changes the most here
        }
    }

    ///
    /// For a given cell, add counts for a set of cells
    ///
    pub fn add_value_at_index(
        &mut self, 
        feature_index: u32, 
        cell_index: u32, 
        cnt: u32
    ) {
        self.entries.push((feature_index, cell_index, cnt));
    }

    ///
    /// For a given cell, add unclassified counts
    ///
    pub fn add_unclassified(
        &mut self, 
        cell_index: u32, 
        counter: u32
    ) {
        self.map_cell_unclassified_count.insert(cell_index, counter);
    }


    pub fn compress_feature_column(
        &mut self, 
        prefix: &str
    ) -> anyhow::Result<()> {
        //Get all unique feature IDs
        let mut set_taxid = HashSet::new();
        for (feature, _cell, _cnt) in &self.entries {
            set_taxid.insert(*feature);
        }

        //Assign feature IDs
        let mut map_taxid_featureid = HashMap::new();
        for f in set_taxid {
            let feature_name = format!("{}{}", prefix, f);
            //            let fid = self.get_or_create_feature(feature_name.as_bytes());

            let fid = self.cur_num_feature;
            self.feature_to_index
                .insert(feature_name.as_bytes().to_vec(), fid);
            self.cur_num_feature += 1;

            map_taxid_featureid.insert(f, fid);
        }

        //Remap all feature IDs to new space
        for (feature, _cell, _cnt) in self.entries.iter_mut() {
            *feature = *map_taxid_featureid
                .get(feature)
                .expect("Error in feature remapping");
        }

        //row < self.rows /root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/sprs-0.11.3/src/sparse/triplet.rs

        Ok(())
    }


    ///
    /// Sort content and store as andata object
    ///
    pub fn save_to_anndata(
        &mut self, 
        p: &PathBuf
    ) -> anyhow::Result<()> {
        /*
         * rows: cells (observations)
         * cols: features (gene, chroms, taix)
         */

        //Figure out the dimensions of the matrix. This is needed for KRAKEN matrices in particular, as no names of the features are given
        let n_rows = self.cur_num_cell;
        let mut n_cols = self.cur_num_feature;
        if n_cols == 0 {
            for (feature, _cellid, _cnt) in &self.entries {
                if *feature >= n_cols {
                    n_cols = *feature + 1;
                }
            }
        }
        println!(
            "Size of count matrix: {}x{}  (cells x features)",
            n_rows, n_cols
        );

        //Add all entries to this matrix
        let mut trimat = TriMat::new((n_rows as usize, n_cols as usize));
        for (feature, cell, cnt) in &self.entries {
            trimat.add_triplet(
                *cell as usize, ////////////// too few cells when storing queryFq matrix
                *feature as usize,
                *cnt,
            );
        }

        //Convert matrix to Csr format
        let csr_mat: CsMat<_> = trimat.to_csr();

        log::info!("Saving count matrix");


        let mut file = SparseMatrixAnnDataWriter::create_anndata(p)?;

        file.store_sparse_count_matrix(
            &csr_mat,
            n_rows,
            n_cols
        )?;

        //Store the names of the features, if present
        let list_feature_names = Self::gather_map_to_index(
            &self.feature_to_index, 
            self.cur_num_feature as usize
        );
        file.store_feature_names(
            &list_feature_names
        )?;
        

        //println!("Features {:?}", list_feature_names);

        //Store the names of the cells. Map to an array first
        let list_cell_names = Self::gather_map_to_index(&self.cell_to_index, self.cur_num_cell as usize);

        //TODO: storing unmapped count, in "obs" data frame. need new builder?

        //Store count of unmapped
        let mut list_cell_unmapped: Vec<uint> = vec![0; n_rows as usize];
        for (cellid, cellid_int) in &self.map_cell_unclassified_count {
            list_cell_unmapped[*cellid as usize] = *cellid_int;
        }

        file.store_cell_names(
            &list_cell_names,
            Some(&list_cell_unmapped)
        )?;


        Ok(())
    }




    fn gather_map_to_index(
        map_to_index: &BTreeMap<Cellid, uint>,
        len: usize,
    ) -> Vec<hdf5::types::VarLenUnicode> {
        let mut list_cell_names: Vec<hdf5::types::VarLenUnicode> = vec![VarLenUnicode::new(); len]; // Vec::w();
        for (cellid, cellid_int) in map_to_index {
            list_cell_names[*cellid_int as usize] = SparseMatrixAnnDataWriter::listu8_to_h5_string(cellid);
        }
        list_cell_names
    }







}




///
/// Writer for AnnData files, assuming data has already been prepared for writing
/// 
pub struct SparseMatrixAnnDataWriter {
    file: hdf5::File
}
impl SparseMatrixAnnDataWriter {


    ///
    /// x
    /// 
    pub fn create_anndata(
        p: &PathBuf,
    ) -> anyhow::Result<SparseMatrixAnnDataWriter>{
        if p.exists() {
            std::fs::remove_file(&p).expect("Failed to delete previous output file");
        }
        let file = H5File::create(p)?; // open for writing

        Ok(SparseMatrixAnnDataWriter {
            file: file
        })
    }





    ///
    /// x
    /// 
    pub fn store_feature_names(
        &mut self,
        list_feature_names: &Vec<VarLenUnicode>
    ) -> anyhow::Result<()> {
        let group = self.file.create_group("var")?;
        let builder = group.new_dataset_builder();
        let _ = builder
            .with_data(list_feature_names.as_slice())
            .create("_index")?;
        Ok(())
    }



    ///
    /// x
    /// 
    pub fn store_cell_names(
        &mut self,
        list_cell_names: &Vec<VarLenUnicode>,
        list_cell_unmapped: Option<&Vec<uint>>
    ) -> anyhow::Result<()> {
        let group = self.file.create_group("obs")?;

        //Store names of cells
        let builder = group.new_dataset_builder();
        let _ = builder
            .with_data(list_cell_names.as_slice())
            .create("_index")?;

        //Optional: Store count of unmapped reads (property of cell, rather than in count matrix)
        if let Some(list_cell_unmapped) = list_cell_unmapped {
            let builder = group.new_dataset_builder();
            let _ = builder
                .with_data(list_cell_unmapped.as_slice())
                .create("_unmapped")?;
        }

        Ok(())
    }

    
    pub fn csr_mat_u32_to_u16(
        csr_mat: &CsMat<u32>
    ) -> CsMat<u16> {
        let csr_mat: CsMat<u16> = CsMat::new(
            csr_mat.shape().into(),
            csr_mat.indptr().as_slice().unwrap().to_vec(),
            csr_mat.indices().into(),
            csr_mat.data().iter().map(|x| *x as u16).collect()
        );
        csr_mat
    }
    

    ///
    /// Cast the storage type of a CSR matrix
    /// 
    pub fn cast_csr_mat<'a, X,Y> (
        csr_mat: &'a CsMat<X>
    ) -> CsMat<Y> 
    where
        X: Copy,
        Y: TryFrom<X>,
        <Y as TryFrom<X>>::Error: std::fmt::Debug   // is the try_from slow? better implement a specific converter?
//        Y: From<&'a X>
    {
        CsMat::new(
            csr_mat.shape().into(),
            csr_mat.indptr().as_slice().unwrap().to_vec(),
            csr_mat.indices().into(),
            csr_mat.data().iter().map(|x| Y::try_from(*x).unwrap()).collect()
        )
    }

/* 
    /// 
    pub fn store_sparse_count_matrix(
        &mut self,
        csr_mat: &CsMat<u32>,  //u16 is enough in most cases. can try to downscale!
        n_rows: u32,
        n_cols: u32
    ) -> anyhow::Result<()> {


        //Extract separate vectors to store
        let mat_indices = csr_mat.indices();
        let mat_data = csr_mat.data();
        let mat_indptr = csr_mat.indptr();
        let mat_indptr = mat_indptr.as_slice().unwrap();

        //Store the sparse matrix here
        let group = self.file.create_group("X")?;
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_data).create("data")?; //Data
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indices).create("indices")?; // Columns
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indptr).create("indptr")?; // Rows

        //Store the matrix size
        let builder = group.new_dataset_builder();
        let _ = builder
            .with_data(
                &[
                    n_rows, //num cells
                    n_cols, //num features
                ]
                .as_slice(),
            )
            .create("shape")?;
        Ok(())
    }
*/

    ///
    /// x
    /// 
    pub fn store_sparse_count_matrix<X>(
        &mut self,
        csr_mat: &CsMat<X>,  //u16 is enough in most cases. can try to downscale!
        n_rows: u32,
        n_cols: u32
    ) -> anyhow::Result<()> 
    where 
        X: hdf5::H5Type
    {
        //Extract separate vectors to store
        let mat_indices = csr_mat.indices();
        let mat_data = csr_mat.data();
        let mat_indptr = csr_mat.indptr();
        let mat_indptr = mat_indptr.as_slice().unwrap();

        //Store the sparse matrix here
        let group = self.file.create_group("X")?;
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_data).create("data")?; //Data
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indices).create("indices")?; // Columns
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&mat_indptr).create("indptr")?; // Rows

        //Store the matrix size
        let builder = group.new_dataset_builder();
        let _ = builder
            .with_data(
                &[
                    n_rows, //num cells
                    n_cols, //num features
                ]
                .as_slice(),
            )
            .create("shape")?;
        Ok(())
    }


    ///
    /// Helper: convert string to HDF5 variable length unicode
    ///
    pub fn listu8_to_h5_string(list: &Vec<u8>) -> hdf5::types::VarLenUnicode {
        let f = String::from_utf8(list.to_vec()).unwrap();
        f.parse().unwrap()
    }

    ///
    /// Helper: convert list of strings to HDF5
    ///
    pub fn list_string_to_h5(
        list: &Vec<Vec<u8>>
    ) -> Vec<hdf5::types::VarLenUnicode> {
        list.iter().map(|x| Self::listu8_to_h5_string(x)).collect()
    }

    

}
