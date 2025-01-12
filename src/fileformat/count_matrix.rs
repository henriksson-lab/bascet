use std::path::PathBuf;

//#[cfg(feature = "blosc")]
//use hdf5::filters::blosc_set_nthreads;  blosc currently disabled (for some reason)
use hdf5::File;


// how to use: https://docs.rs/crate/hdf5/latest

// most active fork: https://github.com/metno/hdf5-rust
// hdf5-metno


/*  
#[derive(H5Type, Clone, PartialEq, Debug)] // register with HDF5
#[repr(C)]
struct MatrixShape {
    xy: (i64, i64),
}

*/


pub struct SparseCountMatrix {

    pub cells: Vec<String>,
    pub features: Vec<String>,
    pub entries: Vec<(u32,u32,u32)>   //row, col, count

}
impl SparseCountMatrix {

    pub fn new() -> Self {
        Self {
            cells: Vec::new(),
            features: Vec::new(),
            entries: Vec::new()
        }
    }

    pub fn add_feature(&mut self, feature: &String) -> usize {
        let id = self.features.len();
        self.features.push(feature.clone());
        id as usize
    }

    pub fn add_cell(&mut self, cell: &String) -> usize {
        let id = self.cells.len();
        self.cells.push(cell.clone());
        id as usize
    }

    pub fn add_value(
        &mut self, 
        cell: usize, 
        feature: usize, 
        value: u32
    ) {
        self.entries.push((cell as u32, feature as u32, value));
    }


    pub fn save_to_anndata(&self, p: &PathBuf) -> anyhow::Result<()> {
        
        //Delete output file if it exists already; HDF5 library complains otherwise
        if p.exists() {
            std::fs::remove_file(&p).expect("Failed to delete previous output file");
        }
        
        let file = File::create(p)?; // open for writing

        //Current 
        // V         = [ 10 20 30 40 50 60 70 80 ]
        // COL_INDEX = [  0  1  1  3  2  3  4  5 ]   ///except it is 1-based?? or 0??
        // ROW_INDEX = [  0  2  4  7  8 ]

        // shape is [size of indptr i.e. number of rows   ;   columns ]

        //Extract separate vectors
        let csr_data: Vec<u32> = self.entries.iter().map(|(_row,_col,data)| *data).collect();
        let csr_cols: Vec<u32> = self.entries.iter().map(|(_row,col,_data)| *col).collect();
        let csr_rows: Vec<u32> = self.entries.iter().map(|(row,_col,_data)| *row).collect(); //must be compressed

        //Figure out where rows start in this list        ////////// This assumes that we added counts, cell by cell. otherwise sort the array before!!
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
        let n_rows = self.cells.len();
        let n_cols = self.features.len();
        //let shape = vec![n_rows,n_cols];
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&[n_rows,n_cols].as_slice()).create("shape")?; 

        /* 
        let attr = group.new_attr::<u32>().shape([2]).create("shape")?; //anndata spec says it should be an attribute; but seems hard to read out with hdf5r, and read_10x in seurat cannot handle it
        attr.write(&[n_rows,n_cols])?;
        */

        //Store the format of the matrix
        /* 
        let attr = group.new_attr::<hdf5::types::VarLenUnicode>().create("encoding-type")?;
        let value: hdf5::types::VarLenUnicode = "csr_matrix".parse().unwrap();
        attr.write_scalar(&value);*/

        create_str_attr_unicode_to_group(&group, "encoding-type", "csr_matrix")?;
        create_str_attr_unicode_to_group(&group, "encoding-version", "0.1.0")?;
    

        //Store the names of the cells
        let list_cell_names = vec_to_h5_string(self.cells.as_slice());
        let group = file.create_group("obs")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.
            with_data(list_cell_names.as_slice()).
            create("_index")?;

        //Store the names of the features
        let list_features = vec_to_h5_string(self.features.as_slice());
        let group = file.create_group("var")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.
            with_data(list_features.as_slice()).
            create("_index")?;
        

        Ok(())

    }



}


fn vec_to_h5_string(list: &[String]) -> Vec<hdf5::types::VarLenUnicode> {
    list.iter().map(|f| f.parse().unwrap()).collect()
}


fn create_str_attr_unicode_to_group (group: &hdf5::Group, name: &str, value: &str) -> hdf5::Result<()> {
    let attr = group.new_attr::<hdf5::types::VarLenUnicode>().create(name)?;
    let value: hdf5::types::VarLenUnicode = value.parse().unwrap();
    attr.write_scalar(&value)
}


/* 
fn create_str_attr_unicode<T>(location: &T, name: &str, value: &str) -> hdf5::Result<()>
where
    T: std::ops::Deref<Target = hdf5::Container>,
{
    let attr = location.new_attr::<hdf5::types::VarLenUnicode>().create(name)?;
    let value: hdf5::types::VarLenUnicode = value.parse().unwrap();
    attr.write_scalar(&value)
}
*/



// https://anndata.readthedocs.io/en/latest/fileformat-prose.html
// list(store.keys())
// ['X', 'layers', 'obs', 'obsm', 'obsp', 'uns', 'var', 'varm', 'varp']

// should set attributes
//dict(store.attrs)
//{'encoding-type': 'anndata', 'encoding-version': '0.1.0'}


/*


Dataframe Specification (v0.2.0)
A dataframe MUST be stored as a group

The group’s metadata:

MUST contain the field "_index", whose value is the key of the array to be used as an index/ row labels

MUST contain encoding metadata "encoding-type": "dataframe", "encoding-version": "0.2.0"

MUST contain "column-order" an array of strings denoting the order of column entries

The group MUST contain an array for the index

Each entry in the group MUST correspond to an array with equivalent first dimensions

Each entry SHOULD share chunk sizes (in the HDF5 or zarr container)


*/


/*

An AnnData object MUST be a group.

The group’s metadata MUST include entries: "encoding-type": "anndata", "encoding-version": "0.1.0".

An AnnData group MUST contain entries "obs" and "var", which MUST be dataframes (though this may only have an index with no columns).



The group MAY contain an entry X, which MUST be either a dense or sparse array and whose shape MUST be (n_obs, n_var)

The group MAY contain a mapping layers. Entries in layers MUST be dense or sparse arrays which have shapes (n_obs, n_var)

The group MAY contain a mapping obsm. Entries in obsm MUST be sparse arrays, dense arrays, or dataframes. These entries MUST have a first dimension of size n_obs

The group MAY contain a mapping varm. Entries in varm MUST be sparse arrays, dense arrays, or dataframes. These entries MUST have a first dimension of size n_var

The group MAY contain a mapping obsp. Entries in obsp MUST be sparse or dense arrays. The entries first two dimensions MUST be of size n_obs

The group MAY contain a mapping varp. Entries in varp MUST be sparse or dense arrays. The entries first two dimensions MUST be of size n_var

The group MAY contain a mapping uns. Entries in uns MUST be an anndata encoded type.

*/
