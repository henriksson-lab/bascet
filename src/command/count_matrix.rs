use std::path::PathBuf;

//#[cfg(feature = "blosc")]
//use hdf5::filters::blosc_set_nthreads;  blosc currently disabled (for some reason)
use hdf5::{File, H5Type, Result};
use ndarray::{arr2, s};


/// how to use: https://docs.rs/crate/hdf5/latest

// most active fork: https://github.com/metno/hdf5-rust
// hdf5-metno


#[derive(H5Type, Clone, PartialEq, Debug)] // register with HDF5
#[repr(C)]
pub struct MatrixShape {
    xy: (i64, i64),

}




pub struct SparseCountMatrix {

    pub cells: Vec<String>,
    pub features: Vec<String>,
    pub entries: Vec<(u32,u32,u32)>

}
impl SparseCountMatrix {

    pub fn new() -> Self {
        Self {
            cells: Vec::new(),
            features: Vec::new(),
            entries: Vec::new()
        }
    }

    pub fn add_feature(&mut self, feature: &String){
        self.features.push(feature.clone());
    }

    pub fn add_value(&mut self, cell: u32, feature: u32, value: u32) {
        self.entries.push((cell, feature, value));
    }


    pub fn save_to_hd5(&self, p: &PathBuf) -> anyhow::Result<()> {
        
        let file = File::create(p)?; // open for writing


        //Sparswe matrix here
        let group = file.create_group("X")?; 
        let builder = group.new_dataset_builder();
        let ds = builder.with_data(&[1,2,3]).create("indptr")?;
 //       let ds = builder.with_data(&arr2([1,2,3])).create("indices")?;
//        let ds = builder.with_data(&arr2([1,2,3])).create("data")?;



/* 
        let attr = group.new_attr::<String>().shape([1]).create("encoding-type")?; //allocates the space
        attr.write(&["csr_matrix"])?;

        let attr = group.new_attr::<String>().shape([1]).create("encoding-version")?; //allocates the space
        attr.write(&["0.1.0"])?;
*/

        create_str_attr(&ds, "another_unicode_attribute", "‽🚐")?;


/* 



        //these are attributes
        builder.with_data("csr_matrix").create("encoding-type");
        builder.with_data("csr_matrix").create("encoding-version");
        builder.with_data(&arr2(&[5,3])).create("shape");  //size of matrix

//        builder.cr


        let attr = group.new_attr::<u32>().shape([2]).create("shape")?; //allocates the space
        attr.write(&[5,3])?;
*/


        /* 
        #[cfg(feature = "blosc")]
        blosc_set_nthreads(2); // set number of blosc threads
        #[cfg(feature = "blosc")]
        let builder = builder.blosc_zstd(9, true); // zstd + shuffle
        let ds = builder
            .with_data(&arr2(&[
                // write a 2-D array of data
                [Pixel::new(1, 2, R), Pixel::new(2, 3, B)],
                [Pixel::new(3, 4, G), Pixel::new(4, 5, R)],
                [Pixel::new(5, 6, B), Pixel::new(6, 7, G)],
            ]))
            // finalize and write the dataset
            .create("pixels")?;
    */
        /* 
        // create an attr with fixed shape but don't write the data
        let attr = ds.new_attr::<Color>().shape([3]).create("colors")?;
        // write the attr data
        attr.write(&[R, G, B])?;
        */
        Ok(())

    }



}


fn create_str_attr<T>(location: &T, name: &str, value: &str) -> hdf5::Result<()>
where
    T: std::ops::Deref<Target = hdf5::Container>,
{
    let attr = location.new_attr::<hdf5::types::VarLenUnicode>().create(name)?;
    let value: hdf5::types::VarLenUnicode = value.parse().unwrap();
    attr.write_scalar(&value)
}




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
