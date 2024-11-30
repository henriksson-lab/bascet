use anyhow::Result;
use rust_htslib::{
    bam::{self, ext::BamRecordExtensions, record::Aux, Read},
    htslib,
};
use std::collections::HashSet;

// pub struct Chunk {
//     start:
// }

pub struct Reader {}
impl Reader {
    pub fn new() -> Self {
        Self {}
    }
    pub fn create_cell_index<P>(file: P, out: P) -> Result<()>
    where
        P: AsRef<std::path::Path>,
    {
        let mut bam = rust_htslib::bam::Reader::from_path(file)?;
        let mut record = rust_htslib::bam::Record::new();

        while let Some(Ok(_)) = bam.read(&mut record) {
            if let Ok(aux) = record.aux(b"CB") {
                if let Aux::String(cb_str) = aux {
                    println!("{:?}", record.qual());
                }
            }
        }

        Ok(())
    }
}
