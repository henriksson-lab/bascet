use anyhow::Result;
use rust_htslib::bam::{self, Read, record::Aux};
use std::collections::HashSet;

// pub struct Chunk {
//     start: 
// }

pub struct Reader {

}
impl Reader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_cell_index<P>(file: P, out: P) -> Result<()>
    where
        P: AsRef<std::path::Path>,
    {
        let mut bam = bam::Reader::from_path(file)?;
        let mut record = bam::Record::new();
        let mut seen_barcodes = HashSet::new();

        while let Some(Ok(())) = bam.read(&mut record) {
            if let Ok(aux) = record.aux(b"CB") {
                if let bam::record::Aux::String(cb_str) = aux {
                    if seen_barcodes.insert(cb_str.to_string()) {
                      
                    } else {
                        println("was not in sequence");
                    }
                }
            }
        }
        Ok(())
    }
}