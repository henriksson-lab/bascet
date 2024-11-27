use anyhow::Result;
use rust_htslib::bam;
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
        while let Some(Ok(())) = bam.read(&mut record) {
            println!("{}", record.tid())
        }
        Ok(())
    }
}