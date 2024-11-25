use std::fs::File;
use rust_htslib::bgzf;
use std::io::Read;
use anyhow::Result;

pub struct Reader {}

impl Reader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_cell_index<P>(file: P, out: P) -> Result<()>
    where
        P: AsRef<std::path::Path>
    {
        let mut bam = bam::Reader::from_path(file)?;
        let mut record = bam::Record::new();
        
        while let Some(_) = bam.read(&mut record)? {
            if let Some(ref_id) = record.ref_id() {
                println!("Reference ID: {}", ref_id);
            }
        }

        Ok(())
    }
}