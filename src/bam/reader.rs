use rust_htslib::bam;
use rust_htslib::bam::Read;
use anyhow::Result;
use std::collections::HashSet;

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
        let names = bam.header().target_names();
        let mut record = bam::Record::new();
        let mut seen = HashSet::new();
        
        while let Some(()) = bam.read(&mut record)? {
            if record.tid() >= 0 {
                let target_name = String::from_utf8_lossy(&names[record.tid() as usize]);
                if seen.insert(target_name.to_string()) {
                    println!("Reference: {}", target_name);
                }
            }
        }

        Ok(())
    }
}