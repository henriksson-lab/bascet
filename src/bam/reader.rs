use anyhow::Result;
use rust_htslib::bam;
use rust_htslib::bam::Read;
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
        // not important right now
        // let names: Vec<_> = bam
        //     .header()
        //     .target_names()
        //     .iter()
        //     .map(|n| String::from_utf8_lossy(n).to_string())
        //     .collect();

        let mut record = bam::Record::new();

        while let Some(Ok(())) = bam.read(&mut record) {
            println!("{}", String::from_utf8(record.qname().to_vec()).unwrap())
        }
        Ok(())
    }
}