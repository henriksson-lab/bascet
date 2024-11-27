use anyhow::Result;
use rust_htslib::bam::{self, ext::BamRecordExtensions, record::Aux, Read};
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
        let mut bam = rust_htslib::bam::Reader::from_path(file)?;
        let mut record = rust_htslib::bam::Record::new();
        
        loop {
            let start = unsafe { htslib::bgzf_tell(bam.inner()) };
            match bam.read(&mut record) {
                Some(Ok(())) => {
                    let end = unsafe { htslib::bgzf_tell(bam.inner()) };
                    if let Ok(Some(aux)) = record.aux(b"CB") {
                        if let rust_htslib::bam::record::Aux::String(cb_str) = aux {
                            println!("start: {}, end: {}, CB:Z:{}", start, end, cb_str);
                        }
                    }
                },
                None => break,
                Some(Err(e)) => return Err(e),
            }
        }
       
        Ok(())
    }
}
