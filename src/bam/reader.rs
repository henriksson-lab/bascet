use std::fs::File;
use rust_htslib::bgzf;
use std::io::Read;

pub struct Reader {}

impl Reader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_cell_index<P>(file: P, out: P) 
    where
        P: AsRef<std::path::Path>
    {
        let mut bgzf = bgzf::Reader::from_path(file).unwrap();
        let mut buffer = [0u8; 4];
        let mut name_buffer = Vec::new();

        while bgzf.read_exact(&mut buffer).is_ok() {
            let name_len = buffer[0] as usize;
            if name_len > 0 {
                name_buffer.resize(name_len, 0);
                if let Ok(_) = bgzf.read_exact(&mut name_buffer) {
                    if let Ok(name) = std::str::from_utf8(&name_buffer[..name_len-1]) {
                        println!("Read name: {}", name);
                    }
                }
            }
        }
    }
}