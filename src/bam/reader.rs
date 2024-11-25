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
        let mut bgzf = bgzf::Reader::from_path(file)?;
        
        // Read BAM magic
        let mut magic = [0u8; 4];
        bgzf.read_exact(&mut magic)?;
        assert_eq!(&magic, b"BAM\x01");

        // Skip header text
        let mut header_len = [0u8; 4];
        bgzf.read_exact(&mut header_len)?;
        let l_text = i32::from_le_bytes(header_len);
        let mut header = vec![0u8; l_text as usize];
        bgzf.read_exact(&mut header)?;

        // Skip references
        let mut n_ref_buf = [0u8; 4];
        bgzf.read_exact(&mut n_ref_buf)?;
        let n_ref = i32::from_le_bytes(n_ref_buf);
        
        for _ in 0..n_ref {
            // Read reference name length
            let mut l_name_buf = [0u8; 4];
            bgzf.read_exact(&mut l_name_buf)?;
            let l_name = i32::from_le_bytes(l_name_buf);
            
            // Skip name and length
            let mut ref_data = vec![0u8; l_name as usize + 4];  // +4 for length field
            bgzf.read_exact(&mut ref_data)?;
        }

        // Now read alignment records
        let mut block_size_buf = [0u8; 4];
        
        while bgzf.read_exact(&mut block_size_buf).is_ok() {
            let block_size = i32::from_le_bytes(block_size_buf);
            if block_size <= 0 { break; }
            
            // Read fixed-length record fields
            let mut fixed_data = [0u8; 32];
            bgzf.read_exact(&mut fixed_data)?;
            
            // Get name length (includes null terminator)
            let l_read_name = fixed_data[8] as usize;
            
            // Read name
            let mut name = vec![0u8; l_read_name];
            bgzf.read_exact(&mut name)?;
            
            // Print name (excluding null terminator)
            if l_read_name > 0 {
                println!("Read name: {}", String::from_utf8_lossy(&name[..l_read_name-1]));
            }
            
            // Skip rest of record
            let remaining = block_size as usize - 32 - l_read_name;
            let mut remaining_data = vec![0u8; remaining];
            bgzf.read_exact(&mut remaining_data)?;
        }

        Ok(())
    }
}