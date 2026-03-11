use std::{fs::File, io::{BufWriter, Write}, path::PathBuf};

use crate::fileformat::{ReadPair, ShardFileExtractor};


///
/// Extractor for readpairs already kept in memory. Nonideal in terms of streaming but simple drop-in solution
/// 
pub struct ShardFileExtractorInmem {
    pub cellid: String,
    pub rp: Vec<ReadPair>,
}
impl ShardFileExtractor for ShardFileExtractorInmem {



    fn extract_as(&mut self, file_name: &String, path_outfile: &PathBuf) -> anyhow::Result<()> {


        if file_name == "r1.fq" {
            let f = File::create(path_outfile).expect("Could not open r1.fq file for writing");
            let mut writer = BufWriter::new(f);

            for one_read in self.rp.iter() {          
                writer.write_all(b"@x")?;
                //writer.write_all(head.as_slice())?;  //no name of read needed
                writer.write_all(b"\n")?;
                writer.write_all(one_read.r1.as_slice())?;
                writer.write_all(b"\n+\n")?;
                writer.write_all(&one_read.q1.as_slice())?;
                writer.write_all(b"\n")?;
            }
            anyhow::Ok(())
        } else if file_name == "r2.fq" {
            let f = File::create(path_outfile).expect("Could not open r2.fq file for writing");
            let mut writer = BufWriter::new(f);

            for one_read in self.rp.iter() {                
                writer.write_all(b"@x")?;
                //writer.write_all(head.as_slice())?;  //no name of read needed
                writer.write_all(b"\n")?;
                writer.write_all(one_read.r2.as_slice())?;
                writer.write_all(b"\n+\n")?;
                writer.write_all(&one_read.q2.as_slice())?;
                writer.write_all(b"\n")?;
            }
            anyhow::Ok(())
        } else {
            anyhow::bail!("File not present")
        }

    }




    fn extract_to_outdir(
        &mut self,
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf,
    ) -> anyhow::Result<bool> {
        for f in needed_files {
            let res = self.extract_as(&f, &out_directory.join(&f));
            if res.is_err() {
                if fail_if_missing {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn get_files_for_cell(&mut self) -> anyhow::Result<Vec<String>> {
        let mut list_files: Vec<String> = Vec::new();
        list_files.push("r1.fq".to_string());
        list_files.push("r2.fq".to_string());
        Ok(list_files)
    }
}

