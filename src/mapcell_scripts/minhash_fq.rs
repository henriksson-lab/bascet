
use std::path::PathBuf;
use std::env;

use crate::mapcell::CompressionMode;
use crate::mapcell::MissingFileMode;
use crate::mapcell::MapCellFunction;

use crate::kmer::kmc_counter::KmerCounter;


#[derive(Clone, Debug)] 
pub struct MapCellKmcMinHashFQ {
}
impl MapCellFunction for MapCellKmcMinHashFQ {

    fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        _num_threads: usize
    ) -> anyhow::Result<(bool, String)> {

        //Define files
        let input_file_r1 = input_dir.join("r1.fq"); 
        let input_file_r2 = input_dir.join("r2.fq"); 
        let output_file = output_dir.join("minhash.txt");

        //Parse parameters
        let kmer_size: usize = get_param_kmer_size().unwrap_or(31);
        let num_min_hash = get_param_num_minhash().unwrap_or(1000);

        log::debug!("Chosen KMER size: {}", kmer_size);
        log::debug!("Chosen #minhash: {}", num_min_hash);

        let mut min_hash = KmerCounter::extract_fq(
            input_file_r1,
            input_file_r2,
            kmer_size,
            num_min_hash
        ).expect("Could not get minhash");

        KmerCounter::store_minhash(
            kmer_size,
            &mut min_hash,
            &output_file
        );
            
        Ok((true, String::from("")))
    }  

    fn get_missing_file_mode(&self) -> MissingFileMode {
        MissingFileMode::Skip
    }

    fn get_compression_mode(&self, _fname: &str) -> CompressionMode {
        CompressionMode::Default
    }

    fn get_expect_files(&self) -> Vec<String> {
        let mut expect = Vec::new();
        expect.push("kmc_dump.txt".to_string()); 
        expect
    }


    fn preflight_check(&self) -> bool {
        // KMER_SIZE must be set
        //get_param_kmer_size().is_some()
        true
    }
}




fn get_param_kmer_size() -> Option<usize> {
    let key = "KMER_SIZE";
    let val = env::var(key);
    if let Ok(val) = val {
        Some(val.parse::<usize>().unwrap())
    } else {
        None
    }
}



fn get_param_num_minhash() -> Option<usize> {
    let key = "NUM_MINHASH";
    let val = env::var(key);
    if let Ok(val) = val {
        Some(val.parse::<usize>().unwrap())
    } else {
        None
    }
}


