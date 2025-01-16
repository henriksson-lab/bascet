
use std::path::PathBuf;

use crate::mapcell::CompressionMode;
use crate::mapcell::MissingFileMode;
use crate::mapcell::MapCellFunction;

use crate::kmer::kmc_counter::KmerCounterParams;
use crate::kmer::kmc_counter::KmerCounter;



pub struct MapCellKmcMinHash {
}
impl MapCellFunction for MapCellKmcMinHash {

    fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        num_threads: usize
    ) -> anyhow::Result<(bool, String)> {

        let input_file = input_dir.join("kmc_dump.txt");
        let output_file = output_dir.join("minhash.txt");

        let kmer_size = KmerCounter::detect_kmcdump_kmer_size(&input_file);


        let num_min_hash = 1000;  ///////////// TODO: provide as parameter

        if let Ok(kmer_size) = kmer_size {

            log::debug!("Detected KMER size: {}", kmer_size);

            let params = KmerCounterParams {
                path_kmcdump: input_file,
                kmer_size: kmer_size,
                features_nmin: num_min_hash
            };
    
            let mut min_hash = KmerCounter::extract_kmcdump_parallel(&params, num_threads).expect("Could not get minhash");
            KmerCounter::store_minhash(
                kmer_size,
                &mut min_hash,
                &output_file
            );          
            
        }
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


}








