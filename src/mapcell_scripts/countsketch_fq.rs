
use std::path::PathBuf;
use std::env;

use crate::mapcell::CompressionMode;
use crate::mapcell::MissingFileMode;
use crate::mapcell::MapCellFunction;

use crate::kmer::kmc_counter::KmerCounter;


#[derive(Clone, Debug)] 
pub struct MapCellCountSketchFQ {
}
impl MapCellFunction for MapCellCountSketchFQ {

    fn invoke(
        &self,
        input_dir: &PathBuf,
        output_dir: &PathBuf,
        _num_threads: usize
    ) -> anyhow::Result<(bool, String)> {

        //Define files
        let input_file_r1 = input_dir.join("r1.fq"); 
        let input_file_r2 = input_dir.join("r2.fq"); 
        let output_file = output_dir.join("countsketch.txt");

        //Parse parameters
        let kmer_size: usize = get_param_num("KMER_SIZE").unwrap_or(31);
        let sketch_size = get_param_num("SKETCH_SIZE").unwrap_or(100);
        let max_reads = get_param_num("MAX_READS").unwrap_or(100000000);

        log::debug!("Chosen KMER size: {}", kmer_size);
        log::debug!("Chosen sketch size: {}", sketch_size);
        log::debug!("Chosen max reads: {}", max_reads);

/*         println!("Chosen KMER size: {}", kmer_size);
        println!("Chosen sketch size: {}", sketch_size);
        println!("Chosen max reads: {}", max_reads); */

        //Example: novaseq cell of 8M reads - this took quite some time with this function
        let mut sketch = KmerCounter::get_countsketch_fq(
            input_file_r1,
            input_file_r2,
            kmer_size,
            sketch_size,
            max_reads
        ).expect("Could not get countsketch");

        KmerCounter::store_countsketch_seq(
            kmer_size,
            &mut sketch,
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
        expect.push("r1.fq".to_string()); 
        expect.push("r2.fq".to_string()); 
        expect
    }


    fn get_recommend_threads(&self) -> usize {
        1
    }

    fn preflight_check(&self) -> bool {
        // KMER_SIZE must be set
        //get_param_kmer_size().is_some()
        true
    }
}



fn get_param_num(key: &str) -> Option<usize> {
    let val = env::var(key);
    if let Ok(val) = val {
        let num = val.parse::<usize>();
        if let Ok(num) = num {
            Some(num)
        } else {
            panic!("Expected a number for parameter {}, but got {}", key, val);
        }
    } else {
        None
    }
}

