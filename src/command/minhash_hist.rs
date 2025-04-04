use std::sync::Arc;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::io::BufRead;
use std::collections::HashMap;

use crate::fileformat::CellID;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::fileformat::shard::ShardCellDictionary;


pub struct MinhashHistParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub include_cells: Option<Vec<CellID>>,

    pub threads_work: usize,  

}



pub struct MinhashHist {
}
impl MinhashHist {


    pub fn run(
        params: &Arc<MinhashHistParams>
    ) -> anyhow::Result<()> {

        let mut file_input = ZipBascetShardReader::new(&params.path_input).expect("Failed to open input file");


        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            let _ = fs::create_dir(&params.path_tmp);  
        }

        //TODO need to support multiple shard files as input!!
        //or be prepared to always do one final merge if needed --

        //Pick cells to work on
        let list_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {
            file_input.get_cell_ids().expect("Failed to get content listing for input file")
        };
        println!("Preparing to process {} cells", list_cells.len());

        //Allocate a big array for kmers
        let mut all_kmer: Vec<String> = Vec::with_capacity(list_cells.len()*500);

        // Unzip all cell-specific min-hash specific databases
        let mut cur_file_id = 0;
        for cell_id in &list_cells {

            if cur_file_id%1000 == 0 {
                println!("Processing cell {}", cur_file_id);
            }

            //Check if a minhash is present for this cell, otherwise exclude it
            let list_files = file_input.get_files_for_cell(&cell_id).expect("Could not get list of files for cell");
            let f1="minhash.txt".to_string();
            if list_files.contains(&f1) {

                let path_f1 = params.path_tmp.join(format!("cell_{}.minhash.txt", cur_file_id).to_string());
                file_input.extract_as(&cell_id, &f1, &path_f1).unwrap();

                //Get the content and add to list
                let file = File::open(&path_f1)?;
                let lines = std::io::BufReader::new(file).lines();
                for line in lines {
                    let line = line.unwrap();
                    
                    //Only get the kmer; there can be more columns as well
                    let mut splitter = line.split("\t");
                    let kmer_string = splitter.next().expect("Could not parse KMER sequence from cell db");

                    all_kmer.push(kmer_string.to_string());
                }

                //Delete file when done with it
                std::fs::remove_file(&path_f1)?;
            } 
            cur_file_id = cur_file_id + 1;
        }
        println!("Obtained minhashes from {} cells", list_cells.len());

        //Sort the KMERs; we assume that they are few enough that we can do it in memory for speed
        println!("Counting minhashes");
        let hist = count_element_function(all_kmer);

        //TODO support for multiple input files

        //Write out histogram
        println!("Storing histogram");
        let f=File::create(&params.path_output).expect("Could not open KMER histogram file for writing");
        let mut bw=BufWriter::new(f);
        for (kmer_string,cnt) in hist {
            writeln!(bw, "{}\t{}", &kmer_string, cnt).unwrap();    
        }

        //Delete temp folder
        fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }


    
}




fn count_element_function<I>(it: I) -> HashMap<I::Item, usize>
where
    I: IntoIterator,
    I::Item: Eq + core::hash::Hash,
{
    let mut result = HashMap::new();

    for item in it {
        *result.entry(item).or_insert(0) += 1;
    }

    result
}

