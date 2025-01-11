use std::{path::PathBuf, sync::Arc};
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::io::BufWriter;
use itertools::Itertools;
use std::collections::HashMap;


use crate::fileformat::CellID;
use crate::fileformat::ShardFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::fileformat::shard::ShardCellDictionary;

use crate::utils::check_kmc_tools;

use super::count_matrix::SparseCountMatrix;



//use std::sync::Mutex;

//use rand::rngs::SmallRng;
//use rand::SeedableRng;

//use crate::utils::BoundedMaxHeap;
//use crate::utils::BoundedMinHeap;





pub struct QueryParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
    pub path_features: std::path::PathBuf,  /// called reference for julian


    pub threads_work: usize,  
//    pub threads_work: usize,  

}



/* 

example dump.txt -- if we read one line, we can tell the kmer size

mahogny@beagle:~/github/bascet$ head testdata/features.0.txt  
AAAAAAAAAA AAAAACCCCA CCAGATTAAT C	1
AAAAAAAAAAAAAACCCCACCAGATTAATCT	1
AAAAAAAAAAAAACAACCCCCCAGATTAATC	1
AAAAAAAAAAAAACCCCACCAGATTAATCTC	1
AAAAAAAAAAAAACCCCCCCCCAGATTAATT	1
*/

/**
 * 
 * as input, take total count matrix, pick features that are within a certain percentile. randomize and subset these further to get a good list!
 * 
 * 
 */

pub struct Query {
}
impl Query {


    pub fn run(
        params: &Arc<QueryParams>
    ) -> anyhow::Result<()> {

        check_kmc_tools().unwrap();

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            let _ = fs::create_dir(&params.path_tmp);  
        }

        // TODO: below, should we require instead a plain list of KMERs??

        //Prepare matrix that we will store into
        let mut mm = SparseCountMatrix::new();

        //Below reads list of features to include. Set up a map: KMER => column in matrix.    NOTE: input is the dump.txt from kmc after merging
        //Also figure out what kmer size to use
        let mut features_reference: HashMap<String, usize> = HashMap::new();
        let file_features_ref = File::open(&params.path_features).unwrap();
        let bufreader_features_ref = BufReader::new(&file_features_ref);
        let mut kmer_size = 0;
        for (feature_index, rline) in bufreader_features_ref.lines().enumerate() {  //////////// use CSV parser instead?
            if let Ok(line) = rline { ////// whwn is this false??

                let feature = line.split("\t").next().expect("Could not parse KMER sequence");

                features_reference.insert(feature.to_string(), feature_index + 1); //+1, because matrixmarket counts from 1
                kmer_size = feature.len();

                mm.add_feature(&feature.to_string());
            }
        }

        if kmer_size==0 {
            anyhow::bail!("Feature file has no features");
        }


        //Open file and figure out what cells are present
        //TODO: support for multiple input files!!!!!!!!!!!!!!!!!!!!!!
        let mut file_input = ZipBascetShardReader::new(&params.path_input).expect("Failed to open input file");
        let list_cells = file_input.get_cell_ids().expect("Failed to get content listing for input file");


        // Unzip all cell-specific kmer databases (dump.txt format).   NOTE: this can end up a lot of files! so best to stream!!
        let mut cur_file_id = 0;
      //  let mut dbs_to_merge: Vec<(PathBuf, String)> = Vec::new();
        for cell_id in list_cells {

            //Check if a KMC database is present for this cell, otherwise exclude it
            let list_files = file_input.get_files_for_cell(&cell_id).expect("Could not get list of files for cell");
            let f1="dump.txt".to_string();
            if list_files.contains(&f1) {

          //      let db_file_path = params.path_tmp.join(format!("cell_{}", cur_file_id).to_string());
                let path_f1 = params.path_tmp.join(format!("cell_{}.dump.txt", cur_file_id).to_string());

                //Extract the files
                file_input.extract_as(&cell_id, &f1, &path_f1).unwrap();

                //Add this db to the list of all db's to merge later
                // NOTE: '-' is a unary operator in kmc complex scripts. cannot be part of name
                //dbs_to_merge.push((db_file_path, cell_id));   //// is there any reason to keep cell_id at all?
                cur_file_id+=1;




                //Extract counts from KMC database already here



                //TODO for the right place
                mm.add_value(1, 2, 100);



            } 
        }


        //Write last part of matrix
//        mm.finish();


        Ok(())
    }
}














/* 


pub struct MatrixMM {

    p: PathBuf,
    count_lines_written: i32,
    bufwriter_feature_matrix: BufWriter<File>,
    

    //TODO also 10x format output?

}
impl MatrixMM {

    pub fn new(p: &PathBuf) -> Self {
        //Keep the path for later
        self.p=p.clone();

        /////// Matrix writing and counting are performed at the same time!!
        let file_feature_matrix = File::create(&p).unwrap();
        let mut bufwriter_feature_matrix: BufWriter<&File> = BufWriter::new(&file_feature_matrix);
        let header = "%%MatrixMarket matrix coordinate integer general";
        writeln!(bufwriter_feature_matrix, "{}", header).unwrap();
        writeln!(bufwriter_feature_matrix, "0 0 0").unwrap();
        
        Self {
            p: p.clone(),
            count_lines_written: 0,
            bufwriter_feature_matrix: bufwriter_feature_matrix
        }
    }

    pub fn add(
        &mut self, 
        cell_index: u32,
        feature_index: u32, 
        count: u128
    ) {

        writeln!(
            bufwriter_feature_matrix,
            "\t{} {} {}",
            cell_index, feature_index, count
        )
        .unwrap();
        self.count_lines_written += 1;
    }

    pub fn finish(&mut self){

        let _ = self.bufwriter_feature_matrix.flush();


        //// Go back to the header and fill in the number of features written
        let mut file = OpenOptions::new().write(true).open(&self.path_out).unwrap();
        file.seek(SeekFrom::Start(header.len() as u64 + 1)).unwrap(); // +1 for newline char

        writeln!(
            file,
            "{} {} {}",
            self.features_nmin + self.features_nmax - 1,
            (&queries).iter().map(|(i, _)| i).max().unwrap() - 1,
            count_lines_written - 1
        )
        .unwrap();


    }
}








*/







