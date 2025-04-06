use std::sync::Arc;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::BufRead;
use std::io::BufReader;
use std::collections::HashMap;


use crate::fileformat::ShardRandomFileExtractor;
use crate::fileformat::ZipBascetShardReader;
use crate::fileformat::shard::ShardCellDictionary;
use crate::fileformat::SparseCountMatrix;




pub struct QueryKmcParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
    pub path_features: std::path::PathBuf, 


    pub threads_work: usize,  
}



pub struct QueryKmc {
}
impl QueryKmc {


    pub fn run(
        params: &Arc<QueryKmcParams>
    ) -> anyhow::Result<()> {



        //Prepare matrix that we will store into
        let mut mm = SparseCountMatrix::new();

        //crate::utils::check_kmc_tools().unwrap();

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            println!("Using tempdir {}", params.path_tmp.display());
            if fs::create_dir_all(&params.path_tmp).is_err() {
                panic!("Failed to create temporary directory");
            };  
        }



        //Below reads list of features to include. Set up a map: KMER => column in matrix.
        //Also figure out what kmer size to use
        let mut features_reference: HashMap<String, usize> = HashMap::new();
        let file_features_ref = File::open(&params.path_features).unwrap();
        let bufreader_features_ref = BufReader::new(&file_features_ref);
        let mut kmer_size = 0;

        for (feature_index, rline) in bufreader_features_ref.lines().enumerate() {  //////////// should be a plain list of features
            if let Ok(feature) = rline { ////// when is this false??
                features_reference.insert(feature.to_string(), feature_index);
                mm.add_feature(&feature.to_string());

                //Detect kmer size. should be the same for all entries, not checked
                kmer_size = feature.len();
            } else {
                println!("one feature line nope");
            }
        }

        if kmer_size==0 {
            anyhow::bail!("Feature file has no features");
        } else {
            println!("Read {} features. Detected kmer-length of {}", features_reference.len(), kmer_size);
        }


        //Open file and figure out what cells are present
        //TODO: support for multiple input files!!!!!!!!!!!!!!!!!!!!!!
        let mut file_input = ZipBascetShardReader::new(&params.path_input).expect("Failed to open input file");
        let list_cells = file_input.get_cell_ids().expect("Failed to get content listing for input file");



/////////////// ABSTRACTION: could enable the kmer counting over FASTQ and contigs as well. instead of looking up kmers, it would be counted de novo

        // Unzip all cell-specific kmer databases (dump.txt format).   NOTE: this can end up a lot of files! so best to stream!!
        for cell_id in list_cells {

            println!("doing cell {}", cell_id);

            //Add cell ID to matrix and get its matrix position
            let cell_index = mm.add_cell(&cell_id);

            //Check if a KMC database is present for this cell, otherwise exclude it
            let list_files = file_input.get_files_for_cell(&cell_id).expect("Could not get list of files for cell");

            let f1="kmc_dump.txt".to_string();
            if list_files.contains(&f1) {

                //println!("has dump");

                //Extract the files
                let path_f1 = params.path_tmp.join(format!("cell_{}.kmc_dump.txt", cell_id).to_string());
                file_input.extract_as(&cell_id, &f1, &path_f1).unwrap();

                //Extract counts from KMC database already here
                //TODO maybe for now get the dump.txt file and scan it directly... later, C++ api for kmc should be the fastest option!!!

                let file_features_ref = File::open(&path_f1).unwrap();
                let mut reader = BufReader::new(&file_features_ref);
                count_from_dump(
                    cell_index,
                    &features_reference,
                    &mut mm,
                    &mut reader
                );
            } else {
                println!("No kmc_dump.txt present; File list: {:?}", list_files);
            }
        }

        //Save the final count matrix
        println!("Storing count table to {}", params.path_output.display());
        mm.save_to_anndata(&params.path_output).expect("Failed to save to HDF5 file");

        //TODO delete temp files
        println!("Cleaning up temp files");
        //fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }
}



pub fn count_from_dump(
    cell_index: usize,
    features_reference: &HashMap<String, usize>,
    mm: &mut SparseCountMatrix,
    reader: &mut BufReader<impl Read>
){
    for (_feature_index, rline) in reader.lines().enumerate() {
        if let Ok(line) = rline { ////// when is this false??
            //println!("line ok");

            let mut splitter = line.split("\t");
            let feature = splitter.next().expect("Could not parse KMER sequence from cell db");

            if let Some(feature_index) = features_reference.get(feature) {
                let cnt = splitter.next().expect("Could not parse KMER count from cell db").
                    parse::<u32>().expect("Count for kmer is not a u32");

                mm.add_value(cell_index, *feature_index, cnt);  
            }
        } else {
            println!("line failed");
        }
    }
}







