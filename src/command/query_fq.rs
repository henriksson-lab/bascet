use std::sync::{Arc, Mutex};
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::collections::BTreeMap;
use crossbeam::channel::Receiver;

use crate::fileformat::SparseCountMatrix;
use crate::fileformat::{CellID, ReadPair};

type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;



pub struct QueryFqParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
    pub path_features: std::path::PathBuf, 


    pub threads_work: usize,  
}



pub struct QueryFq {
}
impl QueryFq {


    pub fn run(
        params: &Arc<QueryFqParams>
    ) -> anyhow::Result<()> {



        //Prepare matrix that we will store into
        let mut mm = SparseCountMatrix::new();

        //Need to create temp dir
        if params.path_tmp.exists() {
            //todo delete temp dir after run
            anyhow::bail!("Temporary directory '{}' exists already. For safety reasons, this is not allowed. Specify as a subdirectory of an existing directory", params.path_tmp.display());
        } else {
            let _ = fs::create_dir(&params.path_tmp);  
        }



        //Below reads list of features to include. Set up a map: KMER => column in matrix.
        //Also figure out what kmer size to use.
        //Ensure order of KMER in dictionary is the same as the order of columns in matrix
        let mut features_reference: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        let file_features_ref = File::open(&params.path_features).unwrap();
        let bufreader_features_ref = BufReader::new(&file_features_ref);
        let mut kmer_size = 0;

        let mut all_features: Vec<Vec<u8>> = Vec::new();
        for rline in bufreader_features_ref.lines() {
            let feature = rline.unwrap();
            all_features.push(feature.as_bytes().to_vec());
        }
        all_features.sort();

        //Allocate positions in matrix for each feature
        for feature in all_features {
            //Detect kmer size. should be the same for all entries, not checked
            kmer_size = feature.len();

            //Get feature index
            let sfeature = String::from_utf8_lossy(feature.as_slice());
            let feature_index = mm.add_feature(&sfeature.to_string());
            features_reference.insert(feature, feature_index);
        }

        if kmer_size==0 {
            anyhow::bail!("Feature file has no features");
        } else {
            println!("Read {} features. Detected kmer-length of {}", features_reference.len(), kmer_size);
        }



        // Set up channel for sending data, reader => counters
        let n_output=10;
        let thread_pool_write = threadpool::ThreadPool::new(n_output); 
        let (tx_data, rx_data) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(n_output*2);
        let (tx_data, rx_data) = (Arc::new(tx_data), Arc::new(rx_data));
        let mm: Arc<Mutex<SparseCountMatrix>> = Arc::new(Mutex::new(mm));

        //Set up counters
        let features_reference = Arc::new(features_reference);
        for _ in 0..n_output {
            setup_matrix_counter(
                &Arc::clone(&features_reference),
                kmer_size,
                &mm,
                &thread_pool_write,
                &rx_data
            )?;
        }


        //Use streaming API to read all data
        let mut list_input:  Vec<std::path::PathBuf> = Vec::new();
        list_input.push(params.path_input.clone());
        super::transform::create_stream_readers(
            &list_input,
            &tx_data
        ).unwrap();

        //Tell all counters to shut down, then wait for it to happen
        for _ in 0..n_output {
            tx_data.send(None).unwrap();
        }
        thread_pool_write.join();

        //Save the final count matrix
        println!("Storing count table to {}", params.path_output.display());
        let mm=mm.lock().unwrap();
        mm.save_to_anndata(&params.path_output).expect("Failed to save to HDF5 file");


        Ok(())
    }
}






pub fn setup_matrix_counter(
    features_reference: &Arc<BTreeMap<Vec<u8>, usize>>, //Map from feature to index
    kmer_size: usize,
    mm: &Arc<Mutex<SparseCountMatrix>>,
    thread_pool: &threadpool::ThreadPool,
    rx_data: &Arc<Receiver<Option<ListReadWithBarcode>>>,
) -> anyhow::Result<()> {

    let features_reference = Arc::clone(features_reference);
    let mm = Arc::clone(mm);
    let rx_data = Arc::clone(rx_data);

    thread_pool.execute(move || {
        println!("Starting KMER counter process");
        
        while let Ok(Some(dat)) = rx_data.recv() {

            let cell_id=&dat.0;
            let list_reads = &dat.1;

            //A common place to count KMERs
            let mut features_count: BTreeMap<Vec<u8>, usize> = BTreeMap::new();

            for rp in list_reads.iter() {

                count_from_seq(
                    &features_reference,
                    &mut features_count,
                    &rp.r1,
                    kmer_size    
                ).unwrap();

                count_from_seq(
                    &features_reference,
                    &mut features_count,
                    &rp.r2,
                    kmer_size    
                ).unwrap();

            }


            //Lock the matrix and add KMER count for this cell
            let mut mm=mm.lock().unwrap();

            //Add cell ID to matrix and get its matrix position
            let cell_index = mm.add_cell(&cell_id);

            //Add counts to the matrix.
            //The order is guaranteed to be correct given the sorting of entries
            for (feature, cnt) in features_count {
                let feature_index = features_reference.get(&feature).unwrap();
                mm.add_value(cell_index, *feature_index, cnt as u32);  
            }

        }
        println!("Shutting down KMER counter");
    });

    Ok(())
}



pub fn count_from_seq(
    features_reference: &BTreeMap<Vec<u8>, usize>, //Map from feature to index
    features_count: &mut BTreeMap<Vec<u8>, usize>, //Map from feature to count
    seq: &Vec<u8>,
    kmer_size: usize
) -> anyhow::Result<()> {

    //Check for presence of chosen KMERs
    for kmer in seq.windows(kmer_size) {
        if features_reference.contains_key(kmer) {
            *features_count.entry(kmer.to_owned()).or_default() += 1;
        }
    }

    let rc_seq = revcomp(seq);

    //Check for presence of chosen KMERs -- reverse complement
    for kmer in rc_seq.windows(kmer_size) {
        if features_reference.contains_key(kmer) {
            *features_count.entry(kmer.to_owned()).or_default() += 1;
        }
    }

    Ok(())
}


/// Implementation is taken from https://doi.org/10.1101/082214
/// This function handles ATCG
pub fn revcomp(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|c| if c & 2 != 0 { c ^ 4 } else { c ^ 21 })
        .collect()
}