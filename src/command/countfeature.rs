use clap::Args;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

use std::sync::Arc;
use std::sync::Mutex;

use bio::bio_types::strand::ReqStrand;
use rust_htslib::bam::record::Record as BamRecord;
use rust_htslib::bam::Read;

use noodles::gff::feature::record::Strand;

use crate::umi::umi_dedup::UMIcounter;
use super::determine_thread_counts_1;

use sprs::{CsMat, TriMat};

use crate::fileformat::gff::*;


type Cellid = Vec<u8>;


#[derive(Args)]
pub struct CountFeatureCMD {
    /// BAM or CRAM file; has to be sorted
    #[arg(short = 'i', value_parser)]
    pub path_in: PathBuf,

    /// GFF3 file
    #[arg(short = 'g', value_parser)]
    pub path_gff: PathBuf,

    /// Full path to file to store in
    #[arg(short = 'o', value_parser)]
    pub path_out: PathBuf,

    // Feature to count
    #[arg(long = "use-feature", default_value = "gene")]
    //Not used, but kept here for consistency with other commands
    pub use_feature: String,


    // Attribute id for gene ID
    #[arg(long = "attr-id", default_value = "gene_id")]
    //Not used, but kept here for consistency with other commands
    pub attr_id: String,

    // Attribute id for gene ID
    #[arg(long = "attr-name", default_value = "name")]
    //Not used, but kept here for consistency with other commands
    pub attr_name: String,


    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = "temp")]
    //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl CountFeatureCMD {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        let num_threads_total = determine_thread_counts_1(self.num_threads_total)?;
        println!("Using threads {}", num_threads_total);

        //TODO Can check that input file is sorted via header

        let gff_settings = GFFparseSettings {
            use_feature: self.use_feature.clone(),
            attr_id: self.attr_id.clone(),
            attr_name: self.attr_name.clone(),
        };

        CountFeature::run(
            self.path_in.clone(),
            self.path_gff.clone(),
            self.path_out.clone(),
            gff_settings,
            num_threads_total,
        )?;

        log::info!("CountFeature has finished succesfully");
        Ok(())
    }
}

/*
pub enum BascetStrand {  //equivalent to GFF
    None,
    Forward,
    Reverse,
    Unknown,
}
 */



///
/// Counter known and unknown cells. This is used to reduce memory consumption by avoiding the storage of strings.
/// Known cells already have
/// 
/// an option is to rewrite this using https://docs.rs/flashmap/latest/flashmap/ ; but not sure if possible if we need to generate IDs too
/// 
pub struct CountPerCell {
    pub known_cells: Arc<CellIntMapping>,
    pub counter_known_cell: BTreeMap<u32, u32>,   //Option: store a list (cell, cnt), and presort until later
    pub counter_other_cell: BTreeMap<Vec<u8>, u32>,
}
impl CountPerCell {

    pub fn new(known_cells: Arc<CellIntMapping>) -> CountPerCell {
        CountPerCell {
            known_cells: known_cells,
            counter_known_cell: BTreeMap::new(),
            counter_other_cell: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, cellid: &Vec<u8>, cnt: u32) {
        if let Some(i) = self.known_cells.map_cell_int.get(cellid) {
            self.counter_known_cell.insert(*i as u32, cnt);
        } else {
            self.counter_other_cell.insert(cellid.clone(), cnt);
        }
    }

    fn add_unknown_ids(&mut self, new_cellintmapping: &mut CellIntMapping) {

        //For all unknown cells, update their identity
        for (cellid,cnt) in &self.counter_other_cell {
            if let Some(i) = new_cellintmapping.map_cell_int.get(cellid) {
                //This cell received an ID from another process already, so it can just be inserted
                self.counter_known_cell.insert(*i as u32, *cnt);
            } else {
                //Generate a new ID
                let i = new_cellintmapping.list_cell.len();
                new_cellintmapping.map_cell_int.insert(cellid.clone(),i);

                //Now insert cell in known list
                self.counter_known_cell.insert(i as u32, *cnt);
            }
        }

        //All cells are now known
        self.counter_other_cell.clear();
    }
}





#[derive(Clone)]
pub struct CellIntMapping {
    pub map_cell_int: HashMap<Vec<u8>, usize>,
    pub list_cell: Vec<Vec<u8>>
}
impl CellIntMapping {
    fn new() -> CellIntMapping {
        CellIntMapping {
            map_cell_int: HashMap::new(),
            list_cell: Vec::new()
        }
    }
}



pub struct CountedNamedCells {
//    counters: CountPerCell, // BTreeMap<Vec<u8>, u32>,
    processed_reads: u64,
}


struct CurrentCounterState {
    finished_genes: Vec<(
            GeneMeta,
            BTreeMap<u32, u32>,   //Option: store a list (cell, cnt), and presort until later
        )>,
    processed_reads: u64,
    processed_features: u64,
    num_features: usize,
    current_cellintmapping: Arc<CellIntMapping>
}


///
/// Feature counter. A single thread reads the file while deduplication is performed on separate threads
/// 
pub struct CountFeature {
}
impl CountFeature {

    ///
    /// Initialize a new feature counter
    /// 
    pub fn run(
        path_in: PathBuf,
        path_gff: PathBuf,
        path_out: PathBuf,
        gff_settings: GFFparseSettings,
        num_threads: usize,        
    ) -> anyhow::Result<()> {

        //Check that the input file is present to give a nicer error message before threads start
        if !path_in.exists() {
            anyhow::bail!(format!("Input BAM does not exist: {:?}", path_in));
        }
        let mut path_bam_index = path_in.to_string_lossy().to_string();
        path_bam_index.push_str(".bai");
        let path_bam_index = PathBuf::from(path_bam_index);
        if !path_bam_index.exists() {
            anyhow::bail!(format!("Input BAI does not exist: {:?}", path_bam_index));
        }

        //Parse GFF file
        println!("Reading feature GFF file");
        let gff = FeatureCollection::read_file(
            &path_gff,
            &gff_settings
        )?;


        //Common data for threads
        let current_state = CurrentCounterState {
            finished_genes: Vec::new(),
            processed_reads: 0,
            processed_features: 0,
            num_features: gff.list_feature.len(),
            current_cellintmapping: Arc::new(CellIntMapping::new()),
        };
        let current_state = Arc::new(Mutex::new(current_state));


        //Prepare thread pool
        let thread_pool_work = threadpool::ThreadPool::new(num_threads);
        let (tx, rx) = crossbeam::channel::bounded(num_threads * 2);

        //Prepare to process BAM in parallel
        for tidx in 0..num_threads {
            let rx = rx.clone();
            let current_state = Arc::clone(&current_state);
            let path_in = path_in.clone();

            println!("Starting deduper thread {}", tidx);

            thread_pool_work.execute(move || {
                
                //Open file for reading in this thread. This can fail if file is not there; or index not present. ideally check earlier!
                let mut bam = rust_htslib::bam::IndexedReader::from_path(path_in).unwrap();

                while let Ok(Some(meta)) = rx.recv() {

                    //Get a suitable counter
                    let current_cellintmapping = {
                        let state = current_state.lock().unwrap();
                        Arc::clone(&state.current_cellintmapping)
                    };
                    let mut cell_counter = CountPerCell::new(current_cellintmapping); 

                    //Read BAM file and deduplicate
                    let cnt = Self::process_bam_one_feature(
                        &mut bam,
                        &meta,
                        &mut cell_counter
                    ).expect("Failed to count featuee in BAM");

                    //Put count data into matrix
                    let mut state = current_state.lock().unwrap();


                    if !cell_counter.counter_other_cell.is_empty() {
                        //Need to extend common list of cells with new IDs
                        let mut new_cellintmapping = CellIntMapping::clone(&state.current_cellintmapping);
                        cell_counter.add_unknown_ids(&mut new_cellintmapping);

                        //Ensure other threads use the updated cellIDs
                        state.current_cellintmapping=Arc::new(new_cellintmapping);
                    }

                    //now store counts as-is
                    let known_counter = cell_counter.counter_known_cell;

                    state.finished_genes.push((meta, known_counter));
                    state.processed_reads += cnt.processed_reads;
                    state.processed_features += 1;

                    //Don't print too frequently as this need to lock screen I/O. Should possibly do this one main thread only
                    if state.processed_features % 1000 == 0 {
                        println!(
                            "Processed #features: {} / {}\t#reads: {}", 
                            state.processed_features, 
                            state.num_features, 
                            state.processed_reads
                        );  
                    }
                }
                println!("Ending thread {}", tidx);
            });
        }


        //Ask for each feature to be processed
        for f in &gff.list_feature {
            tx.send(Some(f.clone())).unwrap();
        }

        // Send termination signals to workers, then wait for them to complete
        println!("Shutting down BAM counters");
        for _ in 0..num_threads {
            let _ = tx.send(None);
        }
        thread_pool_work.join();

        println!("Writing count matrix");
        let current_state = current_state.lock().unwrap();
        Self::write_matrix(
            &current_state, 
            &gff,
            &path_out
        )?;

        Ok(())
    }






    /// 
    /// Extract counts for one single feature
    /// 
    fn process_bam_one_feature(
        bam: &mut rust_htslib::bam::IndexedReader,
        meta: &GeneMeta,
        map_cell_count: &mut CountPerCell
    ) -> anyhow::Result<CountedNamedCells> {  

        let mut counters: HashMap<Cellid, CellCounter> = HashMap::new(); 

        let bam_feature = rust_htslib::bam::FetchDefinition::RegionString(meta.gene_chr.as_slice(), meta.gene_start as i64, meta.gene_end as i64);
        bam.fetch(bam_feature).expect(format!("Could not find feature {:?} {} {}", meta.gene_chr, meta.gene_start as i64, meta.gene_end as i64).as_str());


        let mut num_reads: u64 = 0;

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Only keep mapping reads
            let flags = record.flags();
            if flags & 0x4 == 0 {
                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name = record.qname();
                let mut splitter = read_name.split(|b| *b == b':');
                let cell_id = splitter
                    .next()
                    .expect("Could not parse cellID from read name");
                let umi = splitter.next().expect("Could not parse UMI from read name");

                let _strand = match record.strand() {
                    ReqStrand::Forward => Strand::Forward,
                    ReqStrand::Reverse => Strand::Reverse,
                };

                //This gene overlaps, so add to its read count
                let counter = counters
                    .entry(cell_id.into())
                    .or_insert(CellCounter::new());

                //counter.umis.push(umi.into());
                counter.push(umi);

                //Keep track of where we are
                num_reads += 1;
            }


            if num_reads > 10000000 && num_reads%1000000 == 0 {
                
                let chrom = String::from_utf8_lossy(meta.gene_chr.as_slice());
                let gene_id = String::from_utf8_lossy(meta.gene_id.as_slice());

                println!("Suspicious feature with {} reads: {}    {}:{}-{}", num_reads, gene_id, chrom, meta.gene_start, meta.gene_end);
            }
        }


        //Convert UMI to cell counts
//        let mut map_cell_count: BTreeMap<Vec<u8>, u32> = BTreeMap::new();

//        let mut map_cell_count: CountPerCell::new();

        for (cellid, counter) in counters.iter() {

            //Perform UMI deduplication and counting
            let mut prep_data = UMIcounter::prepare_from_str(&counter.umis);
            let cnt = UMIcounter::directional_algorithm(&mut prep_data, 1);
            
            map_cell_count.insert(cellid, cnt);
        }


        Ok(CountedNamedCells {
//            counters: map_cell_count,
            processed_reads: num_reads
        })
    }




    /// Write count matrix to disk
    fn write_matrix(
        state: &CurrentCounterState,
        gff: &FeatureCollection,
        path_out: &PathBuf
    ) -> anyhow::Result<()> {
        
        //Operate on the finished counts from all threads
        let finished_genes = &state.finished_genes;

//        let mut set_cellid = HashSet::new();

        let mut cur_gene_index = 0;
        let mut map_gene_index = HashMap::new();

        //Gather genes and cell names
        println!("- Assign features to matrix index");
        print_mem_usage();
        for g in &gff.list_feature {
            //Give matrix index for genes
            map_gene_index.insert(g.gene_id.to_vec(), cur_gene_index);
            cur_gene_index += 1;

            //Collect cell names
//            for cell_id in map.keys() {
//                set_cellid.insert(cell_id);
//            }
        }

        //Give matrix index for cell names
        /*
        println!("- Get index for cell names");
        print_mem_usage();
        let mut cur_cellid_index = 0;
        let mut map_cellid_index = HashMap::new();
        for cell_id in set_cellid {
            map_cellid_index.insert(cell_id, cur_cellid_index);
            cur_cellid_index += 1;
        }

 */
        
        let num_features = gff.list_feature.len();

        //Proceed to fill in matrix in triplet format.
        //matrix is indexed as [gene,cell]
        println!("- Add triplets");
        print_mem_usage();
        let mut trimat = TriMat::new((cur_gene_index, num_features));
        for (gene, map) in finished_genes.iter() {
            for (cell_id, cnt) in map {
                let g = map_gene_index.get(&gene.gene_id).unwrap();
                //let c = cell_id; //map_cellid_index.get(&cell_id).unwrap();

                trimat.add_triplet(
                    *g, 
                    *cell_id as usize, 
                    *cnt
                );   // is u32 too small? maybe not for kraken etc
            }
        }

        println!("- To CSR format");
        print_mem_usage();
        // This matrix type does not allow computations, and must to
        // converted to a compatible sparse type, using for example
        let compressed_mat: CsMat<_> = trimat.to_csr();

        //TODO store the matrix in a better way
        println!("- Store as matrix market :(");
        print_mem_usage();
        sprs::io::write_matrix_market(&path_out, &compressed_mat)?;

        anyhow::Ok(())
    }

    
}






/// 
/// Counter: cell level
/// 
pub struct CellCounter {
    pub umis: Vec<Vec<u8>>, 


    /////////// TODO. terrible to use Vec<u8>. should we prescan to get most cellIDs, then look the rest up upon need?
    // to avoid plenty locking, we can let threads keep a copied own list of a dictionary so far, then let them make a new
    // dictionary for newly encountered cells. the additional dictionary is merged with a central dictionary at the end
    // of a run (single lock), where no particular merging is needed if the central dictionary has not changed.
    // future BAM files could also get a list of cell names in the beginning with IDs back to it. e.g. CBI:0, Cell Barcode Index.
    // CBI is not compatible with cellSNP though, so we would need to rewrite some tools
    //
    // Since we got a single reader, it could take care of lookup right away?

    // is a hashset counter better? 


}
impl CellCounter {

    fn new() -> CellCounter {
        CellCounter { umis: Vec::new() }
    }

    pub fn push(&mut self, umi: &[u8]) {
        self.umis.push(umi.into());
    }
}



pub fn print_mem_usage(){
    if let Some(usage) = memory_stats::memory_stats() {
//    println!("Current physical memory usage: {}", usage.physical_mem);
    println!("...........Current virtual memory usage: {}", usage.virtual_mem);
    } 
}