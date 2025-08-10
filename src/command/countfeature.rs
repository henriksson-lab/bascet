use clap::Args;
use flate2::read::GzDecoder;
use noodles::gff::feature::RecordBuf;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use threadpool::ThreadPool;

use bio::bio_types::strand::ReqStrand;
use rust_htslib::bam::record::Record as BamRecord;
use rust_htslib::bam::Read;

use noodles::gtf as gtf;

use noodles::gff as gff;
use noodles::gff::feature::record::Strand;

use crate::umi::umi_dedup::UMIcounter;
use super::determine_thread_counts_1;

use sprs::{CsMat, TriMat};

type Cellid = Vec<u8>;
type ChromosomeID = Vec<u8>;

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

        CountFeature::new(
            self.path_in.clone(),
            self.path_gff.clone(),
            self.path_out.clone(),
            gff_settings,
            num_threads_total,
        )
        .run()?;

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

pub struct GFFparseSettings {
    pub use_feature: String,
    pub attr_id: String,
    pub attr_name: String,
}

///
/// Feature counter. A single thread reads the file while deduplication is performed on separate threads
/// 
pub struct CountFeature {
    pub path_in: PathBuf,
    pub path_gff: PathBuf,
    pub path_out: PathBuf,
    pub gff_settings: GFFparseSettings,
    pub num_threads: usize,


    thread_pool_work: ThreadPool,
    tx: Sender<Option<GeneCounter>>,
    rx: Receiver<Option<GeneCounter>>,

    ///List of genes that have been finally counted
    finished_genes: Arc<Mutex<Vec<(
        GeneCounter,
        BTreeMap<Vec<u8>, u32>
    )>>>,

}
impl CountFeature {

    ///
    /// Initialize a new feature counter
    /// 
    pub fn new(
        path_in: PathBuf,
        path_gff: PathBuf,
        path_out: PathBuf,
        gff_settings: GFFparseSettings,
        num_threads: usize,        
    ) -> CountFeature {

        //Prepare thread pool
        let thread_pool_work = threadpool::ThreadPool::new(num_threads);
        let (tx, rx) = crossbeam::channel::bounded(num_threads * 3);

        CountFeature {
            path_in: path_in.clone(),
            path_gff: path_gff.clone(),
            path_out: path_out.clone(),
            num_threads: num_threads,

            gff_settings: gff_settings,

            thread_pool_work: thread_pool_work,
            tx: tx,
            rx: rx,

            finished_genes: Arc::new(Mutex::new(Vec::new())),
        }
    }


    /// 
    /// Process on BAM file
    /// 
    fn process_bam(
        &mut self,
        //params: &CountFeature,
        gff: &mut GenomeCounter,
    ) -> anyhow::Result<()> {
        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads.
        //cannot be TIRF; if we divide up reads we risk double counting
        let mut bam = rust_htslib::bam::Reader::from_path(&self.path_in)?;

        //Activate multithreaded reading
        bam.set_threads(self.num_threads).unwrap();

        //Keep track of last chromosome seen (assuming that file is sorted)
        //let mut last_chr:Vec<u8> = Vec::new();

        //Map cellid -> count. Note that we do not have a list of cellid's at start; we need to harmonize this later
        //let mut map_cell_count: BTreeMap<Cellid, uint> = BTreeMap::new();

        let mut num_reads: u64 = 0;

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Only keep mapping reads
            let flags = record.flags();
            if flags & 0x4 == 0 {
                let header = bam.header();
                let chr = header.tid2name(record.tid() as u32);

                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name = record.qname();
                let mut splitter = read_name.split(|b| *b == b':');
                let cell_id = splitter
                    .next()
                    .expect("Could not parse cellID from read name");
                let umi = splitter.next().expect("Could not parse UMI from read name");

                let strand = match record.strand() {
                    ReqStrand::Forward => Strand::Forward,
                    ReqStrand::Reverse => Strand::Reverse,
                };

                gff.count_read(
                    cell_id,
                    umi,
                    chr,
                    record.pos(), //or mpos? TODO
                    record.cigar().end_pos(),
                    strand,
                    &self,
                );

                //Keep track of where we are
                num_reads += 1;
                if num_reads % 1000000 == 0 {
                    println!("Processed {} reads", num_reads);
                }
            }
        }

        Ok(())
    }

    /// Start all deduplication threads and make them ready for processing
    pub fn start_dedupers(&mut self) {
        for tidx in 0..self.num_threads {
            let rx = self.rx.clone();
            let finished_genes = Arc::clone(&self.finished_genes);

            println!("Starting deduper thread {}", tidx);

            self.thread_pool_work.execute(move || {
                while let Ok(Some(gene)) = rx.recv() {
                    //Deduplicate
                    let cnt = gene.get_counts(); 


                    //Put into matrix
                    let mut data = finished_genes.lock().unwrap();
                    data.push((gene, cnt));
                }
                println!("Ending deduper thread {}", tidx);
            });
        }
    }

    /// End deduplication threads
    fn end_dedupers(&self) {
        // Send termination signals to workers, then wait for them to complete
        for _ in 0..self.num_threads {
            let _ = self.tx.send(None);
        }
        self.thread_pool_work.join();
    }

    /// Write count matrix to disk
    fn write_matrix(&self) -> anyhow::Result<()> {
        //Operate on the finished counts from all threads
        let finished_genes = self.finished_genes.lock().unwrap();

        let mut set_cellid = HashSet::new();

        let mut cur_gene_index = 0;
        let mut map_gene_index = HashMap::new();

        //Gather genes and cell names
        for (g, map) in finished_genes.iter() {
            //Give matrix index for genes
            map_gene_index.insert(g.gene_id.to_vec(), cur_gene_index);
            cur_gene_index += 1;

            //Collect cell names
            for cell_id in map.keys() {
                set_cellid.insert(cell_id);
            }
        }

        //Give matrix index for cell names
        let mut cur_cellid_index = 0;
        let mut map_cellid_index = HashMap::new();
        for cell_id in set_cellid {
            map_cellid_index.insert(cell_id, cur_cellid_index);
            cur_cellid_index += 1;
        }

        //Proceed to fill in matrix in triplet format.
        //matrix is indexed as [gene,cell]
        let mut trimat = TriMat::new((cur_gene_index, cur_cellid_index));
        for (gene, map) in finished_genes.iter() {
            for (cell_id, cnt) in map {
                let g = map_gene_index.get(&gene.gene_id).unwrap();
                let c = map_cellid_index.get(&cell_id).unwrap();

                trimat.add_triplet(*g, *c, *cnt);
            }
        }

        // This matrix type does not allow computations, and must to
        // converted to a compatible sparse type, using for example
        let compressed_mat: CsMat<_> = trimat.to_csr();

        //TODO store the matrix in a better way
        sprs::io::write_matrix_market(&self.path_out, &compressed_mat)?;

        anyhow::Ok(())
    }

    /// Run the feature counting algorithm
    pub fn run(&mut self) -> anyhow::Result<()> {
        //Set up counter data structure
        let mut gc = GenomeCounter::read_gff_file(
            &self.path_gff,
            &self.gff_settings
        )?;

        //Start multithreaded deduplicators
        self.start_dedupers();

        //Read file
        //TODO: check if BAM is sorted
        self.process_bam(&mut gc)?;

        //Signal that we are done reading the file
        self.end_dedupers();

        //Write matrix to disk
        self.write_matrix()?;

        Ok(())
    }
}

/// Counter: cell level
pub struct CellCounter {
    pub umis: Vec<Vec<u8>>,
}
impl CellCounter {
    fn new() -> CellCounter {
        CellCounter { umis: Vec::new() }
    }
}

//// Counter: gene level
pub struct GeneCounter {
    pub gene_chr: ChromosomeID,
    pub gene_start: i64,
    pub gene_end: i64,
    pub gene_strand: Strand,

    pub gene_id: Vec<u8>,
    pub gene_name: Vec<u8>,

    pub counters: HashMap<Cellid, CellCounter>,
}
impl GeneCounter {
    fn get_counts(&self) -> BTreeMap<Vec<u8>, u32> { 
        // type inference lets us omit an explicit type signature (which
        // would be `BTreeMap<&str, &str>` in this example).
        let mut map_cell_count: BTreeMap<Vec<u8>, u32> = BTreeMap::new();

        //For each cell
        for (cellid, counter) in self.counters.iter() {


            //Perform UMI deduplication and counting
            let mut prep_data = UMIcounter::prepare_from_str(&counter.umis);
            let cnt = UMIcounter::directional_algorithm(&mut prep_data, 1);
            
            map_cell_count.insert(cellid.clone(), cnt);
        }
        map_cell_count
    }
}

/// Counter: chromosome level
pub struct ChromosomeCounter {
    pub genes: Vec<GeneCounter>,
}
impl ChromosomeCounter {
    /// Create a new chromosome
    pub fn new() -> ChromosomeCounter {
        ChromosomeCounter { genes: Vec::new() }
    }

    /// Add a feature to this chromosome
    pub fn add_feature(&mut self, f: GeneCounter) {
        self.genes.push(f);
    }

    /// Sort features along this chromosome. This must be done before counting starts as
    /// the features must follow the same order as reads appear in the BAM input file
    pub fn sort(&mut self) {
        self.genes.sort_by_key(|e| (e.gene_start, e.gene_end));
        //The first element will be the last element of this vector.
        //This means that items can be popped off at the end in O(1), keeping ownership
        self.genes.reverse();
    }

    /// Signal that this chromosome is done. Thus, finalize all cell counts on this chromosome
    pub fn finish(&mut self, cf: &CountFeature) {
        //For any remaining genes, wrap up
        while let Some(this_gene) = self.genes.pop() {
            cf.tx.send(Some(this_gene)).unwrap();
        }
    }
}

/// Counter: chromosome level
pub struct GenomeCounter {
    chroms: HashMap<Vec<u8>, ChromosomeCounter>,
    last_chrom: Vec<u8>,
    failed_to_get_name: usize
}
impl GenomeCounter {
    pub fn new() -> GenomeCounter {
        GenomeCounter {
            chroms: HashMap::new(),
            last_chrom: Vec::new(),
            failed_to_get_name: 0
        }
    }

    pub fn count_read(
        &mut self,
        cell_id: &[u8],
        umi: &[u8],
        chr: &[u8],
        start: i64,
        end: i64,
        _strand: Strand, //TOOD make use of this. chemistry dependent. input flag?
        cf: &CountFeature,
    ) {
        //If we moved to a new chromosome, ensure we wrap up the counters on the previous one
        if chr != self.last_chrom {
            let prev = self.chroms.get_mut(&self.last_chrom);
            if let Some(prev) = prev {
                prev.finish(&cf);
            }
            self.last_chrom = chr.to_vec();
        }

        //Investigate relevant chromosome
        let gff_chrom = self.chroms.get_mut(chr);
        if let Some(gff_chrom) = gff_chrom {
            //Loop over all genes on this chromosome. Note that the list is sorted backwards
            //such that genes can be popped off the end in O(1) once they are done
            let mut cur_gene = (gff_chrom.genes.len() - 1) as i64;
            while cur_gene >= 0 {
                let this_gene = gff_chrom.genes.get_mut(cur_gene as usize).unwrap();

                //See if the read overlaps current gene
                if this_gene.gene_end < start {
                    //We are past this gene. The counting can be finalized
                    let this_gene = gff_chrom.genes.pop().unwrap();
                    cf.tx.send(Some(this_gene)).unwrap();
                } else if end < this_gene.gene_start {
                    //This gene is beyond the current read. Since reads are sorted by position, we need not check more genes
                    break;
                } else {
                    //This gene overlaps, so add to its read count
                    let counter = this_gene
                        .counters
                        .entry(cell_id.into())
                        .or_insert(CellCounter::new());
                    counter.umis.push(umi.into());
                }

                //Proceed to check the next gene
                cur_gene -= 1;
            }
        } else {
            println!(
                "Read from chromosome not declared in GFF; ignoring: {}",
                String::from_utf8(chr.into()).unwrap()
            );
        }
    }

    pub fn add_feature(&mut self, f: GeneCounter) {
        self.chroms
            .entry(f.gene_chr.clone())
            .and_modify(|e| e.add_feature(f))
            .or_insert(ChromosomeCounter::new());
    }

    pub fn sort(&mut self) {
        for (_, val) in self.chroms.iter_mut() {
            val.sort();
        }
    }



    

    /// 
    /// For GFF/GTF reading, process one record
    /// 
    fn add_gene_record(
        gff: &mut GenomeCounter, 
        params: &GFFparseSettings, 
        record: &RecordBuf
    ) {
        //Only insert records that the user have chosen; typically genes
        if record.ty() == params.use_feature {

            /*
            println!(
                "{}\t{}\t{}",
                record.reference_sequence_name(),
                record.start(),
                record.end(),
            );
            */
            
            //let fieldid_id = "ID"; // fieldGeneId   for yersinia
            let fieldid_id = "gene_id"; // fieldGeneId
            let fieldid_name = "name"; // fieldGeneId

            let attr = record.attributes();
            let attr_id = attr.get(fieldid_id.as_bytes());

            if let Some(attr_id)=attr_id {
                let attr_id = attr_id.as_string().expect("GFF: ID is not a string").to_string();

                //Pick a name. Use ID if nothing else
                let attr_name = attr.get(fieldid_name.as_bytes());
                let attr_name = match attr_name {
                    Some(attr_name) => attr_name.as_string().expect("GFF: Name is not a string").to_string(),
                    None => {
                        gff.failed_to_get_name += 1;
                        attr_id.clone()
                    }
                };

                let gc = GeneCounter {
                    gene_chr: record.reference_sequence_name().to_vec(),
                    gene_start: record.start().get() as i64,
                    gene_end: record.end().get() as i64,
                    gene_strand: record.strand(),
        
                    gene_id: attr_id.as_bytes().to_vec(),
                    gene_name: attr_name.as_bytes().to_vec(),

                    counters: HashMap::new(),
                };

                gff.add_feature(gc);

            } else {
                println!("GFF: Requested feature has no ID");
            }
        }
    }


    /// 
    /// Read a GFF file - from a reader
    /// 
    fn read_gff_from_reader<R>(
        reader: &mut gff::io::Reader<R>, 
        params: &GFFparseSettings
    ) -> anyhow::Result<GenomeCounter> where R:std::io::BufRead  {
        let mut gff = GenomeCounter::new();
        for result in reader.record_bufs() {
            let record = result.expect("Could not read a GFF record; is it actually a GTF?");
            Self::add_gene_record(&mut gff, params, &record);
        }
        anyhow::Ok(gff)
    }


    /// 
    /// Read a GTF file - from a reader
    /// 
    fn read_gtf_from_reader<R>(
        reader: &mut gtf::io::Reader<R>, 
        params: &GFFparseSettings
    ) -> anyhow::Result<GenomeCounter> where R:std::io::BufRead  {
        let mut gff = GenomeCounter::new();
        for result in reader.record_bufs() {
            let record = result.expect("Could not read a GFF record; is it actually a GTF?");
            Self::add_gene_record(&mut gff, params, &record);
        }
        anyhow::Ok(gff)
    }


    /// 
    /// Read a GFF file
    /// 
    fn read_gff_file(path_gff: &PathBuf, params: &GFFparseSettings) -> anyhow::Result<GenomeCounter> {


        let spath = path_gff.to_string_lossy();

        let mut gff = if spath.ends_with("gff.gz") {

            println!("Reading gzipped GFF: {:?}",path_gff);
            let mut reader = File::open(&path_gff)
                .map(GzDecoder::new)
                .map(BufReader::new)
                .map(gff::io::Reader::new)?;
            Self::read_gff_from_reader(&mut reader, params)

        } else if spath.ends_with("gff") {

            println!("Reading flat GFF: {:?}",path_gff);
            let mut reader = File::open(&path_gff)
                .map(BufReader::new)
                .map(gff::io::Reader::new)?;
            Self::read_gff_from_reader(&mut reader, params)

        } else if spath.ends_with("gtf.gz") {

            println!("Reading gzipped GTF: {:?}",path_gff);
            let mut reader = File::open(&path_gff)
                .map(GzDecoder::new)
                .map(BufReader::new)
                .map(gtf::io::Reader::new)?;
            Self::read_gtf_from_reader(&mut reader, params)

        } else if spath.ends_with("gtf") {

            println!("Reading gzipped GTF: {:?}",path_gff);
            let mut reader = File::open(&path_gff)
                .map(BufReader::new)
                .map(gtf::io::Reader::new)?;
            Self::read_gtf_from_reader(&mut reader, params)
            
        } else {
            anyhow::bail!("Could not tell file format for GFF/GTF file {:?}", path_gff);
        }?;

        //Sort records to make it ready for counting
        gff.sort();

        //See if it worked
        let mut num_features = 0;
        for chr in gff.chroms.values() {
            num_features += chr.genes.len();
        }
        println!("Done reading GFF; number of features: {}", num_features);
        println!("Number of features for which name field was missing: {}  (not all files have a name field - feature ID will be reported instead)", gff.failed_to_get_name);
        if num_features == 0 {
            anyhow::bail!("Stopping because there are no features");
        }        

        anyhow::Ok(gff)       
    }







/* 
https://gmod.org/wiki/GFF3

OUR GFF
NC_006153.2	RefSeq	gene	56826	58085	.	+	.	ID=gene-YPTB_RS21810;Name=yscD;gbkey=Gene;gene=yscD;gene_biotype=protein_coding;locus_tag=YPTB_RS21810;old_locus_tag=pYV0080
NC_006153.2	Protein Homology	CDS	56826	58085	.	+	0	ID=cds-WP_002212919.1;Parent=gene-YPTB_RS21810;Dbxref=GenBank:WP_002212919.1;Name=WP_002212919.1;gbkey=CDS;gene=yscD;inference=COORDINATES: similar to AA sequence:RefSeq:WP_002212919.1;locus_tag=YPTB_RS21810;product=SctD family type III secretion system inner membrane ring subunit YscD;protein_id=WP_002212919.1;transl_table=11

BASIC GFF
ctg123 . mRNA            1300  9000  .  +  .  ID=mrna0001;Name=sonichedgehog
ctg123 . exon            1300  1500  .  +  .  Parent=mrna0001
*/




/* 
    use noodles_gtf as gtf;
let reader = gtf::io::Reader::new(io::empty());
let _ = reader.get_ref();
*/

}
