use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::collections::HashMap;

use bio::bio_types::strand::ReqStrand;
use rust_htslib::bam::Read;
use rust_htslib::bam::record::Record as BamRecord;


use anyhow::Result;
use clap::Args;

use noodles_gff::feature::record::Strand;
use noodles_gff as gff;


use super::determine_thread_counts_1;

   

//pub const DEFAULT_PATH_TEMP: &str = "temp";


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
    #[arg(short = 'f', default_value = "gene")] //Not used, but kept here for consistency with other commands
    pub use_feature: String,
    
    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = "temp")] //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl CountFeatureCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        let num_threads_total = determine_thread_counts_1(self.num_threads_total)?;
        println!("Using threads {}",num_threads_total);

        //TODO Can check that input file is sorted via header

        CountFeature::run(&CountFeature {
            path_in: self.path_in.clone(),
         //   path_tmp: self.path_tmp.clone(),
            path_gff: self.path_gff.clone(),
            path_out: self.path_out.clone(),
            use_feature: self.use_feature.clone(),
            num_threads: num_threads_total
        }).unwrap();

        log::info!("CountFeature has finished succesfully");
        Ok(())
    }
}



type Cellid = Vec<u8>;

type ChromosomeID = Vec<u8>;


pub enum BascetStrand {  //equivalent to GFF 
    None,
    Forward,
    Reverse,
    Unknown,
}
 




pub struct CountFeature { 
    pub path_in: PathBuf,
    pub path_gff: PathBuf,
    pub path_out: PathBuf,
    pub use_feature: String,
    pub num_threads: usize
}
impl CountFeature {


    pub fn process_bam(
        params: &CountFeature,
        gff: &mut GFF
    ) -> anyhow::Result<()> {

        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads.
        //cannot be TIRF; if we divide up reads we risk double counting
        let mut bam = rust_htslib::bam::Reader::from_path(&params.path_in)?;

        //Activate multithreaded reading
        bam.set_threads(params.num_threads).unwrap();

        //Keep track of last chromosome seen (assuming that file is sorted)
        //let mut last_chr:Vec<u8> = Vec::new();        

        //Map cellid -> count. Note that we do not have a list of cellid's at start; we need to harmonize this later
        //let mut map_cell_count: BTreeMap<Cellid, uint> = BTreeMap::new();

        let mut num_reads=0;

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Only keep mapping reads
            let flags = record.flags();
            if flags & 0x4 ==0 {

                let header = bam.header();
                let chr = header.tid2name(record.tid() as u32);

                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name = record.qname();
                let mut splitter = read_name.split(|b| *b == b':'); 
                let cell_id = splitter.next().expect("Could not parse cellID from read name");
                let umi = splitter.next().expect("Could not parse UMI from read name");

               
                let strand = match record.strand() {
                    ReqStrand::Forward => Strand::Forward,
                    ReqStrand::Reverse => Strand::Reverse
                };

                gff.count_read(
                    cell_id,
                    umi,
                    chr,
                    record.pos(), //or mpos? TODO
                    record.cigar().end_pos(),
                    strand
                );


                //Keep track of where we are
                num_reads+=1;
                if num_reads%1000000 == 0 {
                    println!("Processed {} reads", num_reads);
                }
            }
        }

        Ok(())
    }



    pub fn run(
        params: &CountFeature
    ) -> anyhow::Result<()> {

        //TODO: check if BAM is sorted
    
//        "/husky/fromsequencer/241210_joram_rnaseq/ref/all.gff3"

        let mut gff = GFF::read_file(&params)?;


        CountFeature::process_bam(&params, &mut gff)?;


        Ok(())
    }
}










//// Counter of reads for one gene and cell
pub struct CounterForCell {
    pub umis: Vec<Vec<u8>>,
}
impl CounterForCell {
    fn new() -> CounterForCell {
        CounterForCell {
            umis: Vec::new()
        }
    }
}



//// Information about one gene, and collectors of reads from each cell
pub struct GeneCounter {
    pub gene_chr: ChromosomeID,
    pub gene_start: i64,
    pub gene_end: i64,
    pub gene_strand: Strand,

    pub gene_id: String,
    pub gene_name: String,

    pub counters: HashMap<Cellid, CounterForCell>,
}
impl GeneCounter {
    fn finalize(&mut self) {
        //Don't send this gene for calculation if trivially empty
        if self.counters.len() > 0 {

            //For each cell:
            for (cellid, counter) in self.counters.iter() {






            }


            //Perform UMI deduplication

            //Count

            //Send count to a writer



            // https://github.com/sstadick/rumi  -- can use. wants htslib Record; keep all the way to the end?

        }
    }
}



//// Container for genes, located on one chromosome
pub struct GFFchrom {
    pub genes: Vec<GeneCounter>,
    pub current_pos: usize
}
impl GFFchrom {

    pub fn new() -> GFFchrom {
        GFFchrom {
            genes: Vec::new(),
            current_pos: 0
        }
    }

    pub fn add_feature(
        &mut self,
        f: GeneCounter
    ) {
        self.genes.push(f);
    }


    pub fn sort(&mut self) {
        self.genes.sort_by_key(|e| e.gene_start); /////////////////////////////// todo need to also sort by end position!!! currently there is a bug TODO
    }

    pub fn finish(&mut self) {
        //For any remaining genes, wrap up
        let mut cur_gene: usize = self.current_pos;
        while cur_gene < self.genes.len() {
            let this_gene = self.genes.get_mut(cur_gene).unwrap();
            this_gene.finalize();
            cur_gene += 1;
        }
    }
}




//// Container of genes for all chromosomes
pub struct GFF {
    chroms: HashMap<Vec<u8>, GFFchrom>,
    last_chrom: Vec<u8>
}
impl GFF {


    pub fn new() -> GFF {
        GFF { 
            chroms: HashMap::new(),
            last_chrom: Vec::new()
        }
    }


    pub fn count_read(
        &mut self,
        cell_id: &[u8],
        umi: &[u8],
        chr: &[u8],
        start: i64,
        end: i64,
        _strand: Strand  //TOOD make use of this. chemistry dependent
    ) {

        //If we moved to a new chromosome, ensure we wrap up the counters on the previous one
        if chr!=self.last_chrom {    
            let prev = self.chroms.get_mut(&self.last_chrom);
            if let Some(prev) = prev {
                prev.finish();
            }
            self.last_chrom = chr.to_vec();
        }

        //Investigate relevant chromosome
        let gff_chrom = self.chroms.get_mut(chr);
        if let Some(gff_chrom) = gff_chrom {
            //Loop over all genes on this chromosome
            let mut cur_gene = gff_chrom.current_pos;
            while cur_gene < gff_chrom.genes.len() {
                let this_gene = gff_chrom.genes.get_mut(cur_gene).unwrap();

                //See if the read overlaps current gene
                if this_gene.gene_end < start {
                    //We are past this gene. The counting can be finalized
                    this_gene.finalize();
                    gff_chrom.current_pos += 1;
                } else if end < this_gene.gene_start {
                    //This gene is beyond the current read. Since reads are sorted,
                    //we need not check more genes
                    break;
                }

                //This gene overlaps, so add the read count
                let counter = this_gene.counters.entry(cell_id.into()).or_insert(CounterForCell::new());
                counter.umis.push(umi.into());

                //Proceed to check the next gene
                cur_gene += 1;
            }





        } else {
            println!("Read from chromosome not declared in GFF; ignoring: {}", String::from_utf8(chr.into()).unwrap());
        }
    }




    pub fn add_feature(
        &mut self,
        f: GeneCounter
    ) {
        self.chroms.entry(f.gene_chr.clone()).
            and_modify(|e| e.add_feature(f)).
            or_insert(GFFchrom::new());
    }

    pub fn sort(&mut self) {
        for (_, val) in self.chroms.iter_mut() {
            val.sort();
        }
    }


    pub fn read_file(params: &CountFeature) -> anyhow::Result<GFF> {

        let mut gff = GFF::new();

        /* 
        https://gmod.org/wiki/GFF3

        OUR GFF
        NC_006153.2	RefSeq	gene	56826	58085	.	+	.	ID=gene-YPTB_RS21810;Name=yscD;gbkey=Gene;gene=yscD;gene_biotype=protein_coding;locus_tag=YPTB_RS21810;old_locus_tag=pYV0080
        NC_006153.2	Protein Homology	CDS	56826	58085	.	+	0	ID=cds-WP_002212919.1;Parent=gene-YPTB_RS21810;Dbxref=GenBank:WP_002212919.1;Name=WP_002212919.1;gbkey=CDS;gene=yscD;inference=COORDINATES: similar to AA sequence:RefSeq:WP_002212919.1;locus_tag=YPTB_RS21810;product=SctD family type III secretion system inner membrane ring subunit YscD;protein_id=WP_002212919.1;transl_table=11

        BASIC GFF
        ctg123 . mRNA            1300  9000  .  +  .  ID=mrna0001;Name=sonichedgehog
        ctg123 . exon            1300  1500  .  +  .  Parent=mrna0001
        */

        //Read all records
        let mut reader = File::open(&params.path_gff)
            .map(BufReader::new)
            .map(gff::io::Reader::new)?;

        for result in reader.record_bufs() {
            let record = result?;

            //Only insert records that the user have chosen; typically genes
            if record.ty() == params.use_feature {

                println!(
                    "{}\t{}\t{}",
                    record.reference_sequence_name(),
                    record.start(),
                    record.end(),
                );

                let attr = record.attributes();
                let attr_id = attr.get(b"ID");

                if let Some(attr_id)=attr_id {
                    let attr_id = attr_id.as_string().expect("ID is not a string").to_string();

                    //Pick a name. Use ID if nothing else
                    let attr_name = attr.get(b"Name");
                    let attr_name = match attr_name {
                        Some(attr_name) => attr_name.as_string().expect("Name is not a string").to_string(),
                        None => attr_id.clone()
                    };

                    let gc = GeneCounter {
                        gene_chr: record.reference_sequence_name().to_vec(),
                        gene_start: record.start().get() as i64,
                        gene_end: record.end().get() as i64,
                        gene_strand: record.strand(),
            
                        gene_id: attr_id,
                        gene_name: attr_name,

                        counters: HashMap::new()
                    };

                    gff.add_feature(gc);

                } else {
                    println!("Requested feature has no ID");
                }
            }
        }

    //Sort records
    gff.sort();


    anyhow::Ok(gff)
    }

}


