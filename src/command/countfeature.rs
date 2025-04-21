//use std::any::Any;
//use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
//use std::collections::LinkedList;

/* 
use hdf5::types::VarLenUnicode;
use rust_htslib::bam::Read;
use rust_htslib::bam::record::Record as BamRecord;
use rust_htslib::htslib::uint;
use hdf5::File as H5File;*/
use anyhow::Result;
use clap::Args;

use noodles::gff::record::Strand;
use noodles::gff as gff;


 
pub enum BascetStrand {  //equivalent to GFF 
    None,
    Forward,
    Reverse,
    Unknown,
}
    

use super::determine_thread_counts_1;

pub const DEFAULT_PATH_TEMP: &str = "temp";


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
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)] //Not used, but kept here for consistency with other commands
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






use std::collections::HashMap;




pub struct CountFeature { 
    pub path_in: PathBuf,
    pub path_gff: PathBuf,
    pub path_out: PathBuf,
    pub use_feature: String,
    pub num_threads: usize
}
impl CountFeature {



    pub fn run(
        params: &CountFeature
    ) -> anyhow::Result<()> {

        //TODO: check if BAM is sorted
    
//        "/husky/fromsequencer/241210_joram_rnaseq/ref/all.gff3"

        let _gff = GFF::read_file(&params)?;






        Ok(())
    }
}










//// Counter of reads for one gene and cell
pub struct CounterForCell {
    pub umis: Vec<String>,
}



//// Information about one gene
pub struct GeneCounter {
    pub gene_chr: String,
    pub gene_start: usize,
    pub gene_end: usize,
    pub gene_strand: Strand,

    pub gene_id: String,
    pub gene_name: String,

    pub counters: HashMap<String, CounterForCell>,
}
impl GeneCounter {


}



//// Container of genes for one chromosome
pub struct GFFchrom {
    pub genes: Vec<GeneCounter>,
    pub current_pos: usize

    //Or keep pointer to start, use a vec

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
        self.genes.sort_by_key(|e| e.gene_start);
    }
}




//// Container of genes for all chromosomes
pub struct GFF {
    chroms: HashMap<String, GFFchrom>
    //    pub genes: LinkedList<GeneCounter>
    //Or keep pointer to start, use a vec    
}
impl GFF {


    pub fn new() -> GFF {
        GFF { 
            chroms: HashMap::new()
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
                let attr_id = attr.get("ID");

                if let Some(attr_id)=attr_id {
                    let attr_id = attr_id.as_string().expect("ID is not a string").to_string();

                    //Pick a name. Use ID if nothing else
                    let attr_name = attr.get("Name");
                    let attr_name = match attr_name {
                        Some(attr_name) => attr_name.as_string().expect("Name is not a string").to_string(),
                        None => attr_id.clone()
                    };

                    let gc = GeneCounter {
                        gene_chr: record.reference_sequence_name().to_string(),
                        gene_start: record.start().into(),
                        gene_end: record.end().into(),
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


