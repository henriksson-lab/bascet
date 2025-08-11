use flate2::read::GzDecoder;
use noodles::gff::feature::RecordBuf;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use noodles::gtf as gtf;

use noodles::gff as gff;
use noodles::gff::feature::record::Strand;





type ChromosomeID = Vec<u8>;



pub struct GFFparseSettings {
    pub use_feature: String,
    pub attr_id: String,
    pub attr_name: String,
}





#[derive(Clone, Debug)]  //, PartialEq, Eq, PartialOrd, Ord
pub struct GeneMeta {
    pub gene_chr: ChromosomeID,
    pub gene_start: i64,
    pub gene_end: i64,
    pub gene_strand: Strand,

    pub gene_id: Vec<u8>,
    pub gene_name: Vec<u8>,
}




/// 
/// List of features to count
/// 
pub struct FeatureCollection {
    pub list_feature: Vec<GeneMeta>,
    failed_to_get_name: usize
}
impl FeatureCollection {
    pub fn new() -> FeatureCollection {
        FeatureCollection {
            list_feature: Vec::new(),
            failed_to_get_name: 0
        }
    }


    pub fn add_feature(&mut self, f: GeneMeta) {
        self.list_feature.push(f);
    }

    

    /// 
    /// For GFF/GTF reading, process one record
    /// 
    fn add_gene_record(
        gff: &mut FeatureCollection, 
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

                let gene_meta = GeneMeta {
                    gene_chr: record.reference_sequence_name().to_vec(),
                    gene_start: record.start().get() as i64,
                    gene_end: record.end().get() as i64,
                    gene_strand: record.strand(),
        
                    gene_id: attr_id.as_bytes().to_vec(),
                    gene_name: attr_name.as_bytes().to_vec(),
                };


                if record.reference_sequence_name().to_string() == "1" {  ////////// for testing
                    gff.add_feature(gene_meta);
                }


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
    ) -> anyhow::Result<FeatureCollection> where R:std::io::BufRead  {
        let mut gff = FeatureCollection::new();
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
    ) -> anyhow::Result<FeatureCollection> where R:std::io::BufRead  {
        let mut gff = FeatureCollection::new();
        for result in reader.record_bufs() {
            let record = result.expect("Could not read a GFF record; is it actually a GTF?");
            Self::add_gene_record(&mut gff, params, &record);
        }
        anyhow::Ok(gff)
    }


    /// 
    /// Read a GFF or GTF file
    /// 
    pub fn read_file(path_gff: &PathBuf, params: &GFFparseSettings) -> anyhow::Result<FeatureCollection> {


        let spath = path_gff.to_string_lossy();

        let gff = if spath.ends_with("gff.gz") {

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

        //See if it worked
        let num_features = gff.list_feature.len();
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

