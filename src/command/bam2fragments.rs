use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use bgzip::Compression;
use rust_htslib::bam::Read;
use rust_htslib::bam::record::Record as BamRecord;


pub struct Bam2FragmentsParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub num_threads: usize,
}




/**
 * 
 * as input, take total count matrix, pick features that are within a certain percentile. randomize and subset these further to get a good list!
 * 
 * 
 */

pub struct Bam2Fragments {
}
impl Bam2Fragments {


    pub fn run(
        params: &Bam2FragmentsParams
    ) -> anyhow::Result<()> {

        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads
        let mut bam = rust_htslib::bam::Reader::from_path(&params.path_input)?;

        //Activate multithreaded reading
        bam.set_threads(params.num_threads).unwrap();

        //Save a "Fragments.tsv", bgzip-format. Writer is multithreaded
        let mut outfile = File::create(&params.path_output).expect("Could not open output file");
        let mut writer = bgzip::write::BGZFMultiThreadWriter::new(&mut outfile, Compression::default());
        writer.write_all(b"#CHR\tFROM\tTO\tCELLID\tCNT\tUMI\n")?; // UMI is optional; what works with Signac?

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Only keep mapping reads
            let flags = record.flags();
            if flags & 0x4 ==0 {

                /* 
                println!("{:?} ",record);
                println!("{:?} ",record.pos());
                println!("{:?} ",record.mpos());
                */

                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name = record.qname();
                let mut splitter = read_name.split(|b| *b == b':'); 
                let cell_id = splitter.next().expect("Could not parse cellID from read name");

                let header = bam.header();

                let chr = header.tid2name(record.tid() as u32);

                //Get left-most mapping position
                let startpos =record.pos();
                
                //mpos();  
                //From samtools specification: "1-based leftmost mapping POSition of the first CIGAR operation that “consumes” a reference base". ==> This is any of MDN=I
                //If POS is 0, no assumptions can be made about RNAME and CIGAR"
                
                //Figure the end-position from the CIGAR
                let cigar = record.cigar();
                let endpos = cigar.end_pos();
                
                //TODO: future option is to split read by S* to handle splicing.
                //Note that resorting is then needed. but the local nature suggests that a priority queue can be used along with other tricks

                //Write the BED record
                writer.write_all(chr).unwrap();
                write!(&mut writer, "\t{}\t{}\t",startpos, endpos).unwrap();
                writer.write(cell_id).unwrap();
                write!(&mut writer, "\t1\t\n").unwrap(); //Leaving space for a future UMI here                
            }

        }
        writer.close()?;

        //Tabix-index the output file to prepare it for loading
        println!("Indexing final output file");
        index_fragments(&params.path_output).expect("Failed to tabix index output file");

        Ok(())
    }
}




pub fn index_fragments(p: &PathBuf) -> anyhow::Result<()> {
    let p = p.to_str().expect("could not form path").to_string();
    let mut process = Command::new("tabix");
    let process = process.
        arg("-p").
        arg("bed").
        arg(p);

    let _ = process.output().expect("Failed to run tabix");
    Ok(())
}