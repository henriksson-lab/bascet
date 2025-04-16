use std::fs::File;
use std::path::PathBuf;
use anyhow::bail;


use super::ShardRandomFileExtractor;
use super::TirpBascetShardReader;
use super::ZipBascetShardReader;
//use super::ListFastqReader;



#[derive(Debug,Clone,PartialEq,Eq)]
pub enum DetectedFileformat {
    TIRP,
    ZIP,
    SingleFASTQ,
    PairedFASTQ,
    BAM,
    ListFASTQ,
    Other
}




pub fn detect_shard_format(p: &PathBuf) -> DetectedFileformat {
    let p_string = p.file_name().expect("cannot convert OS string when detecting file format").to_string_lossy();

    if p_string.ends_with(".tirp.gz") {
        DetectedFileformat::TIRP
    } else if p_string.ends_with(".zip") { 
        DetectedFileformat::ZIP
    } else if p_string.ends_with(".listfastq"){ 
        DetectedFileformat::ListFASTQ
    } else if p_string.ends_with(".bam") | p_string.ends_with(".cram"){ 
        DetectedFileformat::BAM
    } else if p_string.ends_with(".R1.fq.gz") | p_string.ends_with(".R1.fastq.gz")  | p_string.ends_with(".R1.fq")  | p_string.ends_with(".R1.fastq") { 
        DetectedFileformat::PairedFASTQ
    } else if p_string.ends_with(".fq.gz") | p_string.ends_with(".fastq.gz")  | p_string.ends_with(".fq")  | p_string.ends_with(".fastq") { 
        DetectedFileformat::SingleFASTQ
    } else {
        println!("Warning: Unknown file format for {}", p.display());
        DetectedFileformat::Other
    }
}


pub fn get_suitable_file_extractor(
    p: &PathBuf, 
    format: &DetectedFileformat
) -> Box::<dyn ShardRandomFileExtractor> {
    match format {
        DetectedFileformat::TIRP => 
            Box::new(TirpBascetShardReader::new(&p).expect("Failed to create TIRP reader")),
        DetectedFileformat::ZIP => 
            Box::new(ZipBascetShardReader::new(&p).expect("Failed to create ZIP reader")),
        DetectedFileformat::SingleFASTQ => 
            panic!("FASTQ cannot be used for file extraction currently"),
        DetectedFileformat::PairedFASTQ => 
            panic!("FASTQ cannot be used for file extraction currently"),
        DetectedFileformat::BAM => 
            panic!("BAM-like formats cannot be used for file extraction currently"),
        DetectedFileformat::ListFASTQ => 
            panic!("ListFASTQ cannot be used for file extraction currently"),
        DetectedFileformat::Other => 
            panic!("Cannot figure out how to open input file for file extraction")
    }
}






/////// Check that the specified file is a FASTQ file
pub fn verify_input_fq_file(path_in: &PathBuf) -> anyhow::Result<()> {
    let file_format = detect_shard_format(path_in);
    if file_format==DetectedFileformat::SingleFASTQ || file_format==DetectedFileformat::PairedFASTQ {
        if let Ok(file) = File::open(&path_in) {
            if file.metadata()?.len() == 0 {
                //anyhow::bail!("Empty input file");
                print!("Warning: input file is empty");
            }
            Ok(())
        } else {
            bail!("Cannot open input file");
        }
    } else {
        bail!("Input file must be a fastq file")
    }
}



pub fn get_fq_filename_r2_from_r1(
    path: &PathBuf
) -> anyhow::Result<PathBuf> {
    let p_string = path.file_name().expect("cannot convert OS string when detecting file format").to_string_lossy();
    if p_string.ends_with(".R1.fq.gz") { 
        anyhow::Ok(PathBuf::from(replace_file_ending(path, ".R1.fq.gz", ".R2.fq.gz")))
    } else if p_string.ends_with(".R1.fastq.gz") { 
        anyhow::Ok(PathBuf::from(replace_file_ending(path, ".R1.fastq.gz", ".R2.fastq.gz")))
    } else if p_string.ends_with(".R1.fq") { 
        anyhow::Ok(PathBuf::from(replace_file_ending(path, ".R1.fq", ".R2.fq")))
    } else if p_string.ends_with(".R1.fastq") { 
        anyhow::Ok(PathBuf::from(replace_file_ending(path, ".R1.fastq", ".R2.fastq")))
    } else {
        bail!("Cannot figure out R2 for file {}", path.display())
    }
}

pub fn replace_file_ending(path: &PathBuf, old_ending: &str, new_ending: &str) -> String {
    let full_fname = path.to_str().expect("Could not get string from path");
    let mut sub = full_fname[0..(full_fname.len()-old_ending.len())].to_owned();
    sub.push_str(new_ending);
    sub
}