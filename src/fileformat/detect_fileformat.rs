use std::fs::File;
use std::path::PathBuf;
use anyhow::bail;


use super::ShardFileExtractor;
use super::TirpBascetShardReader;
use super::ZipBascetShardReader;



#[derive(Debug,Clone,PartialEq,Eq)]
pub enum DetectedFileformat {
    TIRP,
    ZIP,
    FASTQ,
    BAM,
    Other
}




pub fn detect_shard_format(p: &PathBuf) -> DetectedFileformat {
    let p_string = p.file_name().expect("cannot convert OS string when detecting file format").to_string_lossy();

    if p_string.ends_with(".tirp.gz") {
        DetectedFileformat::TIRP
    } else if p_string.ends_with(".zip") { 
        DetectedFileformat::ZIP
    } else if p_string.ends_with(".bam") | p_string.ends_with(".cram"){ 
        DetectedFileformat::BAM
    } else if p_string.ends_with(".fq.gz") | p_string.ends_with(".fastq.gz")  | p_string.ends_with(".fq")  | p_string.ends_with(".fastq") { 
        DetectedFileformat::FASTQ
    } else {
        DetectedFileformat::Other
    }
}


pub fn get_suitable_shard_reader(
    p: &PathBuf, 
    format: &DetectedFileformat
) -> Box::<dyn ShardFileExtractor> {
    match format {
        DetectedFileformat::TIRP => Box::new(TirpBascetShardReader::new(&p).expect("Failed to create TIRP reader")),
        DetectedFileformat::ZIP => Box::new(ZipBascetShardReader::new(&p).expect("Failed to create ZIP reader")),
        _ => panic!("Cannot figure out how to open input file (could not detect shard type)")
    }
}






/////// Check that the specified file is a fastq file
pub fn verify_input_fq_file(path_in: &PathBuf) -> anyhow::Result<()> {
    if let Ok(file) = File::open(&path_in) {
        if file.metadata()?.len() == 0 {
            //anyhow::bail!("Empty input file");
            print!("Warning: input file is empty");
        }
    }

    let filename = path_in.file_name().unwrap().to_str().unwrap();

    if filename.ends_with("fq") | filename.ends_with("fq.gz") | 
        filename.ends_with("fastq") | filename.ends_with("fastq.gz")  {
        //ok
    } else {
        bail!("Input file must be a fastq file")
    }

    Ok(())
}