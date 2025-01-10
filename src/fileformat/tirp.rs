use log::debug;
use std::collections::HashSet;
use std::io::BufWriter;
use std::io::Write;
use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use std::process::Command;
use anyhow::bail;

use super::shard::ConstructFromPath;
use super::shard::ShardFileExtractor;
use super::shard::ReadPair;
use super::shard::CellID;

use crate::fileformat::ReadPairReader;
//use crate::fileformat::ReadPairWriter;
use crate::fileformat::ShardCellDictionary;

use rust_htslib::tbx::Reader as TabixReader;
use rust_htslib::tbx::Read;

use noodles::fastq::Writer as FastqWriter;
use noodles::fastq::record::Definition;
use noodles::fastq::Record as FastqRecord;


#[derive(Debug,Clone)]
pub struct TirpBascetShardReaderFactory {
}
impl TirpBascetShardReaderFactory {
    pub fn new() -> TirpBascetShardReaderFactory {
        TirpBascetShardReaderFactory {}
    } 
}
impl ConstructFromPath<TirpBascetShardReader> for TirpBascetShardReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<TirpBascetShardReader> {  ///////// maybe anyhow prevents spec of reader?
        TirpBascetShardReader::new(fname)
    }
}


//#[derive(Debug)]  //// not sure about all of these
pub struct TirpBascetShardReader {
    tabix_reader: TabixReader     // https://docs.rs/rust-htslib/latest/rust_htslib/tbx/index.html
}
impl TirpBascetShardReader {


    pub fn new(fname: &PathBuf) -> anyhow::Result<TirpBascetShardReader> {  ///////// maybe anyhow prevents spec of reader?

        //TODO : check that the index .tbi-file is present; give better error message
        let hist_path = get_histogram_path_for_tirp(&fname);
        if !hist_path.exists() {
            bail!("Cannot find tabix index for {}; is this really a TIRP file?", fname.display());
        }

        // Create a tabix reader for reading a tabix-indexed BED file.
        let tbx_reader = TabixReader::from_path(&fname)
            .expect(&format!("Could not open {:?}", fname));

        let dat = TirpBascetShardReader {
            tabix_reader: tbx_reader
        };
        Ok(dat)
    }

}



impl ReadPairReader for TirpBascetShardReader {


    fn get_reads_for_cell(
        &mut self, 
        cell_id: &String, 
    ) -> anyhow::Result<Arc<Vec<ReadPair>>>{

        //Get tabix id for the cell
        let tid = self.tabix_reader.tid(&cell_id).expect("Could not tabix ID for cell");

        // Seek to the reads (all of them)
        self.tabix_reader
            .fetch(tid, 0, 100) //hopefully ok!
            .expect("could not find reads");

        //Get all reads
        let mut reads:Vec<ReadPair> = Vec::new();
        for line in self.tabix_reader.records() {
            let line = line.expect("Failed to get one TIRP line");
            let rp = parse_tirp_readpair(&line);
            reads.push(rp);
        }
        Ok(Arc::new(reads))
    }


}



impl ShardCellDictionary for TirpBascetShardReader {
    
    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        Ok(self.tabix_reader.seqnames())
    }


    fn has_cell(&mut self, cellid: &CellID) -> bool {
        self.tabix_reader.seqnames().contains(&cellid)
    }

}




impl ShardFileExtractor for TirpBascetShardReader {


    fn get_files_for_cell(&mut self, _cell_id: &CellID) -> anyhow::Result<Vec<String>>{
        println!("request files for cell in TIRP, but this is not implemented");
        Ok(Vec::new())
    }


    fn extract_to_outdir (
        &mut self, 
        cell_id: &String, 
        needed_files: &Vec<String>,
        _fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>{

        let mut valid_files_to_request: HashSet<&str> = HashSet::new();
        valid_files_to_request.extend(["r1.fq","r2.fq"].iter());

        //Figure out which files to get
        //let mut list_toget: Vec<&String> = Vec::new();
        for f in needed_files {
            if f=="*" {
                //panic!("asking * from a gascet is not supported");
            } else if !valid_files_to_request.contains(f.as_str()) {
                panic!("Unable to request file {}", f);
            }
        }

        //Get tabix id for the cell
        let tid = self.tabix_reader.tid(&cell_id).expect("Could not tabix ID for cell");

        ///// Prepare r1 fastq
        let path_outfile = out_directory.join(PathBuf::from("r1.fq"));
        let file_out = File::create(&path_outfile).unwrap();
        let bufwriter_out = BufWriter::new(file_out);
        let mut fqwriter_r1 = FastqWriter::new(bufwriter_out);


        ///// Prepare r2 fastq
        let path_outfile = out_directory.join(PathBuf::from("r2.fq"));
        let file_out = File::create(&path_outfile).unwrap();
        let bufwriter_out = BufWriter::new(file_out);
        let mut fqwriter_r2 = FastqWriter::new(bufwriter_out);
        
        // Seek to the reads (all of them)
        self.tabix_reader
            .fetch(tid, 0, 100) //hopefully ok!
            .expect("could not find reads");

        //For now, keep it simple and just provide r1.fq and r2.fq.
        //Read through all records in region.
        let mut num_read = 0;
        for line in self.tabix_reader.records() {

            let line = line.expect("Failed to get one TIRP line");
            let rp = parse_tirp_readpair(&line);

            let rec_r1 = FastqRecord::new(Definition::new(format!("r{}", num_read), ""), rp.r1, rp.q1);
            let rec_r2 = FastqRecord::new(Definition::new(format!("r{}", num_read), ""), rp.r2, rp.q2);

            let _ = fqwriter_r1.write_record(&rec_r1);
            let _ = fqwriter_r2.write_record(&rec_r2);

            num_read = num_read + 1;                
        }
        debug!("wrote {} reads to fastq", num_read);

        //Flushing is essential for buffered writer ---  will this flush all the way down? possible bug here!! if so, just use bufferedwriter directly
        //fqwriter_r1.flush();
        //fqwriter_r2.flush();

        Ok(true)
    }

}






pub fn write_records_pair_to_tirp(
//    writer: &mut BufWriter<impl Write>, 
    writer: &mut impl Write, 
    cell_id: &CellID,    
    read: &ReadPair,
)  { //where W:Write
    //Structure of each line:
    //cell_id  1   1   r1  r2  q1  q2 umi

    let tab="\t".as_bytes();
    let one="1".as_bytes();
    let newline="\n".as_bytes();


    _ = writer.write_all(cell_id.as_bytes());
    _ = writer.write_all(tab);

    _ = writer.write_all(one);
    _ = writer.write_all(tab);

    _ = writer.write_all(one);
    _ = writer.write_all(tab);

    _ = writer.write_all(&read.r1);
    _ = writer.write_all(tab);
    _ = writer.write_all(&read.r2);
    _ = writer.write_all(tab);
    _ = writer.write_all(&read.q1);
    _ = writer.write_all(tab);
    _ = writer.write_all(&read.q2);
    _ = writer.write_all(tab);
    _ = writer.write_all(&read.umi);
    _ = writer.write_all(newline);

}



pub fn parse_tirp_readpair(
    line: &Vec<u8>,   
) -> ReadPair {

    //Structure of each line:
    //cell_id  1   1   r1  r2  q1  q2 umi

    let tab = b'\t';
    let parts = split_delimited(line, &tab);

    ReadPair {
        r1: parts[3].to_vec(),
        r2: parts[4].to_vec(),
        q1: parts[5].to_vec(),
        q2: parts[6].to_vec(),
        umi: parts[7].to_vec()
    }
}




fn split_delimited<'a, T>(input: &'a [T], delim: &T) -> Vec<&'a [T]>
    where T: PartialEq<T> {
        let indices: Vec<usize> = input.iter().enumerate().filter(|(_, value)| *value == delim).map(|(i, _)| i).collect();
        let mut output = Vec::new();
        output.push(&input[0..(*indices.first().unwrap())]);
        for pair in indices.windows(2) {
            output.push(&input[(pair[0]+1)..pair[1]]);
        }
        output.push(&input[(*indices.last().unwrap()+1)..]);

        output
}



pub fn index_tirp(p: &PathBuf) -> anyhow::Result<()> {
    let p = p.to_str().expect("could not form path").to_string();
    let mut process = Command::new("tabix");
    let process = process.
        arg("-p").
        arg("bed").
        arg(p);

    let _ = process.output().expect("Failed to run tabix");
    Ok(())
}





pub fn get_histogram_path_for_tirp(p: &PathBuf) -> PathBuf {
    let p = p.as_os_str().as_encoded_bytes();
    let mut histpath = p.to_vec();
    let mut ext = ".hist".as_bytes().to_vec();
    histpath.append(&mut ext);
    let histpath = String::from_utf8(histpath).expect("unable to form histogram path");
    PathBuf::from(histpath)    
}

