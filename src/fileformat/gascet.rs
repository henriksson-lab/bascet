use std::collections::HashSet;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::fs::File;
//use anyhow::bail;
use log::debug;

use super::bascet::ShardReader;

use rust_htslib::tbx::Reader as TabixReader;
use rust_htslib::tbx::Read;
//use noodles_bgzf::Writer as FastqWriter;   // why both bgzf and fastq??   ... this is for fastq.gz!!!

use noodles::fastq::Writer as FastqWriter;
use noodles::fastq::record::Definition;
use noodles::fastq::Record as FastqRecord;

pub type CellID = String;




#[derive(Debug,Clone)]
pub struct ReadPair {
    pub r1: Vec<u8>,
    pub r2: Vec<u8>,
    pub q1: Vec<u8>,
    pub q2: Vec<u8>,
    pub umi: Vec<u8>
}



//#[derive(Debug)]  //// not sure about all of these
pub struct GascetShardReader {
    tabix_reader: TabixReader     // https://docs.rs/rust-htslib/latest/rust_htslib/tbx/index.html
}





impl ShardReader for GascetShardReader {




    fn new(fname: &PathBuf) -> anyhow::Result<GascetShardReader> {

        //TODO : check that the index .tbi-file is present; give better error message

        // Create a tabix reader for reading a tabix-indexed BED file.
        //let path_bed = "file.bed.gz";
        let tbx_reader = TabixReader::from_path(&fname)
            .expect(&format!("Could not open {:?}", fname));

        Ok(GascetShardReader {
            tabix_reader: tbx_reader
        })
    }


    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        Ok(self.tabix_reader.seqnames())
    }

    fn get_files_for_cell(&mut self, _cell_id: &CellID) -> anyhow::Result<Vec<String>>{
        println!("get files for cell in gascet not implemented");
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

            let line = line.expect("Failed to get one line from the gascet");

            let rp = parse_gascet_readpair(&line);

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






pub fn write_records_pair_to_gascet<W>(
    writer: &mut BufWriter<impl Write>, 
    cell_id: &CellID,    
    read: &ReadPair,
) where W:Write {
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



pub fn parse_gascet_readpair(
    line: &Vec<u8>,   
) -> ReadPair {

    //Structure of each line:
    //cell_id  1   1   r1  r2  q1  q2 umi

    let tab = b'\t';
    let parts = split_delimited(line, &tab);

/* 
    println!("");
    println!("");
    println!("");
    println!("'{:?}'", parts[0].to_vec());
    println!("'{:?}'", parts[1].to_vec());
    println!("'{:?}'", parts[2].to_vec());
    println!("'{:?}'", parts[3].to_vec());
//    '[9, 84, 84, 71, 65, 65, 84, 65, 84, 71, 65, 84, 71, 84, 65, 71, 65, 84, 65, 65, 84, 65, 65, 65, 65, 65, 84, 65, 67, 65, 71, 84, 71, 84, 65, 84, 65, 84, 67, 71, 65, 84, 71, 67, 71, 84, 84, 71, 65, 65, 67, 67, 71, 84, 67, 71, 84, 65, 84, 84, 71, 
*/
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
        /* 
        if indices.get(0) != Some(&0) {
            indices.insert(0, 0);
        }*/
        let mut output = Vec::new();
        output.push(&input[0..(*indices.first().unwrap())]);
        for pair in indices.windows(2) {
            output.push(&input[(pair[0]+1)..pair[1]]);
        }
        output.push(&input[(*indices.last().unwrap()+1)..]);

        output
}