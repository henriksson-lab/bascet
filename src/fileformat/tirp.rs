use log::debug;
use std::collections::HashSet;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use std::process::Command;
use anyhow::bail;


use super::ConstructFromPath;
use super::ShardRandomFileExtractor;
use super::ShardStreamingFileExtractor;
use super::ReadPair;
use super::CellID;
use super::ReadPairReader;
use super::ShardCellDictionary;
use super::shard::StreamingReadPairReader;

use rust_htslib::tbx::Reader as TabixReader;
use rust_htslib::tbx::Read;

use noodles::fastq::Writer as FastqWriter;
use noodles::fastq::record::Definition;
use noodles::fastq::Record as FastqRecord;


type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;


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

        //Check that the index .tbi-file is present; give better error message
        let index_path = get_tbi_path_for_tirp(&fname);
        if !index_path.exists() {
            bail!("Cannot find tabix .tbi-file for {}; is this really a TIRP file?", fname.display());
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




impl ShardRandomFileExtractor for TirpBascetShardReader {


    fn get_files_for_cell(&mut self, _cell_id: &CellID) -> anyhow::Result<Vec<String>>{
        println!("request files for cell in TIRP, but this is not implemented");
        Ok(Vec::new())
    }


    fn extract_as(
        &mut self, 
        _cell_id: &String, 
        _file_name: &String,
        _path_outfile: &PathBuf
    ) -> anyhow::Result<()>{

        panic!("extract_as not yet implemented");

        //Ok(())
    }




    fn extract_to_outdir (
        &mut self, 
        cell_id: &String, 
        needed_files: &Vec<String>,
        _fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>{

        let mut valid_files_to_request: HashSet<&str> = HashSet::new();
        valid_files_to_request.extend(["r1.fq","r2.fq"].iter());  /////////////////////// TODO support fasta as well


        let mut get_fastq = false ;

        //Figure out which files to get
        //let mut list_toget: Vec<&String> = Vec::new();
        for f in needed_files {
            if f=="*" {
                panic!("asking for file * from a TIRP is not supported");
            } else if !valid_files_to_request.contains(f.as_str()) {
                panic!("Unable to request file {}", f);
            } else {
                if f=="r1.fq" || f=="r2.fq" {
                    get_fastq=true;
                }
            }
        }

        //Get tabix id for the cell
        let tid = self.tabix_reader.tid(&cell_id).expect("Could not tabix ID for cell");

        if get_fastq {
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
        }





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



pub fn parse_tirp_readpair_with_cellid(
    line: &[u8], //Vec<u8>,   
) -> (Vec<u8>, ReadPair) {

    //Structure of each line:
    //cell_id  1   1   r1  r2  q1  q2 umi

    let tab = b'\t';
    let parts = split_delimited(line, &tab);
    let cellid = parts[0].to_vec();

    (
        cellid,
        ReadPair {
            r1: parts[3].to_vec(),
            r2: parts[4].to_vec(),
            q1: parts[5].to_vec(),
            q2: parts[6].to_vec(),
            umi: parts[7].to_vec()
        }
    )
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




pub fn get_tbi_path_for_tirp(p: &PathBuf) -> PathBuf {
    let p = p.as_os_str().as_encoded_bytes();
    let mut index_path = p.to_vec();
    let mut ext = ".tbi".as_bytes().to_vec();
    index_path.append(&mut ext);
    let histpath = String::from_utf8(index_path).expect("unable to form tbi path");
    PathBuf::from(histpath)    
}


















///////////////////
/////////////////// Below is code for streaming of data from TIRPs -- readpair system
///////////////////




#[derive(Debug)]
pub struct TirpStreamingReadPairReader {
    reader: BufReader<rust_htslib::bgzf::Reader>, //TabixReader,     // https://docs.rs/rust-htslib/latest/rust_htslib/tbx/index.html
    last_rp: Option<(Vec<u8>,ReadPair)>,
}
impl TirpStreamingReadPairReader {
    pub fn new(fname: &PathBuf) -> anyhow::Result<TirpStreamingReadPairReader> {

        let reader= rust_htslib::bgzf::Reader::from_path(&fname)
            .expect(&format!("Could not open {:?}", fname));

        let mut reader = BufReader::new(reader);

        //Read the first read right away
        let mut record = String::new();
        let read_size = reader.read_line(&mut record).unwrap();

        if read_size>0 {

            //Remove newline and everything after
            let trimmed_line = &record.as_bytes()[0..(read_size-1)];
            let last_rp = parse_tirp_readpair_with_cellid(trimmed_line);

            Ok(TirpStreamingReadPairReader {
                reader: reader,
                last_rp: Some(last_rp)
            })
        } else {
            //The BAM file is empty!
            println!("Warning: empty input BAM");

            Ok(TirpStreamingReadPairReader {
                reader: reader,
                last_rp: None
            })
    
        }
    }
}




impl StreamingReadPairReader for TirpStreamingReadPairReader {


    fn get_reads_for_next_cell(
        &mut self
    ) -> anyhow::Result<Option<ListReadWithBarcode>> {

        //Check if we arrived at the end already
        if let Some((current_cell, last_rp)) = self.last_rp.clone()  {

            //First push the last read pair we had
            let mut reads:Vec<ReadPair> = Vec::new();
            reads.push(last_rp);
            self.last_rp = None;

            //Keep reading lines until we reach the next cell or the end
            let mut record = String::new();
            loop {

                //Read a line. Note that read_line appends to the buffer
                record.clear();
                let size= self.reader.read_line(&mut record).unwrap();
                if size==0 {
                    break;
                }

                //Remove newline and everything after
                let trimmed_line = &record.as_bytes()[0..(size-1)];

                let (cell_id, rp) = parse_tirp_readpair_with_cellid(trimmed_line);
                //println!("reading line {} {:?} {:?}", size, String::from_utf8_lossy(cell_id.as_slice()), String::from_utf8_lossy(current_cell.as_slice()));
                //println!("{}",&record[0..size]);
                //println!("{}", rp);
                if cell_id == current_cell {
                    //This read belongs to this cell, so add to the list and continue
                    //println!("one more read");
                    reads.push(rp);
                } else {
                    //This read belongs to the next cell, so stop reading for now
                    self.last_rp = Some((
                        cell_id.to_vec(),
                        rp
                    ));
                    break;
                }
                //print!("");
            }

            //Package and return data
            let reads = Arc::new(reads);
            let cellid_reads = (
                String::from_utf8(current_cell).unwrap(), 
                reads
            );

            Ok(Some(Arc::new(cellid_reads)))
        } else {
            //There is nothing more to read
            println!("Reached end of input TIRP file");
            Ok(None)
        }
    }
   
}












#[derive(Debug,Clone)]
pub struct TirpStreamingReadPairReaderFactory {
}
impl TirpStreamingReadPairReaderFactory {
    pub fn new() -> TirpStreamingReadPairReaderFactory {
        TirpStreamingReadPairReaderFactory {}
    } 
}
impl ConstructFromPath<TirpStreamingReadPairReader> for TirpStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<TirpStreamingReadPairReader> {  ///////// maybe anyhow prevents spec of reader?
        TirpStreamingReadPairReader::new(fname)
    }
}






///////////////////
/////////////////// Below is code for streaming of data from TIRPs -- shard system ........... can be put on top of any streaming pair reader; generalize later
///////////////////


pub struct TirpStreamingShardExtractor {
    reader: TirpStreamingReadPairReader,
    last_read: Option<ListReadWithBarcode>
}
impl TirpStreamingShardExtractor{
    pub fn new(fname: &PathBuf) -> anyhow::Result<TirpStreamingShardExtractor> {
        Ok(TirpStreamingShardExtractor {
            reader: TirpStreamingReadPairReader::new(fname)?,
            last_read: None
        })
    }
}

impl ShardStreamingFileExtractor for TirpStreamingShardExtractor {  /// can make it for any readpairstreamer. TODO generalize this

    fn next_cell (
        &mut self, 
    ) -> anyhow::Result<Option<CellID>> {

        //Get new set of reads
        let dat = self.reader.get_reads_for_next_cell()?;

        //Check if we still have cells
        let cellid = if let Some(d) = &dat {
            Ok(Some(d.0.clone()))
        } else {
            Ok(None)
        };
        
        self.last_read=dat;
        cellid
    }


    fn extract_to_outdir (
        &mut self, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool> {

        for f in needed_files {
            if f=="r1.fq" {
                if let Some(rp) = &self.last_read {
                    for one_read in rp.1.iter() {
                        
                        let p = out_directory.join("r1.fq");
                        let f=File::create(p).expect("Could not open r1.fq file for writing");
                        let mut writer=BufWriter::new(f);
                        
                        writer.write_all(b">x")?;
                        //writer.write_all(head.as_slice())?;  //no name of read needed
                        writer.write_all(b"\n")?;
                        writer.write_all(one_read.r1.as_slice())?;
                        writer.write_all(b"\n+\n")?;
                        writer.write_all(&one_read.q1.as_slice())?;
                        writer.write_all(b"\n")?;
                    }
                }
            } else if f=="r2.fq" {
                if let Some(rp) = &self.last_read {
                    for one_read in rp.1.iter() {
                        
                        let p = out_directory.join("r2.fq");
                        let f=File::create(p).expect("Could not open r2.fq file for writing");
                        let mut writer=BufWriter::new(f);
                        
                        writer.write_all(b">x")?;
                        //writer.write_all(head.as_slice())?;  //no name of read needed
                        writer.write_all(b"\n")?;
                        writer.write_all(one_read.r2.as_slice())?;
                        writer.write_all(b"\n+\n")?;
                        writer.write_all(&one_read.q2.as_slice())?;
                        writer.write_all(b"\n")?;
                    }
                }
            } else {
                if fail_if_missing {
                    return Ok(false)
                }
            }
        }
        Ok(true)
    }

}








#[derive(Debug,Clone)]
pub struct TirpStreamingShardReaderFactory {
}
impl TirpStreamingShardReaderFactory {
    pub fn new() -> TirpStreamingShardReaderFactory {
        TirpStreamingShardReaderFactory {}
    } 
}
impl ConstructFromPath<TirpStreamingShardExtractor> for TirpStreamingShardReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<TirpStreamingShardExtractor> {
        TirpStreamingShardExtractor::new(fname)
    }
}