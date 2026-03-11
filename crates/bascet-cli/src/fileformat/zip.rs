use anyhow::bail;
use tracing::debug;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use zip::read::ZipArchive;

use crate::fileformat::ReadPair;
use crate::fileformat::StreamingReadPairReader;

use super::ConstructFromPath;
use super::ShardCellDictionary;
use super::shard::CellID;
use super::shard::ShardFileExtractor;
use super::shard::ShardRandomFileExtractor;


/// 
/// Factory of ZIP-readers, as shards
/// 
#[derive(Debug, Clone)]
pub struct ZipBascetShardReaderFactory {}
impl ZipBascetShardReaderFactory {
    pub fn new() -> ZipBascetShardReaderFactory {
        ZipBascetShardReaderFactory {}
    }
}
impl ConstructFromPath<ZipBascetShardReader> for ZipBascetShardReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<ZipBascetShardReader> {
        ///////// maybe anyhow prevents spec of reader?
        ZipBascetShardReader::new(fname)
    }
}

///////////////////////////////
/// A reader of ZIP-files as shards
pub struct ZipBascetShardReader {
    pub files_for_cell: HashMap<CellID, Vec<String>>,
    zip_shard: ZipArchive<BufReader<File>>,
    current_cell_index: usize, //CellID,
    list_cells: Vec<CellID>,
}
impl ZipBascetShardReader {
    /// 
    /// Constructor of ZIP-file readers
    /// 
    pub fn new(fname: &PathBuf) -> anyhow::Result<ZipBascetShardReader> {
        //FIXME: file paths are not expanded and symlinks not resolved.
        let file = File::open(fname)
            .unwrap_or_else(|e| panic!("Failed to open bascet shard {}: {}", fname.display(), e));
        let bufreader_shard = BufReader::new(file);
        let zip_shard = ZipArchive::new(bufreader_shard).unwrap();

        let mut files_for_cell: HashMap<String, Vec<String>> = HashMap::new();

        for f in zip_shard.file_names() {
            let parts: Vec<&str> = f.splitn(2, "/").collect();
            let cell_name = parts[0];
            let file_name = parts[1];

            if file_name == "" {
                //This must be a directory
            } else {
                //Create map for this cell if not yet present.
                //TODO update to better call for insert
                if !files_for_cell.contains_key(cell_name) {
                    files_for_cell.insert(String::from(cell_name), Vec::new());
                }
                let dict = files_for_cell.get_mut(cell_name).expect(&format!(
                    "cell missing, but this should be impossible. cell={:?}",
                    cell_name
                ));
                dict.push(String::from(file_name));
            }
        }

        debug!("{:?}", files_for_cell);
        let list_cells = files_for_cell.keys().cloned().collect();

        Ok(ZipBascetShardReader {
            files_for_cell: files_for_cell,
            zip_shard: zip_shard,
            current_cell_index: 0,//"".to_string(),
            list_cells: list_cells,
        })
    }
}
impl ShardCellDictionary for ZipBascetShardReader {
    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        let ret = self
            .files_for_cell
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        Ok(ret)
    }

    fn has_cell(&mut self, cellid: &CellID) -> bool {
        self.files_for_cell.contains_key(cellid)
    }
}
impl ShardFileExtractor for ZipBascetShardReader {
    fn get_files_for_cell(&mut self) -> anyhow::Result<Vec<String>> {
        let current_cell = self.list_cells.get(self.current_cell_index);
        if let Some(current_cell) = current_cell {
            let flist=self.files_for_cell.get(current_cell).unwrap();
            anyhow::Ok(flist.clone())
        } else {
            bail!("Cell # {} not in bascet", &self.current_cell_index)
        }
    }

    fn extract_as(&mut self, file_name: &String, path_outfile: &PathBuf) -> anyhow::Result<()> {
        let cell_id = self.list_cells.get(self.current_cell_index).expect("Current cell outside range"); 
        let zip_fname = format!("{cell_id}/{file_name}");
        let mut entry = self
            .zip_shard
            .by_name(&zip_fname)
            .expect("File is missing in zip");

        if entry.is_dir() {
            panic!("The specified file to unzip is a directory. This is currently not supported")
        } else if entry.is_file() {
            debug!("extracting");
            let file_out = File::create(&path_outfile).unwrap();
            let mut bufwriter_out = BufWriter::new(&file_out);
            let mut bufreader_found = BufReader::new(&mut entry);
            std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();

            println!("Copied! {}", path_outfile.display());
        } else {
            panic!("Unable to extract {} as unclear what it is", file_name);
        }

        Ok(())
    }

    fn extract_to_outdir(
        &mut self,
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf,
    ) -> anyhow::Result<bool> {
        let cell_id = self.list_cells.get(self.current_cell_index).expect("Current cell outside range"); 
//        let cell_id = &self.current_cell;

        let list_files_for_cell = self
            .files_for_cell
            .get(cell_id)
            .expect("expected cell to exist");

        //Figure out which files to get
        let mut list_toget: Vec<&String> = Vec::new();
        for f in needed_files {
            if f == "*" {
                list_toget.extend(list_files_for_cell.iter());
            } else {
                //Check if file is present
                if fail_if_missing & !list_files_for_cell.contains(f) {
                    return Ok(false);
                    //                    bail!("Not all expected files are present for the cell");
                }

                list_toget.push(f);
            }
        }

        //Extract all expected files that are present
        for fname in list_toget {
            let zip_fname = format!("{cell_id}/{fname}");

            let mut entry = self
                .zip_shard
                .by_name(&zip_fname)
                .expect("Missing file, but was present before");

            if entry.is_dir() {
                //maybe we should exclude these at earlier stage?
                //or go through needed files!
            }

            if entry.is_file() {
                debug!("extracting"); // {}",fname)
                //TODO add dirs if needed
                //            let _ = fs::create_dir(&path_temp_dir);

                let path_outfile = out_directory.join(PathBuf::from(fname));
                let file_out = File::create(&path_outfile).unwrap();
                let mut bufwriter_out = BufWriter::new(&file_out);
                let mut bufreader_found = BufReader::new(&mut entry);
                std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();
            }
        }
        Ok(true)
    }
}
impl ShardRandomFileExtractor for ZipBascetShardReader {
    /// 
    /// Set current cell to work with
    /// 
    fn set_current_cell(&mut self, cell_id: &CellID) {
        let ind = self.list_cells.iter().position(|n| n == cell_id);
        if let Some(ind) = ind {
            self.current_cell_index=ind;
        } else {
            panic!("No such cellID: {}",cell_id);
        }
    }
}





#[derive(Debug, Clone)]
pub struct ZipStreamingReadPairReaderFactory {}
impl ZipStreamingReadPairReaderFactory {
    pub fn new() -> ZipStreamingReadPairReaderFactory {
        ZipStreamingReadPairReaderFactory {}
    }
}
impl ConstructFromPath<ZipBascetShardReader> for ZipStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<ZipBascetShardReader> {
        ///////// maybe anyhow prevents spec of reader?
        ZipBascetShardReader::new(fname)
    }
}




type ListReadWithBarcode = Arc<(CellID, Arc<Vec<ReadPair>>)>;


impl StreamingReadPairReader for ZipBascetShardReader {

    fn get_reads_for_next_cell(&mut self) -> anyhow::Result<Option<ListReadWithBarcode>> {

        let cell_id = self.list_cells.get(self.current_cell_index);
        if let Some(cell_id) = cell_id {
            //Ensure we move to the next cell for the next call
            self.current_cell_index += 1;

            //Check different ways the reads can be stored in
            let zip_fname_contigs = format!("{cell_id}/contigs.fa");
            let zip_fname_r1 = format!("{cell_id}/r1.fa");
            let zip_fname_r2 = format!("{cell_id}/r2.fa");
            if let Some(dat) = parse_fasta_to_strings(&mut self.zip_shard, &zip_fname_contigs) {
                // A single contigs.fa; empty R2
                let list_rp:Vec<ReadPair> = dat.iter().map(|r| {
                    let qs = make_good_q_for_seq(r.as_bytes());
                    ReadPair {
                        r1: r.clone().into_bytes(),
                        r2: Vec::new(),
                        q1: qs,
                        q2: Vec::new(),
                        umi: Vec::new(),
                    }                    
                }).collect();
                let list_rp = Arc::new(list_rp);
                anyhow::Ok(Some(Arc::new((cell_id.clone(), list_rp))))
            } else if let Some(dat_r1) = parse_fasta_to_strings(&mut self.zip_shard, &zip_fname_r1) {
                // R1 and R2 expected
                let dat_r2 = parse_fasta_to_strings(&mut self.zip_shard, &zip_fname_r2).expect("Found r1.fa, but not r2.fa");
                let twodat = dat_r1.iter().zip(dat_r2.iter());
                let list_rp:Vec<ReadPair> = twodat.map(|(r1,r2)| {
                    let q1 = make_good_q_for_seq(r1.as_bytes());
                    let q2 = make_good_q_for_seq(r2.as_bytes());
                    ReadPair {
                        r1: r1.clone().into_bytes(),
                        r2: r2.clone().into_bytes(),
                        q1: q1,
                        q2: q2,
                        umi: Vec::new(),
                    }                    
                }).collect();                
                let list_rp = Arc::new(list_rp);
                anyhow::Ok(Some(Arc::new((cell_id.clone(), list_rp))))
            } else {
                panic!("No FASTA content for this cell; FASTQ not supported yet");
            }
            //anyhow::Ok(None)
        } else {
            anyhow::Ok(None)
        }
    }

}


fn make_good_q_for_seq(
    r:&[u8],
) -> Vec<u8> {
    let mut qs=Vec::new();
    for _i in 0..r.len() {
        qs.push(b'F');
    }
    qs
}


///
/// Attempt to parse a FASTA to list of strings (one per record)
/// 
fn parse_fasta_to_strings(
    zip_shard: &mut ZipArchive<BufReader<File>>, 
    zip_fname: &String
) -> Option<Vec<String>> {
    let mut entry = zip_shard
        .by_name(&zip_fname);
    if let Ok(entry) = &mut entry {
        let bufreader_found = BufReader::new(entry);
        let mut list_read = Vec::new();
        let mut lines = bufreader_found.lines();

        while let Some(v1) = lines.next() {
            let v1=v1.unwrap();
            let v2=lines.next().expect("No sequence line").unwrap();
            let v2=v2.trim();
            if !v1.starts_with(">") {
                panic!("Expected >; is this FASTA content?")
            }

            list_read.push(v2.to_string());
        }
        Some(list_read)
    } else {
        None
    }
}


/*
///
/// Return a list of (sequence,qscore)
/// Not used yet
/// 
fn parse_fastq_to_strings(
    zip_shard: &mut ZipArchive<BufReader<File>>, 
    zip_fname: &String
) -> Option<Vec<(String, String)>> {
    let mut entry = zip_shard
        .by_name(&zip_fname);
    if let Ok(entry) = &mut entry {
        let bufreader_found = BufReader::new(entry);
        let mut list_read = Vec::new();
        let mut lines = bufreader_found.lines();

        while let Some(line_name) = lines.next() {
            let line_name=line_name.unwrap();
            if !line_name.starts_with("@") {
                panic!("Expected @; is this FASTQ content?")
            }

            let line_seq=lines.next().expect("No FASTQ seq line").unwrap();
            let _line_plus=lines.next().expect("No + line").unwrap();
            let line_q=lines.next().expect("No FASTQ Qscore line").unwrap();

            let line_seq=line_seq.trim();
            let line_q=line_q.trim();
            list_read.push(
                (line_seq.to_string(), line_q.to_string())
            );
        }
        Some(list_read)
    } else {
        None
    }
}
    */