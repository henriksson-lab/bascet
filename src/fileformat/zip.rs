use std::path::PathBuf;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::collections::HashMap;
use zip::read::ZipArchive;
use log::debug;
use anyhow::bail;

use super::shard::ShardFileExtractor;
use super::shard::ShardRandomFileExtractor;
use super::shard::CellID;
use super::ConstructFromPath;
use super::ShardCellDictionary;


///////////////////////////////
/// Factory of ZIP-readers, as shards
#[derive(Debug,Clone)]
pub struct ZipBascetShardReaderFactory {
}
impl ZipBascetShardReaderFactory {
    pub fn new() -> ZipBascetShardReaderFactory {
        ZipBascetShardReaderFactory {}
    } 
}
impl ConstructFromPath<ZipBascetShardReader> for ZipBascetShardReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<ZipBascetShardReader> {  ///////// maybe anyhow prevents spec of reader?
        ZipBascetShardReader::new(fname)
    }
}


///////////////////////////////
/// A reader of ZIP-files as shards
pub struct ZipBascetShardReader {

    pub files_for_cell: HashMap::<CellID,Vec<String>>,
    zip_shard: ZipArchive<BufReader<File>>,
    current_cell: CellID

}
impl ZipBascetShardReader {

    ///////////////////////////////
    /// Constructor of ZIP-file readers
    pub fn new(
        fname: &PathBuf
    ) -> anyhow::Result<ZipBascetShardReader> {

        //FIXME: file paths are not expanded and symlinks not resolved. 
        let file = File::open(fname).unwrap_or_else(|e| panic!("Failed to open bascet shard {}: {}", fname.display(), e));
        let bufreader_shard = BufReader::new(file);
        let zip_shard =    ZipArchive::new(bufreader_shard).unwrap();

        let mut files_for_cell: HashMap::<String,Vec<String>> = HashMap::new();


        for f in zip_shard.file_names() {
            let parts: Vec<&str> = f.splitn(2, "/").collect();
            let cell_name = parts[0];
            let file_name = parts[1];

            if file_name=="" {
                //This must be a directory
            } else {
                //Create map for this cell if not yet present. 
                //TODO update to better call for insert
                if !files_for_cell.contains_key(cell_name) {
                    files_for_cell.insert(String::from(cell_name), Vec::new());
                }
                let dict = files_for_cell.
                    get_mut(cell_name).
                   expect(&format!("cell missing, but this should be impossible. cell={:?}", cell_name));
                dict.push(String::from(file_name));
            }
        }

        debug!("{:?}", files_for_cell);

        Ok(ZipBascetShardReader {
            files_for_cell: files_for_cell,
            zip_shard: zip_shard,
            current_cell: "".to_string()

        })
    }

}
impl ShardCellDictionary for ZipBascetShardReader {


    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        let ret = self.files_for_cell.keys().map(|s| s.to_string()).collect::<Vec<String>>();
        Ok(ret)
    }

    fn has_cell(&mut self, cellid: &CellID) -> bool {
        self.files_for_cell.contains_key(cellid)
    }


}
impl ShardFileExtractor for ZipBascetShardReader {
    fn get_files_for_cell(
        &mut self, 
    ) -> anyhow::Result<Vec<String>>{
        if let Some(flist) = self.files_for_cell.get(&self.current_cell) {
            Ok(flist.clone())
        } else {
            bail!("Cell {:?} not in bascet", &self.current_cell)
        }
    }


    fn extract_as(
        &mut self, 
        file_name: &String,
        path_outfile: &PathBuf
    ) -> anyhow::Result<()>{

        let cell_id = &self.current_cell;
        let zip_fname = format!("{cell_id}/{file_name}");
        let mut entry = self.zip_shard.by_name(&zip_fname).expect("File is missing in zip");

        if entry.is_dir() {
            panic!("The specified file to unzip is a directory. This is currently not supported")
        } else if entry.is_file() {
            debug!("extracting");
            let file_out = File::create(&path_outfile).unwrap();
            let mut bufwriter_out = BufWriter::new(&file_out);
            let mut bufreader_found = BufReader::new(&mut entry);
            std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();

            println!("Copied! {}",path_outfile.display());

        } else {
            panic!("Unable to extract {} as unclear what it is", file_name);
        }

        Ok(())
    }


    fn extract_to_outdir (
        &mut self, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>{

        let cell_id = &self.current_cell;

        let list_files_for_cell = self.files_for_cell.get(cell_id).expect("expected cell to exist");

        //Figure out which files to get
        let mut list_toget: Vec<&String> = Vec::new();
        for f in needed_files {
            if f=="*" {
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

            let mut entry = self.zip_shard.by_name(&zip_fname).expect("Missing file, but was present before");

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

    /////////////////////////////// 
    /// Set cell to work with
    fn set_current_cell (
        &mut self,
        cell_id: &CellID
    ) {
        self.current_cell=cell_id.clone();
    }

}