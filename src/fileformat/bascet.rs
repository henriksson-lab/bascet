use std::path::PathBuf;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::collections::HashMap;
use zip::read::ZipArchive;
use log::debug;
use anyhow::bail;


use crate::fileformat::gascet::CellID;


pub trait ShardReader {

    fn extract_to_outdir (
        &mut self, 
        cell_id: &CellID, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>;

    fn new(fname: &PathBuf) -> anyhow::Result<Self> where Self: Sized;

    fn get_files_for_cell(&mut self, cell_id: &CellID) -> anyhow::Result<Vec<String>>;

    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>>;
}





//#[derive(Debug)]  //// not sure about all of these
pub struct BascetShardReader {

    pub files_for_cell: HashMap::<CellID,Vec<String>>,
    zip_shard: ZipArchive<BufReader<File>>

}


impl ShardReader for BascetShardReader {


    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        let ret = self.files_for_cell.keys().map(|s| s.to_string()).collect::<Vec<String>>();
        Ok(ret)
    }



    fn get_files_for_cell(&mut self, cell_id: &CellID) -> anyhow::Result<Vec<String>>{
        if let Some(flist) = self.files_for_cell.get(cell_id) {
            Ok(flist.clone())
        } else {
            bail!("Cell {:?} not in bascet", &cell_id)
        }
    }



    fn new(fname: &PathBuf) -> anyhow::Result<BascetShardReader> {

        let file = File::open(fname).expect("Failed to open bascet shard");
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
                //Create map for this cell if not yet present. TODO update to better call for insert
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

        Ok(BascetShardReader {
            files_for_cell: files_for_cell,
            zip_shard: zip_shard

        })
    }






    fn extract_to_outdir (
        &mut self, 
        cell_id: &String, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>{

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