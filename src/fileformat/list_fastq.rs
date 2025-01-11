use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;

use super::ConstructFromPath;
//use super::ShardFileExtractor;
use super::ReadPair;
use super::CellID;
use super::ReadPairReader;
use super::ShardCellDictionary;

use seq_io::fastq::Reader as FastqReader;




#[derive(Debug, serde::Deserialize, Eq, PartialEq, Clone)]
struct OneFastqPair {
    name: CellID,
    r1: PathBuf,
    r2: PathBuf
}


pub struct ListFastqReader {
    records: HashMap<CellID, OneFastqPair>,
    num_read: u32
}
impl ListFastqReader {

    pub fn new(p: &PathBuf) -> anyhow::Result<ListFastqReader>{

        let f=File::open(p).expect("Failed to open listfastq file");
        let reader = BufReader::new(f);

        let mut cb: ListFastqReader = ListFastqReader {
            records: HashMap::new(),
            num_read: 0
        };

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(reader);
        for result in reader.deserialize() {
            let record: OneFastqPair = result.unwrap();
            cb.records.insert(record.name.clone(), record);
        }

        if cb.records.is_empty() {
            println!("Warning: empty list-of-fastq file");
        }
        Ok(cb)
    }

}








#[derive(Debug, Clone)]
pub struct ListFastqReaderFactory {
}
impl ListFastqReaderFactory {
    pub fn new() -> ListFastqReaderFactory {
        ListFastqReaderFactory {}
    } 
}
impl ConstructFromPath<ListFastqReader> for ListFastqReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<ListFastqReader> {  ///////// maybe anyhow prevents spec of reader?
        ListFastqReader::new(fname)
    }
}




impl ShardCellDictionary for ListFastqReader {

    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        let ret = self.records.keys().map(|s| s.to_string()).collect::<Vec<String>>();
        Ok(ret)
    }

    fn has_cell(&mut self, cellid: &CellID) -> bool {
        self.records.contains_key(cellid)
    }

}







impl ReadPairReader for ListFastqReader {


    fn get_reads_for_cell(
        &mut self, 
        cell_id: &String, 
    ) -> anyhow::Result<Arc<Vec<ReadPair>>>{


        let mut list_rp: Vec<ReadPair> = Vec::new();

        let mut forward_file = open_fastq(&self.records.get(cell_id).unwrap().r1).expect("Could not open fastq R1");
        let mut reverse_file = open_fastq(&self.records.get(cell_id).unwrap().r2).expect("Could not open fastq R2");

        loop {
            if let Some(record) = reverse_file.next() {
                let reverse_record = record.expect("Error reading record rev");
                let forward_record = forward_file.next().unwrap().expect("Error reading record fwd");
    
                //Owning the records will deal with concatenation of lines
                let reverse_record = reverse_record.to_owned_record();
                let forward_record = forward_record.to_owned_record();
    
                let umi = Vec::new();
                let rp = ReadPair {
                    r1: forward_record.seq,
                    r2: reverse_record.seq,
                    q1: forward_record.qual,
                    q2: reverse_record.qual,
                    umi: umi
    
                };
                list_rp.push(rp);
    
                self.num_read += 1;
    
                if self.num_read % 100000 == 0 {
                    println!("read: {:?}", self.num_read);
                }
    
            } else {
                break
            }
        }
        Ok(Arc::new(list_rp))
    }


}





//// TODO also in getraw; consolidate in some common util?
pub fn open_fastq(file_handle: &PathBuf) -> anyhow::Result<FastqReader<Box<dyn std::io::Read>>> {

    let opened_handle = File::open(file_handle).
        expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    let (reader,compression) = niffler::get_reader(Box::new(opened_handle)).
        expect(format!("Could not open fastq file {}", &file_handle.display()).as_str());

    log::debug!(
        "Opened file {} with compression {:?}",
        &file_handle.display(),
        &compression
    );
    Ok(FastqReader::new(reader))
}


