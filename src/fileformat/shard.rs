use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::fmt;

use super::ZipBascetShardReader;
use super::TirpBascetShardReader;
use super::DetectedFileformat;

//use super::TirpBascetShardReaderFactory;
//use crate::ZipBascetShardReaderFactory;


///////////////////////////////
/////////////////////////////// The type of the cell ID
///////////////////////////////

pub type CellID = String;
pub type CellUMI = Vec<u8>;



///////////////////////////////
/////////////////////////////// One pair of reads with UMI
///////////////////////////////


#[derive(Debug,Clone)]
pub struct ReadPair {
    pub r1: Vec<u8>,
    pub r2: Vec<u8>,
    pub q1: Vec<u8>,
    pub q2: Vec<u8>,
    pub umi: Vec<u8>
}

impl fmt::Display for ReadPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Use `self.number` to refer to each positional data point.
        write!(f, "({}, {}, {})", 
            String::from_utf8_lossy(self.r1.as_slice()), 
            String::from_utf8_lossy(self.r2.as_slice()) ,
            String::from_utf8_lossy(self.umi.as_slice()) 
    )
    }
}





///////////////////////////////
/////////////////////////////// 
///////////////////////////////


pub trait ConstructFromPath<R> where Self: Clone { ///+Sized added later
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<R> where Self: Sized;
}

/* 
impl<R> ConstructFromPath<R> for ConstructFromPath<Box<dyn R>> where R: Clone+ConstructFromPath<R> {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<R> where Self: Sized {
        self.new_from_path(fname)
    }
}*/


///////////////////////////////
/////////////////////////////// Common shard writer and reader traits, to handle readpairs only
///////////////////////////////

type ListReadWithBarcode = Arc<(CellID,Arc<Vec<ReadPair>>)>;


pub trait ReadPairWriter {

    fn write_reads_for_cell(
        &mut self, 
        cell_id:&CellID, 
        list_reads: &Arc<Vec<ReadPair>>
    );

}


pub trait ReadPairReader { //where Self: ConstructFromPath

    fn get_reads_for_cell(
        &mut self, 
        cell_id:&CellID
    ) -> anyhow::Result<Arc<Vec<ReadPair>>>;

}



pub trait StreamingReadPairReader { //where Self: ConstructFromPath

    fn get_reads_for_next_cell(
        &mut self
    ) -> anyhow::Result<Option<ListReadWithBarcode>>;

}


///////////////////////////////
/////////////////////////////// Common shard trait to get what files are in it
///////////////////////////////


pub trait ShardCellDictionary {

    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>>;
    fn has_cell(&mut self, cellid: &CellID) -> bool;

}

///////////////////////////////
/////////////////////////////// Common shard reader trait  -- random I/O
///////////////////////////////


pub trait ShardRandomFileExtractor  { 

    fn extract_to_outdir (
        &mut self, 
        cell_id: &CellID, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>;

    fn get_files_for_cell(
        &mut self, 
        cell_id: &CellID
    ) -> anyhow::Result<Vec<String>>;

    fn extract_as(
        &mut self, 
        cell_id: &String, 
        file_name: &String,
        path_outfile: &PathBuf
    ) -> anyhow::Result<()>;

}


///////////////////////////////
/////////////////////////////// Common shard reader trait   -- streaming I/O
///////////////////////////////


pub trait ShardStreamingFileExtractor  { //Or CellFileExtractor, make common to above

    fn next_cell (
        &mut self, 
    ) -> anyhow::Result<Option<CellID>>;


    fn extract_to_outdir (
        &mut self, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool>;

    fn get_files_for_cell(
        &mut self
    ) -> anyhow::Result<Vec<String>>;

}






//////////////////
////////////////// instead of dyn, an enum might be a better choice to cover all the different traits being implemented, since not every reader has every property!
//////////////////

pub enum DynShardReader {
    TirpBascetShardReader(TirpBascetShardReader),
    ZipBascetShardReader(ZipBascetShardReader)
}
/* 
impl DynShardReader {  ////// attempt at generalizing

    //This cannot be made to work as mut escapes into the box. the mut is later needed to read the cell dict.
    //so need to read it in advance
    pub fn get_celldict(&mut self) -> Box<dyn ShardCellDictionary> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => Box::new(&r),
            DynShardReader::ZipBascetShardReader(r) => Box::new(&r),
        }

    }

}

pub fn as_celldict<'a>(reader: &'a mut DynShardReader) -> &'a mut Box<&'a dyn ShardCellDictionary> {
    match reader {
        DynShardReader::TirpBascetShardReader(r) => &mut Box::new(r),
        DynShardReader::ZipBascetShardReader(r) => &mut Box::new(r)
    }
}
*/




pub fn get_reader_for_path(p: &PathBuf) -> anyhow::Result<DynShardReader> {
    match crate::fileformat::detect_shard_format(&p) {
        DetectedFileformat::TIRP => {
            Ok(DynShardReader::TirpBascetShardReader(TirpBascetShardReader::new(p).expect(format!("Failed to read {}",p.display()).as_str())))
        },
        DetectedFileformat::ZIP => {
            Ok(DynShardReader::ZipBascetShardReader(ZipBascetShardReader::new(p).expect(format!("Failed to read {}",p.display()).as_str())))
        },
        _ => { 
            anyhow::bail!("File format for {} does not support listing of cell IDs", p.display()) 
        }
    }
}

impl ShardCellDictionary for DynShardReader {

    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.get_cell_ids(),
            DynShardReader::ZipBascetShardReader(r) => r.get_cell_ids()
        }
    }
    fn has_cell(&mut self, cellid: &CellID) -> bool {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.has_cell(&cellid),
            DynShardReader::ZipBascetShardReader(r) => r.has_cell(&cellid)
        }
    }
}


impl ShardRandomFileExtractor for DynShardReader {

    fn extract_to_outdir (
        &mut self, 
        cell_id: &CellID, 
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf
    ) -> anyhow::Result<bool> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.extract_to_outdir(&cell_id, &needed_files, fail_if_missing, &out_directory),
            DynShardReader::ZipBascetShardReader(r) => r.extract_to_outdir(&cell_id, &needed_files, fail_if_missing, &out_directory),
        }
    }

    fn get_files_for_cell(
        &mut self, 
        cell_id: &CellID
    ) -> anyhow::Result<Vec<String>> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.get_files_for_cell(&cell_id),
            DynShardReader::ZipBascetShardReader(r) => r.get_files_for_cell(&cell_id)
        }
    }

    fn extract_as(
        &mut self, 
        cell_id: &String, 
        file_name: &String,
        path_outfile: &PathBuf
    ) -> anyhow::Result<()> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.extract_as(&cell_id, &file_name, &path_outfile),
            DynShardReader::ZipBascetShardReader(r) => r.extract_as(&cell_id, &file_name, &path_outfile),
        }
    }

}





//////////////////
//////////////////
//////////////////
//////////////////


pub fn get_dyn_celldict(
    p: &PathBuf
) -> anyhow::Result<Box<dyn ShardCellDictionary>> {

    match crate::fileformat::detect_shard_format(&p) {
        DetectedFileformat::TIRP => {
            Ok(Box::new(TirpBascetShardReader::new(p).expect(format!("Unable to read cell list for {}",p.display()).as_str())))
        },
        DetectedFileformat::ZIP => {
            Ok(Box::new(ZipBascetShardReader::new(p).expect(format!("Unable to read cell list for {}",p.display()).as_str())))
        },
        _ => { 
            anyhow::bail!("File format for {} does not support listing of cell IDs", p.display()) 
        }
    }

}


//////////////////
/// Try to figure out what cells are present in an input file.           
/// If we cannot list the cells for this file then it will have to stream all the content
pub fn try_get_cells_in_file(
    p: &PathBuf
) -> anyhow::Result<Option<Vec<CellID>>> {

    let mut cell_dict = get_dyn_celldict(p).
        expect(format!("Unable to read cell list for {}",p.display()).as_str());
    Ok(Some(cell_dict.get_cell_ids().unwrap()))
    
}































///////////////////////////////
/////////////////////////////// Histogram for cell barcode counting
///////////////////////////////



#[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct HistogramCsvRow {
    bc: String,
    cnt: u64
}


pub struct BarcodeHistogram {
    histogram: HashMap<CellID, u64>
}
impl BarcodeHistogram {

    pub fn new() -> BarcodeHistogram {
        BarcodeHistogram {
            histogram: HashMap::new()
        }
    }

    pub fn inc(
        &mut self, 
        cellid: &CellID
    ){        
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += 1;
    }

    pub fn inc_by(
        &mut self, 
        cellid: &CellID, 
        cnt: &u64
    ){        
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += cnt;
    }

    pub fn add_histogram(
        &mut self, 
        other: &BarcodeHistogram
    ) {
        for (cellid,v) in other.histogram.iter() {
            let counter = self.histogram.entry(cellid.clone()).or_insert(0);
            *counter += v;    
        }
    }


    pub fn from_file(
        fname: &PathBuf
    ) -> anyhow::Result<BarcodeHistogram> {

        //Open file
        let file = File::open(fname)?;
        let reader= BufReader::new(file);

        //Read it as a CSV file
        let mut hist = BarcodeHistogram::new();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(reader);
        for result in reader.deserialize() {
            let record: HistogramCsvRow = result.unwrap();
            hist.histogram.insert(record.bc, record.cnt);
        }
        Ok(hist)
    }

    pub fn write(
        &self, 
        fname: &PathBuf
    ) -> anyhow::Result<()> {

            //Open file
            let mut writer = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_path(fname)
                .expect("Could not open histogram file for writing");

            for (bc, cnt) in self.histogram.iter() {
                let _ = writer.serialize(HistogramCsvRow {
                    bc: bc.to_string(),
                    cnt: *cnt
                });
            }

            let _ = writer.flush();
        Ok(())
    }


}



