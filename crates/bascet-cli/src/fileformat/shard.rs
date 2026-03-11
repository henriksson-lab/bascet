use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use super::DetectedFileformat;
use super::TirpBascetShardReader;
use super::ZipBascetShardReader;

///////////////////////////////
/// Type: Cell ID
pub type CellID = String;

///////////////////////////////
/// Type: UMI (unique molecular identifier)
pub type CellUMI = Vec<u8>;

type ListReadWithBarcode = Arc<(CellID, Arc<Vec<ReadPair>>)>;

///////////////////////////////
/// One pair of reads with a UMI
#[derive(Debug, Clone)]
pub struct ReadPair {
    pub r1: Vec<u8>,
    pub r2: Vec<u8>,
    pub q1: Vec<u8>,
    pub q2: Vec<u8>,
    pub umi: Vec<u8>,
}
impl fmt::Display for ReadPair {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            String::from_utf8_lossy(self.r1.as_slice()),
            String::from_utf8_lossy(self.r2.as_slice()),
            String::from_utf8_lossy(self.umi.as_slice())
        )
    }
}

///////////////////////////////
/// A constructor of objects given a path (a type of factory)
pub trait ConstructFromPath<R>
where
    Self: Clone,
{
    ///+Sized added later
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<R>
    where
        Self: Sized;
}

///////////////////////////////
/// A writer of pairs of reads for a cell
pub trait ReadPairWriter {
    ///////////////////////////////
    /// Write all read pairs for a given cell
    fn write_reads_for_cell(&mut self, cell_id: &CellID, list_reads: &Arc<Vec<ReadPair>>);

    fn writing_done(&mut self) -> anyhow::Result<()>;
}

///////////////////////////////
/// A random reader of pairs of reads from a cell
pub trait ReadPairReader {
    ///////////////////////////////
    /// Read all read pairs for a given cell
    fn get_reads_for_cell(&mut self, cell_id: &CellID) -> anyhow::Result<Arc<Vec<ReadPair>>>;
}

///////////////////////////////
/// A streaming reader of pairs of reads from a cell
pub trait StreamingReadPairReader {
    /// Read all read pairs for the next cell being streamed
    fn get_reads_for_next_cell(&mut self) -> anyhow::Result<Option<ListReadWithBarcode>>;
}

///////////////////////////////
/// A file that can return which cells are present in it
pub trait ShardCellDictionary {
    ///////////////////////////////
    /// Get list of cells in this file
    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>>;

    ///////////////////////////////
    /// Check if a cell is present
    fn has_cell(&mut self, cellid: &CellID) -> bool;
}

///////////////////////////////
/// Common shard reader trait
pub trait ShardRandomFileExtractor {
    ///////////////////////////////
    /// Set cell to work with
    fn set_current_cell(&mut self, cell_id: &CellID);
}

///////////////////////////////
/// Common shard reader trait   -- streaming I/O
pub trait ShardStreamingFileExtractor {
    //Or CellFileExtractor, make common to above

    ///////////////////////////////
    /// Move to the next cell in the stream
    fn next_cell(&mut self) -> anyhow::Result<Option<CellID>>;
}

///////////////////////////////
/// Common shard reader trait   -- streaming I/O
pub trait ShardFileExtractor {
    //Or CellFileExtractor, make common to above

    ///////////////////////////////
    /// Extract requested file
    fn extract_as(&mut self, file_name: &String, path_outfile: &PathBuf) -> anyhow::Result<()>;

    ///////////////////////////////
    /// Extract requested files to a directory
    fn extract_to_outdir(
        &mut self,
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf,
    ) -> anyhow::Result<bool>;

    ///////////////////////////////
    /// Return a list of files associated with given cell
    fn get_files_for_cell(&mut self) -> anyhow::Result<Vec<String>>;
}

///////////////////////////////
/// An enum holding the type of readers that Bascet supports.
/// instead of dyn, an enum might be a better choice to cover all the different traits being implemented, since not every reader has every property!
pub enum DynShardReader {
    TirpBascetShardReader(TirpBascetShardReader),
    ZipBascetShardReader(ZipBascetShardReader),
}
impl ShardCellDictionary for DynShardReader {
    fn get_cell_ids(&mut self) -> anyhow::Result<Vec<CellID>> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.get_cell_ids(),
            DynShardReader::ZipBascetShardReader(r) => r.get_cell_ids(),
        }
    }
    fn has_cell(&mut self, cellid: &CellID) -> bool {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.has_cell(&cellid),
            DynShardReader::ZipBascetShardReader(r) => r.has_cell(&cellid),
        }
    }
}
impl ShardRandomFileExtractor for DynShardReader {
    ///////////////////////////////
    /// Set cell to work with
    fn set_current_cell(&mut self, cell_id: &CellID) {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.set_current_cell(&cell_id),
            DynShardReader::ZipBascetShardReader(r) => r.set_current_cell(&cell_id),
        }
    }
}
impl ShardFileExtractor for DynShardReader {
    fn extract_to_outdir(
        &mut self,
        needed_files: &Vec<String>,
        fail_if_missing: bool,
        out_directory: &PathBuf,
    ) -> anyhow::Result<bool> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => {
                r.extract_to_outdir(&needed_files, fail_if_missing, &out_directory)
            }
            DynShardReader::ZipBascetShardReader(r) => {
                r.extract_to_outdir(&needed_files, fail_if_missing, &out_directory)
            }
        }
    }

    fn get_files_for_cell(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.get_files_for_cell(),
            DynShardReader::ZipBascetShardReader(r) => r.get_files_for_cell(),
        }
    }

    fn extract_as(&mut self, file_name: &String, path_outfile: &PathBuf) -> anyhow::Result<()> {
        match self {
            DynShardReader::TirpBascetShardReader(r) => r.extract_as(&file_name, &path_outfile),
            DynShardReader::ZipBascetShardReader(r) => r.extract_as(&file_name, &path_outfile),
        }
    }
}

///////////////////////////////
/// Given a path, get a suitable shard reader
pub fn get_shard_reader_for_path(p: &PathBuf) -> anyhow::Result<DynShardReader> {
    match crate::fileformat::detect_shard_format(&p) {
        DetectedFileformat::TIRP => Ok(DynShardReader::TirpBascetShardReader(
            TirpBascetShardReader::new(p)
                .expect(format!("Failed to read {}", p.display()).as_str()),
        )),
        DetectedFileformat::ZIP => Ok(DynShardReader::ZipBascetShardReader(
            ZipBascetShardReader::new(p).expect(format!("Failed to read {}", p.display()).as_str()),
        )),
        _ => {
            anyhow::bail!(
                "File format for {} does not support listing of cell IDs",
                p.display()
            )
        }
    }
}

///////////////////////////////
/// Given a path to a shard file, get a dictionary that can return which cells are in it
pub fn get_dyn_celldict(p: &PathBuf) -> anyhow::Result<Box<dyn ShardCellDictionary>> {
    match crate::fileformat::detect_shard_format(&p) {
        DetectedFileformat::TIRP => Ok(Box::new(
            TirpBascetShardReader::new(p)
                .expect(format!("Unable to read cell list for {}", p.display()).as_str()),
        )),
        DetectedFileformat::ZIP => Ok(Box::new(
            ZipBascetShardReader::new(p)
                .expect(format!("Unable to read cell list for {}", p.display()).as_str()),
        )),
        _ => {
            anyhow::bail!(
                "File format for {} does not support listing of cell IDs",
                p.display()
            )
        }
    }
}

///////////////////////////////
/// Try to figure out what cells are present in an input file.           
/// If we cannot list the cells for this file then it will have to stream all the content
pub fn try_get_cells_in_file(p: &PathBuf) -> anyhow::Result<Option<Vec<CellID>>> {
    let mut cell_dict = get_dyn_celldict(p)
        .expect(format!("Unable to read cell list for {}", p.display()).as_str());
    Ok(Some(cell_dict.get_cell_ids().unwrap()))
}

///////////////////////////////
/// Row in a histogram for cell barcode counting (used for serialization)
#[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
struct BarcodeHistogramRow {
    bc: String,
    cnt: u64,
}

///////////////////////////////
/// Histogram for cell barcode counting
pub struct BarcodeHistogram {
    histogram: HashMap<CellID, u64>,
}
impl BarcodeHistogram {
    /// Create a new empty histogram
    pub fn new() -> BarcodeHistogram {
        BarcodeHistogram {
            histogram: HashMap::new(),
        }
    }

    /// Increment the count of one cell by 1
    pub fn inc(&mut self, cellid: &CellID) {
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += 1;
    }

    /// Increment the count of one cell
    pub fn inc_by(&mut self, cellid: &CellID, cnt: &u64) {
        let counter = self.histogram.entry(cellid.clone()).or_insert(0);
        *counter += cnt;
    }

    /// Add a histogram to this histogram
    pub fn add_histogram(&mut self, other: &BarcodeHistogram) {
        for (cellid, v) in other.histogram.iter() {
            let counter = self.histogram.entry(cellid.clone()).or_insert(0);
            *counter += v;
        }
    }

    /// Read histogram from file
    pub fn from_file(fname: &PathBuf) -> anyhow::Result<BarcodeHistogram> {
        //Open file
        let file = File::open(fname)?;
        let reader = BufReader::new(file);

        //Read it as a CSV file
        let mut hist = BarcodeHistogram::new();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(reader);
        for result in reader.deserialize() {
            let record: BarcodeHistogramRow = result.unwrap();
            hist.histogram.insert(record.bc, record.cnt);
        }
        Ok(hist)
    }

    /// Write histogram to file
    pub fn write_file(&self, fname: &PathBuf) -> anyhow::Result<()> {
        //Open file
        let mut writer = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .from_path(fname)
            .expect("Could not open histogram file for writing");

        for (bc, cnt) in self.histogram.iter() {
            let _ = writer.serialize(BarcodeHistogramRow {
                bc: bc.to_string(),
                cnt: *cnt,
            });
        }

        let _ = writer.flush();
        Ok(())
    }
}
