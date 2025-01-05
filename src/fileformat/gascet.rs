/// disabled mod for now

/* 
General
Annotated
Single
CEll read
Table
*/

use std::cmp::Ordering;
use std::sync::Arc;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::hash::{Hash, Hasher};

//use std::collections::BinaryHeap;

//use priority_queue::PriorityQueue;

//use anyhow::Result;
use anyhow::bail;
//use log::info;
use log::debug;
use rand_distr::num_traits::ToPrimitive;


//Note: already got a multithreaded writer!
//use bgzip::{write::BGZFMultiThreadWriter, write::BGZFWriter, BGZFError, Compression};

/* 
use bgzip::read::BGZFWriter;
use bgzip::read::BGZFError;
use bgzip::read::Compression;
*/

use bgzip::read::BGZFMultiThreadReader;

// https://docs.rs/bgzip/latest/bgzip/read/struct.BGZFMultiThreadReader.html








#[derive(Debug)]  
struct GascetReadPair {
//    pub barcode: Vec<u8>,
    pub umi: Vec<u8>,

    pub r1: Vec<u8>,
    pub r2: Vec<u8>,

    pub q1: Vec<u8>,
    pub q2: Vec<u8>,    
}

type BarcodedGascetReadPair = (Vec<u8>, GascetReadPair);




#[derive(Debug)]  
struct GascetHeader {
    pub version_major: u8,
    pub version_minor: u8,
    pub is_sorted_by_barcode: bool,
}
impl GascetHeader {
    pub fn read(file: &mut impl Read) -> anyhow::Result<GascetHeader>{

        let magic = "GASCET".as_bytes();
        let mut buffer_for_magic = [0u8; 6];
        file.read_exact(&mut buffer_for_magic).expect("Failed to read gascet magic");

        if magic != buffer_for_magic {
            bail!("This does not appear to be a bascet file");
        }

        let version_major = read_u8(file);
        let version_minor = read_u8(file);
        let is_sorted_by_barcode = read_u8(file)>0;
        let _reserved_for_index = read_u64(file);  //Pointer to the index in the file

        Ok(GascetHeader {
            version_major: version_major,
            version_minor: version_minor,
            is_sorted_by_barcode: is_sorted_by_barcode,
        })
    }
}


struct GascetWriter {



    
}
impl GascetWriter {


    pub fn new() -> GascetWriter {
        GascetWriter {

        }
    }


    pub fn sort_by_barcode(reads: &mut Vec<BarcodedGascetReadPair>){
        reads.sort_by(|(bc1,_),(bc2,_)| bc1.cmp(bc2));
    }


    pub fn write(&self, _reads: &Vec<GascetReadPair>){



    }
}



struct SortedGascetReader {

    header: GascetHeader,
    pub current_cell: Option<Vec<u8>>,
    current_cell_reads: Arc<Vec<GascetReadPair>>,
    file_reader: BGZFMultiThreadReader<File>,
    got_more_cells: bool

}
impl SortedGascetReader {


    pub fn new(file_name: &PathBuf) -> anyhow::Result<SortedGascetReader> {

        let mut opened = match File::open(file_name) {
            Ok(file) => file,
            Err(_) => {
                bail!("Could not open gascet file {}", &file_name.display())
            }
        };

        //Read the header
        let header = GascetHeader::read(&mut opened).expect("Failed to read gascet header");
        debug!("Read gascet header {:?}", header);

        let file_reader: BGZFMultiThreadReader<File> = BGZFMultiThreadReader::new(opened).expect("BGZF reader open failed");

        Ok(SortedGascetReader {
            header: header,
            current_cell: None,
            current_cell_reads: Arc::new(Vec::new()),
            file_reader: file_reader,
            got_more_cells: true
        })
    }

    pub fn get_current_cell_no_move(&self) -> Option<Vec<u8>> {
        self.current_cell.clone()
    }
    
    // Return which cell we are currently on. Read another cell if needed
    pub fn get_current_cell(&mut self) -> Option<Vec<u8>> {
        
        //Try read more cells if needed
        if self.current_cell==None {
            self.try_read_cell();
        }
        return self.current_cell.clone();
    }

    pub fn try_read_cell(&mut self) {

        if self.got_more_cells {
            let cell_id = read_string(&mut self.file_reader);
            let _size_cell_bytes = read_u64(&mut self.file_reader); //Can be used to skip forward quickly
            let num_reads = read_u32(&mut self.file_reader); 

            //Check if we have reached the end yet. the end is simply "", a reserved name for a cell
            if cell_id=="".as_bytes() {
                self.got_more_cells = false;
            } else {

                self.current_cell = Some(cell_id);
                let mut current_cell_reads  = Vec::new();

                for _i in 0..num_reads {

                    let umi = read_string(&mut self.file_reader);
                    let r1 = read_string(&mut self.file_reader);
                    let r2 = read_string(&mut self.file_reader);
                    let q1 = read_string(&mut self.file_reader);
                    let q2 = read_string(&mut self.file_reader);

                    current_cell_reads.push(GascetReadPair {
                        umi: umi,
                        r1: r1,
                        r2: r2,
                        q1: q1,
                        q2: q2
                    });
                }
                self.current_cell_reads = Arc::new(current_cell_reads);

            }
        }
    }

    /// Get the reads, consider this cell handled
    pub fn consume(&mut self) -> Arc<Vec<GascetReadPair>> { 
        let current_reads = Arc::clone(&self.current_cell_reads);
        self.current_cell_reads = Arc::new(Vec::new()); 
        current_reads
    }

}


impl Ord for SortedGascetReader {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        //self.get_current_cell().cmp(&other.get_current_cell())
        other.current_cell.cmp(&self.current_cell)  //Note inverse ordering, such that binary heap ends up popping smallest cell id first
    }
}

impl PartialOrd for SortedGascetReader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { //Note inverse ordering, such that binary heap ends up popping smallest cell id first
        //Some(self.cmp(other))
        Some(other.cmp(self))
    }
}


///// This enables sorting of readers by current cell. note that the reader should be taken out of any sorted structure before going to the next cell
impl PartialEq for SortedGascetReader {
    fn eq(&self, other: &Self) -> bool {
        self.current_cell == other.current_cell
    }
}
impl Eq for SortedGascetReader { }

impl Hash for SortedGascetReader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.current_cell.hash(state);
    }
}



/* 


sorting mechanism - doing this in a struct is actually quite hard!
either "just do it" in a reader. no functions needed. 
otherwise, https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html   might help
unclear if priority queue copies the Arc or not. do not want to copy file readers

///////////////////////// Mechanism for reading sorted cells from multiple sources, merging 
struct SortedGascetReaderMerger {
//    pq: BinaryHeap<Arc<SortedGascetReader>> 
    pq: PriorityQueue<Arc<SortedGascetReader>,Vec<u8>>,
//    list_readers: Vec<SortedGascetReader>
}
impl SortedGascetReaderMerger {


    pub fn new(readers: &mut Vec<Arc<SortedGascetReader>>) -> SortedGascetReaderMerger {

        SortedGascetReaderMerger {
            pq: PriorityQueue::new()
            //BinaryHeap::new()
        }
    }

    pub fn add_reader(&mut self, r: &mut Arc<SortedGascetReader>) {
            r.try_read_cell();
            if let Some(current_cell) = r.get_current_cell_no_move()  {
 //               self.pq.push(&mut r, r.current_cell);
                self.pq.push(Arc::clone(r), current_cell);
            }
    }

    pub fn get_cell_and_reads(&mut self) -> Option<(Vec<u8>, Vec<Arc<Vec<GascetReadPair>>>)> {

        if let Some((mut first_reader,_priority)) = self.pq.pop() {
            //Figure out which cell we are concatenating now
            let cell_id = first_reader.get_current_cell_no_move().expect("could not find a cell but is expected");

            //Set up lists of lists, starting with the first reader
            let mut list_of_lists: Vec<Arc<Vec<GascetReadPair>>> = Vec::new();
            list_of_lists.push(first_reader.consume());

            //Re-add this reader to get more cells later
            self.add_reader(&mut Arc::clone(&first_reader));

            while let Some((other_reader,_priority)) = self.pq.peek() {
                let mut other_reader= Arc::clone(&other_reader);
                if other_reader.get_current_cell_no_move().expect("failed to get other cell id")==cell_id {

                    //Remove this reader and 
                    _ = self.pq.pop();
                    list_of_lists.push(other_reader.consume());

                    //Re-add this reader to get more cells later
                    self.add_reader(&mut other_reader);
                } else {
                    //we are done adding lists of reads
                    break;
                }
            }

            Some((cell_id, list_of_lists))
        } else {
            None
        }
    }

}



*/








/// use little endian instead?

fn read_u8(reader: &mut impl Read) -> u8 {
    let mut buffer = [0u8; 1];
    let _ = reader.read_exact(&mut buffer).expect("Failed to read u16");
    buffer[0]
}

fn read_u16(reader: &mut impl Read) -> u16 {
    let mut buffer = [0u8; 2];
    let _ = reader.read_exact(&mut buffer).expect("Failed to read u16");
    u16::from_be_bytes(buffer)
}

fn read_u32(reader: &mut impl Read) -> u32 {
    let mut buffer = [0u8; 4];
    let _ = reader.read_exact(&mut buffer).expect("Failed to read u32");
    u32::from_be_bytes(buffer)
}

fn read_u64(reader: &mut impl Read) -> u64 {
    let mut buffer = [0u8; 8];
    let _ = reader.read_exact(&mut buffer).expect("Failed to read u64");
    u64::from_be_bytes(buffer)
}


fn read_string(reader: &mut impl Read) -> Vec<u8> {
    let string_len = read_u32(reader);
    let string_len = string_len.to_usize().expect("Size too large");
    let mut content: Vec<u8> = Vec::with_capacity(string_len);
    let _ = reader.read_exact(&mut content.as_mut_slice()).expect("Failed to read string");
    content
}


/*****
 * 
 * all numbers are big endian. bool is u8
 * 
 * === One file
 * 
 * 6bytes: "BASCET"
 * u8: major version int
 * u8: minor version int
 * bool: is sorted
 * -- after this, data is in bgzf format --
 * [reads for one cell]
 * string: ""             /// note that "" is not a valid cell name
 * <eof>
 * 
 * 
 * === Reads for one cell
 * 
 * string: Name of cell
 * u64: length of block in bytes, not including name of cell
 * u32: number of reads
 * [reads]
 * 
 * 
 * === One read is like this:
 * 
 * string: umi
 * string: r1
 * string: r2
 * string: qual1
 * string: qual2
 * 
 * 
 * ==== one string
 * u32: length of string in bytes  /// overkill. but if we ever get a long read, it can in principle be > 65kb. thus u16 is a bit small. not worth the saving of u16
 * [u8] the string
 * 
 * 
 */

