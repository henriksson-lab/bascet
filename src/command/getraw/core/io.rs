// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use log::{debug, error};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::process;

use bgzip::{write::BGZFMultiThreadWriter, write::BGZFWriter, BGZFError, Compression};

use noodles_bgzf as bgzf;
use noodles_bgzf::VirtualPosition;

use niffler::get_reader;
use seq_io::fasta::Reader as FastaReader;
use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};

use semver::{Version, VersionReq};

use std::process::Command;


pub struct BGZFFastqReader {
    pub reader: noodles_bgzf::Reader<File>,
}
impl Iterator for BGZFFastqReader {
    type Item = BGZFRecord;
    fn next(&mut self) -> Option<Self::Item> {
        let record = match BGZFRecord::from_reader(&mut self.reader) {
            Ok(record) => Some(record),
            Err(_) => None,
        };
        record
    }
}
impl BGZFFastqReader {
    pub fn position(&self) -> VirtualPosition {
        self.reader.virtual_position()
    }
    pub fn seek(&mut self, pos: u64) -> io::Result<VirtualPosition> {
        let pos = VirtualPosition::from(pos);
        self.reader.seek(pos)
    }
}

#[derive(Debug)]
pub struct BGZFRecord {
    head: String,
    seq: Vec<u8>,
    qual: Vec<u8>,
}
impl BGZFRecord {
    pub fn new(head: String, seq: Vec<u8>, qual: Vec<u8>) -> Self {
        BGZFRecord { head, seq, qual }
    }
    pub fn seq(&self) -> &Vec<u8> {
        &self.seq
    }
    pub fn id(&self) -> &String {
        &self.head
    }
    pub fn from_reader(reader: &mut noodles_bgzf::Reader<File>) -> io::Result<BGZFRecord> {
        let mut head = String::new();
        let mut seq = String::new();
        let mut sep = String::new();
        let mut qual = String::new();
        reader.read_line(&mut head)?;
        reader.read_line(&mut seq)?;
        reader.read_line(&mut sep)?;
        reader.read_line(&mut qual)?;
        let record = BGZFRecord {
            head,
            seq: seq.as_bytes().to_vec(),
            qual: qual.as_bytes().to_vec(),
        };
        // reading from cursor won't fail if the stream reached EOF
        // so we need to check if the record is empty
        if record.seq.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Reached end of file",
            ));
        }
        Ok(record)
    }
    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), BGZFError> {
        writer.write_all(self.head.as_bytes())?;
        writer.write_all(&self.seq)?;
        writer.write_all(b"+\n")?;
        writer.write_all(&self.qual)?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////


// "only traits defined in the current crate can be implemented for types
// defined outside of the crate. Define and implement a trait or new type instead"
// If we'd ever want to have all fastx IO with niffler + seqio ...
// impl std::io::Seek for Box<(dyn std::io::Read + 'static)> {
//    println!("unimplemented!");
// }

pub fn open_fastq(file_handle: &PathBuf) -> FastqReader<Box<dyn std::io::Read>> {
    let opened_handle = match File::open(file_handle) {
        Ok(file) => file,
        Err(_) => {
            error!("Could not open file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let (reader, _) = match get_reader(Box::new(opened_handle)) {
        Ok((reader, compression)) => {
            debug!(
                "Opened file {} with compression {:?}",
                &file_handle.display(),
                &compression
            );
            (reader, compression)
        }
        Err(_) => {
            error!("Could read reverse file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let fastq = FastqReader::new(reader);
    fastq
}

pub fn open_fasta(file_handle: &PathBuf) -> FastaReader<Box<dyn std::io::Read>> {
    let opened_handle = match File::open(file_handle) {
        Ok(file) => file,
        Err(_) => {
            error!("Could not open file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let (reader, _) = match get_reader(Box::new(opened_handle)) {
        Ok((reader, compression)) => {
            debug!(
                "Opened file {} with compression {:?}",
                &file_handle.display(),
                &compression
            );
            (reader, compression)
        }
        Err(_) => {
            error!("Could read reverse file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let fasta = FastaReader::new(reader);
    fasta
}

pub fn open_fastq_bgzipped(file_handle: &PathBuf) -> BGZFFastqReader {
    let opened = match File::open(file_handle) {
        Ok(file) => file,
        Err(_) => {
            error!("Could not open forward file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let file = bgzf::Reader::new(opened);
    BGZFFastqReader { reader: file }
}



pub fn open_buffer_for_writing(path: &PathBuf, append: bool, fail_on_exists: bool) -> File {
    let buffer = OpenOptions::new()
        .write(true)
        .append(append)
        .create(true)
        .create_new(fail_on_exists)
        .open(&path);
    let buffer = match buffer {
        Ok(buffer) => buffer,
        Err(error) => {
            debug!("{:?}", error);
            error!("Could not create output file {}", &path.display());
            process::exit(1)
        }
    };
    buffer
}

pub fn open_bgzf_for_writing(path: &PathBuf, append: bool) -> BGZFWriter<File> {
    let buffer = OpenOptions::new()
        .write(true)
        .append(append)
        .create(true)
        .open(&path);
    let opened_file = match buffer {
        Ok(buffer) => buffer,
        Err(error) => {
            debug!("{:?}", error);
            error!("Could not create output file {}", &path.display());
            process::exit(1)
        }
    };
    let writer = BGZFWriter::new(opened_file, Compression::default());
    writer
}

pub fn fastq_to_bgz(path: &PathBuf, output: &PathBuf) {
    let mut fastq = open_fastq(path);
    let out_buffer = open_buffer_for_writing(output, false, false);
    debug!("Converting {} to Blocked Gzip", &path.display());

    let mut writer = BGZFMultiThreadWriter::new(out_buffer, Compression::default());
    while let Some(record) = fastq.next() {
        let record = record.expect("Error reading record");
        record.write(&mut writer).unwrap();
    }
    writer.close().unwrap();
}






pub fn check_dep_samtools() {
    debug!("Checking for the correct samtools");
    let req_samtools_version = VersionReq::parse(">=1.18").unwrap();
    let samtools = Command::new("samtools").arg("version").output();
    match samtools {
        Ok(samtools) => {
            let samtools_version = String::from_utf8_lossy(
                samtools
                    .stdout
                    .split(|c| *c == b'\n')
                    .next()
                    .unwrap()
                    .split(|c| *c == b' ')
                    .last()
                    .unwrap(),
            );
            let samtools_version = samtools_version.parse::<Version>().unwrap();
            if req_samtools_version.matches(&samtools_version) {
                debug!("Samtools version is recent enough");
            } else {
                error!("babbles extract requires Samtools >= 1.18");
                process::exit(1)
            }
        }
        Err(_error) => {
            error!("Samtools is either not installed or not in PATH");
            process::exit(1)
        }
    };
}







#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_buffer_for_writing() {
        let path = PathBuf::from("tests/data/test.txt");

        let maybe_buffer = open_buffer_for_writing(&path, false, false);
        assert_eq!(maybe_buffer.metadata().unwrap().is_file(), true);

        // cleanup
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_read_barcodes() {
        // read_barcodes() calls open_fasta() which is therefore not tested separately
        let path = PathBuf::from("tests/data/barcodes.fasta");
        let paths = Vec::from([path]);
        let maybe_barcodes = read_barcodes(&paths);

        assert_eq!(maybe_barcodes.len(), 2);
        assert_eq!(maybe_barcodes[0].name, "A_0");
        assert_eq!(maybe_barcodes[1].sequence, b"TTGAGCCG".to_vec());
    }

    #[test]
    fn test_open_fastq_and_seek() {
        use seq_io::fastq::Record;
        let path = PathBuf::from("tests/data/reads.fastq");
        let mut maybe_reader = open_fastq(&path);
        let maybe_id = maybe_reader.next().unwrap().unwrap().to_owned_record();
        assert_eq!(maybe_id.id().unwrap(), "read_1");
    }

    #[test]
    fn test_seek() {
        let sequence = b"CTGCTTGAGCCGAGGGGATTATCTCGTAAGGCAAGCTCGT";

        let mut barcode = Barcode {
            index: 0,
            name: "test".to_string(),
            pool: 1,
            sequence: b"TTGAGCCG".to_vec(),
            pattern: Myers::<u64>::new(b"TTGAGCCG".to_vec()),
        };
        let hits = barcode.seek(sequence, 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].3, 4); // start
    }
}











use rust_htslib;
use rust_htslib::bam;
use std::path::Path;
//use std::path::PathBuf;
use anyhow;
use anyhow::bail;



//detect file format from file extension
pub fn detect_bam_file_format(fname: &Path) -> anyhow::Result<bam::Format> {

    let fext = fname.extension().expect("Output file lacks file extension");
    match fext.to_str().expect("Failing string conversion") {
        "sam" => Ok(bam::Format::Sam),
        "bam" => Ok(bam::Format::Bam),
        "cram" => Ok(bam::Format::Cram),
        _ => bail!("Cannot detect BAM/CRAM/SAM type from file extension")
    }
}

/* 

use rust_htslib::bam::record::Aux;

pub fn create_new_bam(
    fname: &Path
// num_threads
// compression level
) -> anyhow::Result<bam::Writer> {

    let file_format = detect_bam_file_format(fname)?;

    let mut header = bam::Header::new();
    header.push_comment("Debarcoded by Bascet".as_bytes());

    let mut writer = bam::Writer::from_path(fname, &header, file_format).unwrap();

    _ = writer.set_threads(5);  //  need we also give a pool? https://docs.rs/rust-htslib/latest/rust_htslib/bam/struct.Writer.html#method.set_threads
    _ = writer.set_compression_level(bam::CompressionLevel::Fastest);  //or no compression, do later ; for user to specify

    Ok(writer)
}

*/



/* 


pub fn write_records_pair_to_bamlike(
    writer: &mut bam::Writer,
    forward: &impl seq_io::fastq::Record,
    reverse: &impl seq_io::fastq::Record,
    barcodes_hits: &Vec<String>
) {
    // create the forward record
    let fname = forward
        .id()
        .unwrap()
        .as_bytes()
        .split_last_chunk::<2>() // we want to remove the paired identifiers /1 and /2
        .unwrap().0;

    let cell_barcode = barcodes_hits.join("_");  //Note: : and - are not allowed in cell IDs. this because of the possible use of tabix

    ////// Forward record
    let mut record = bam::Record::new();
    let qual = forward.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>();
    record.set(
        fname,
        None,
        forward.seq(),
        qual.as_slice()  //forward.qual()
    );
    _ = record.push_aux("CB".as_bytes(), Aux::String(cell_barcode.as_str()));
    record.set_flags(0x4D); // 0x4D  read paired, read unmapped, mate unmapped, first in pair
    //.set_flags(cram::record::Flags::from(0x07))   what is this?
    writer.write(&record).expect("Failed to write forward read");
    
    ////// Reverse record
    let mut record = bam::Record::new();
    let qual = reverse.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>();
    record.set(
        fname,
        None,
        reverse.seq(),
        qual.as_slice() //reverse.qual()
    );
    _ = record.push_aux("CB".as_bytes(), Aux::String(cell_barcode.as_str()));
    record.set_flags(0x8D); // 0x8D  read paired, read unmapped, mate unmapped, second in pair
    //.set_flags(cram::record::Flags::from(0x03))  hm?
    writer.write(&record).expect("Failed to write reverse read");

}
*/


/* 

/////////////////////////////////// Writer to tagged BAM file
fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool
) -> anyhow::Result<Arc<Sender<Option<ListReadWithBarcode>>>> {

    let outfile = outfile.clone();

    //Limit how many chunks can be in pipe
    let (tx, rx) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(100);  
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));

    thread_pool.execute(move || {
        // Open cram output file
        println!("Creating output file: {}",outfile.display());
        let mut writer = create_new_bam(&outfile).expect("failed to create bam-like file");

        // Write reads
        let mut n_written=0;
        while let Ok(Some(list_pairs)) = rx.recv() {
            for (bam_cell, hits_names) in list_pairs.iter() {
                let reverse_record=&bam_cell.reverse_record;
                let forward_record=&bam_cell.forward_record;

                write_records_pair_to_bamlike(
                    &mut writer,
                    forward_record,
                    reverse_record,
                    &hits_names
                );

                if n_written%100000 == 0 {
                    println!("written to {:?} -- {:?}",outfile, n_written);
                }
                n_written = n_written + 1;
            }

            
        }
    });
    Ok(tx)
}

*/
