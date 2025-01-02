// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use itertools::Itertools;
use log::{debug, error, info};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::process;

use bgzip::{write::BGZFMultiThreadWriter, write::BGZFWriter, BGZFError, Compression};

use noodles::cram;
use noodles::sam;
use noodles_bgzf as bgzf;
use noodles_bgzf::VirtualPosition;

use noodles::sam::alignment::{
    record::data::field::Tag,
    record_buf::{data::field::Value, Data},
};

use niffler::get_reader;
use seq_io::fasta::{Reader as FastaReader, Record as FastaRecord};
use seq_io::fastq::{Reader as FastqReader, Record as FastqRecord};

use bio::alignment::Alignment;
use bio::pattern_matching::myers::Myers;


#[derive(Clone, Debug)]
pub struct Barcode {
    pub index: usize,
    pub name: String,
    pub pool: u32,
    pub sequence: Vec<u8>,
    pub pattern: Myers<u64>,
}
impl Barcode {
    pub fn seek( ////////////// why mutable??
        &mut self,
//        &self,
        record: &[u8],
        distance: u8,
    ) -> Vec<(&String, u32, Vec<u8>, usize, usize, i32)> {
        // use Myers' algorithm to find the barcodes in a read
        // Ref: Myers, G. (1999). A fast bit-vector algorithm for approximate string
        // matching based on dynamic programming. Journal of the ACM (JACM) 46, 395–415.
        let mut hits: Vec<(&String, u32, Vec<u8>, usize, usize, i32)> = Vec::new();
        let mut aln = Alignment::default();
        let mut matches = self.pattern.find_all_lazy(record, distance);  //^^^^^^^^^^^^ `self` is a `&` reference, so the data it refers to cannot be borrowed as mutable
        let maybe_matches = matches.by_ref().min_set_by_key(|&(_, dist)| dist);
        if maybe_matches.len() > 0 {
            for (best_end, _) in maybe_matches {
                matches.alignment_at(best_end, &mut aln);
                hits.push((
                    &self.name,
                    self.pool,
                    self.sequence.to_owned(),
                    aln.ystart,
                    aln.yend,
                    aln.score,
                ));
            }
        }
        hits
    }
}

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

pub fn open_cram(file_handle: &PathBuf) -> cram::io::Reader<File> {
    debug!("opening cram file: {}", file_handle.display());
    let opened = match File::open(file_handle) {
        Ok(file) => file,
        Err(_) => {
            error!("Could not open forward file {}", &file_handle.display());
            process::exit(1)
        }
    };
    let file = cram::io::Reader::new(opened);
    file
}

pub fn read_barcodes(barcode_files: &Vec<PathBuf>) -> Vec<Barcode> {
    let mut barcodes: Vec<Barcode> = Vec::new();
    for barcode_file in barcode_files {
        let mut reader = open_fasta(barcode_file); // all barcodes should be in tsv files
                                                   // open barcode file
                                                   // tsv with the following columns (optional in parantheses):
                                                   // pos	(well)	seq
                                                   // let mut reader = File::open(barcode_file).unwrap();
                                                   // buffer and iterator
        let mut n_barcodes: usize = 0;
        while let Some(record) = reader.next() {
            let record = record.expect("Error reading record");
            let b = Barcode {
                index: n_barcodes,
                name: record.id().unwrap().to_string(),
                pool: 0,
                sequence: record.seq().to_vec(),
                pattern: Myers::<u64>::new(record.seq().to_vec()),
            };
            barcodes.push(b);
            n_barcodes += 1;
        }
    }
    // TODO check the edit distance between barcodes
    info!(
        "Found {} barcodes in specified barcode files",
        barcodes.iter().count()
    );
    barcodes
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

pub fn create_cram_file(name: &PathBuf) -> (sam::Header, cram::io::Writer<File>) {
    debug!("Creating CRAM file: {}", name.display());
    let header = sam::Header::builder()
        .set_header(Default::default()) // need a Map here
        // to add other header felds with Map::other_fields_mut()
        // or tag or program
        .add_comment("babbles")
        .build();

    //Delete file if it exists
    if std::fs::exists(name).expect("Cannot check if cram file already exists") {
        std::fs::remove_file(name).expect("Failing to remove target cram file");
    }

    let out_buffer = open_buffer_for_writing(name, true, false);  //fail_on_exists makes rust just end. set to false because hard to debug
    let mut writer = cram::io::Writer::new(out_buffer);

    writer
        .write_header(&header)
        .expect("Failed to write header to CRAM file");
    (header, writer)
}

pub fn write_records_pair_to_cram(
    header: &sam::Header,
    cram_writer: &mut cram::io::Writer<File>,
    forward: &impl seq_io::fastq::Record,
    reverse: &impl seq_io::fastq::Record,
//    forward: seq_io::fastq::RefRecord,
//    reverse: seq_io::fastq::RefRecord,
    barcodes_hits: &Vec<String>,
    barcodes_seq: &Vec<String>,
) {
    // create the forward record
    let fname = forward
        .id()
        .unwrap()
        .as_bytes()
        .split_last_chunk::<2>() // we want to remove the paired identifiers /1 and /2
        .unwrap();
    let mut forward_tags = Data::default();
    forward_tags.insert(Tag::CELL_BARCODE_ID, Value::from(barcodes_hits.join("-")));
    forward_tags.insert(
        Tag::CELL_BARCODE_SEQUENCE,
        Value::from(barcodes_seq.join("-")),
    );
    let forward_builder = cram::record::Builder::default()
        .set_name(fname.0)
        .set_read_length(forward.seq().len())
        .set_bases(noodles::sam::alignment::record_buf::Sequence::from(
            forward.seq(),
        ))
        .set_quality_scores(noodles::sam::alignment::record_buf::QualityScores::from(
            forward.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>(),
        ))
        .set_flags(cram::record::Flags::from(0x07))
        .set_bam_flags(noodles::sam::alignment::record::Flags::from(0x4D))
        .set_tags(forward_tags);
    let forward_record = forward_builder.build();

    //NOW THE REVERSE
    let rname = reverse
        .id()
        .unwrap()
        .as_bytes()
        .split_last_chunk::<2>() // we want to remove the paired identifiers /1 and /2
        .unwrap(); // TODO assert names are same
    let mut reverse_tags = Data::default();
    reverse_tags.insert(Tag::CELL_BARCODE_ID, Value::from(barcodes_hits.join("-")));
    reverse_tags.insert(
        Tag::CELL_BARCODE_SEQUENCE,
        Value::from(barcodes_seq.join("-")),
    );
    let reverse_builder = cram::record::Builder::default()
        .set_name(rname.0)
        .set_read_length(reverse.seq().len())
        .set_bases(noodles::sam::alignment::record_buf::Sequence::from(
            reverse.seq(),
        ))
        .set_quality_scores(noodles::sam::alignment::record_buf::QualityScores::from(
            reverse.qual().iter().map(|&n| n - 33).collect::<Vec<u8>>(),
        ))
        .set_flags(cram::record::Flags::from(0x03))
        .set_bam_flags(noodles::sam::alignment::record::Flags::from(0x8D))
        .set_tags(reverse_tags);
    let reverse_record = reverse_builder.build();

    // bam flags
    // tags to find forward and reverse: (see samtools doc)
    // >>> 64 + 1 + 4 + 8
    // 77
    // >>> 128 + 1 + 4 + 8
    // 141

    cram_writer
        .write_record(&header, forward_record)
        .expect("Failed to write read to cram");
    cram_writer
        .write_record(&header, reverse_record)
        .expect("Failed to write read to cram");
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