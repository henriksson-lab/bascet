// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process;

use flate2::write::GzEncoder;
use flate2::Compression;
use noodles::cram::record::Record as CramRecord;
use noodles::sam::alignment::record::data::field::Tag;
use seq_io::fastq::Position;
use serde::{Deserialize, Serialize};

use super::stats;

use noodles::cram;

#[derive(Serialize, Deserialize)]
pub struct Index {
    pub header: Header,
    pub content: Droplets,
}

#[derive(Serialize, Deserialize)]
pub struct Header {
    pub magic: u32,      // should be 0xF09FA6A0
    pub is_sorted: bool, // TODO could have other fields here. creation date? babbles version?
}

// Droplet and Droplets need to be serializable for writing to the index file
// PositionDef and Adapters are needed because (i) Position has private fields and
// therefore cannot be serialized by serde, and (ii) we need to serialize a Vec<Position>
// which is not possible, hence the Adapter trick.
#[derive(Serialize, Deserialize, Debug)]
pub struct Droplets {
    // the key is a list of barcodes
    pub droplets: HashMap<String, CramDroplet>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnsortedDroplet {
    pub n_reads: u32,
    pub read_names: Vec<String>,
    pub f_pos: Vec<u64>,
    pub r_pos: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SortedDroplet {
    pub n_reads: u32,
    pub read_names: Vec<String>,
    pub f_start_pos: u64, // u64?
    pub f_end_pos: u64,
    pub r_start_pos: u64,
    pub r_end_pos: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Droplet {
    Unsorted(UnsortedDroplet),
    Sorted(SortedDroplet),
}

impl Droplet {
    pub fn get_n_reads(&self) -> u32 {
        match self {
            Droplet::Unsorted(droplet) => droplet.n_reads,
            Droplet::Sorted(droplet) => droplet.n_reads,
        }
    }
    pub fn get_reads_names(&self) -> &Vec<String> {
        match self {
            Droplet::Unsorted(droplet) => &droplet.read_names,
            Droplet::Sorted(droplet) => &droplet.read_names,
        }
    }
}

// TMP - position in the datacontainer and n_reads for the cram POC
#[derive(Serialize, Deserialize, Debug)]
pub struct CramChunk {
    pub pos: u64, // that is the position of the data container
    pub reads_start: u32,
    pub reads_end: u32,
}
// One droplet should be a vector of cram chunks
#[derive(Serialize, Deserialize, Debug)]
pub struct CramDroplet {
    pub n_reads: u32,
    pub chunks: Vec<CramChunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "seq_io::fastq::Position")]
struct PositionDef {
    #[serde(getter = "Position::line")]
    line: u64,
    #[serde(getter = "Position::byte")]
    byte: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Adapter {
    #[serde(with = "PositionDef")]
    pub pos: Position,
}
// Provide a conversion to construct the remote type.
impl From<PositionDef> for Position {
    fn from(pos: PositionDef) -> Position {
        Position::new(pos.line, pos.byte)
    }
}

// TODO --presets manually exclusive with barcode_file
pub fn index(cram: &PathBuf) {
    // initialise the index structure
    warn!("Experimental function on cram files");
    let mut index = Index {
        header: Header {
            magic: 0xF09FA6A0,
            is_sorted: true,
            // format
            // creation date
            // babbles version
            // checksum of the input files?
        },
        content: Droplets {
            droplets: HashMap::new(),
        },
    };

    // TODO
    // ASSERT THAT IT'S A SORTED CRAM
    let mut cram_reader = cram::io::reader::Builder::default()
        .build_from_path(cram)
        .unwrap();
    let _header = cram_reader.read_header().expect("bad header");

    let mut previous_barcode: String = String::new();
    let mut previous_position = cram_reader.position().unwrap();
    while let Some(container) = cram_reader
        .read_data_container()
        .expect("error reading cram")
    {
        // position is always start of a data container
        // the index should be:
        // position of the container, range of reads to take.
        let current_position = cram_reader.position().unwrap();
        trace!("Reading data container at position {}", current_position);
        let slices = container.slices();
        for slice in slices {
            let mut n_reads_iterated_over = 0;
            let rs = slice.records(container.compression_header()).unwrap();
            // group the reads in pairs
            let chunks: Vec<Vec<_>> = rs.chunks(2).map(|r| r.to_vec()).collect();
            let mut chunks_iterator = chunks.iter();
            while let Some(pair) = chunks_iterator.next() {
                // one pair is a vector of 2 reads
                validate_read_pair(pair);
                let bc_tag = pair[0].tags().get(&Tag::CELL_BARCODE_ID);
                // TODO match so we discard the reads without barcodes
                let current_barcode: String = stats::reformat_value_string(
                    stats::BCValue(bc_tag.unwrap().clone()).to_string(),
                );
                if current_barcode != previous_barcode {
                    trace!("Found a new barcode: {:?}", &current_barcode);
                    // new droplet here
                    // since the cram is sorted, update previous barcode
                    index.content.droplets.insert(
                        current_barcode.clone(),
                        CramDroplet {
                            n_reads: 1,
                            chunks: vec![CramChunk {
                                pos: current_position,
                                reads_start: n_reads_iterated_over,
                                reads_end: n_reads_iterated_over + 1,
                            }],
                        },
                    );
                    previous_barcode = current_barcode;
                    previous_position = current_position;
                } else {
                    // add to the current droplet
                    // check if position is in the same chunk
                    // if yes update read_end
                    // otherwise create a new chunk
                    if current_position == previous_position {
                        // we are still in the same container
                        // update the last chunk (are the vec ordered?)
                        // or is it better to find the chunk by the position?
                        if let Some(droplet) = index.content.droplets.get_mut(&current_barcode) {
                            trace!("Updating existing barcode: {:?}", &current_barcode);
                            droplet.n_reads += 1;
                            // find the chunk by position
                            let chunk = droplet
                                .chunks
                                .iter_mut()
                                .find(|c| c.pos == current_position)
                                .unwrap();
                            chunk.reads_end = n_reads_iterated_over + 1;
                        }
                    } else {
                        // we are in a new data container, need to create a new chunk
                        if let Some(droplet) = index.content.droplets.get_mut(&current_barcode) {
                            trace!(
                                "Updating existing barcode (new chunk): {:?}",
                                &current_barcode
                            );
                            droplet.n_reads += 1;
                            droplet.chunks.push(CramChunk {
                                pos: current_position,
                                reads_start: n_reads_iterated_over,
                                reads_end: n_reads_iterated_over + 1,
                            });
                        }
                        previous_position = current_position;
                    }
                }
                n_reads_iterated_over += 2;
            }
        }
        previous_position = current_position;
    }
    // now write the index to file
    let mut index_path = cram.clone();
    index_path.set_extension("idx");
    let index_file = match File::create(&index_path) {
        Ok(file) => file,
        Err(_) => {
            error!("Could not create index file {}", &index_path.display());
            process::exit(1)
        }
    };
    let index_vec = match bincode::serialize(&index) {
        Ok(v) => v,
        Err(_) => {
            error!("Could not serialize index structure to vector");
            process::exit(1)
        }
    };
    let mut encoder = GzEncoder::new(index_file, Compression::default());
    encoder.write_all(&index_vec).expect("Error writing index");
    info!("Written sorted index to {}", index_path.display());
}

fn validate_read_pair(pair: &Vec<CramRecord>) {
    if pair.len() != 2 {
        trace!("{:?}", pair);
        error!("Read pair does not have 2 reads. Exiting.");
        process::exit(1);
    }
    if pair[0].name() != pair[1].name() {
        trace!("{:?}", pair);
        error!("Reads in a pair do not have the same name. Exiting.");
        process::exit(1);
    }
    if !pair[0].bam_flags().is_first_segment() || !pair[1].bam_flags().is_last_segment() {
        trace!("{:?}", pair);
        error!("Read pair does not have the correct BAM flags. Exiting.");
        process::exit(1);
    }
    let fw_barcodes = pair[0].tags().get(&Tag::CELL_BARCODE_ID);
    let rv_barcodes = pair[1].tags().get(&Tag::CELL_BARCODE_ID);
    if fw_barcodes != rv_barcodes {
        trace!("{:?}", pair);
        error!("Reads in a pair do not have the same barcode. Exiting.");
        process::exit(1);
    }
}