// This file is part of babbles which is released under the MIT license.
// See file LICENSE or go to https://github.com/HadrienG/babbles for full license details.
use log::{error, info, trace};
use std::collections::HashMap;
use std::path::PathBuf;

use csv::Writer;

use noodles::cram;
use noodles::sam::alignment::record::data::field::Tag;
use noodles::sam::alignment::record_buf::data::field::Value;

#[derive(Debug)]
pub struct BCValue(pub Value);

impl std::fmt::Display for BCValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

// TODO the droplet structure should contain statistics about the reads in the droplet
#[derive(Debug)]
struct Droplet {
    // barcodes: Vec<String>,  // unnecessary, we can get this from the hashmap
    // linker: String,  // not used, TODO remove?
    n_reads: u32,
}

pub fn stats(cram_file: &PathBuf, output: &PathBuf) {
    info!("Iterating over reads and compiling statistics");
    // cound n_reads per cell
    // do we need the presets for the stats?

    // general stats
    let mut n_reads_pairs = 0;

    // droplet struct
    // iterate over the cram and add to the droplets hashmap
    let mut droplets: HashMap<String, Droplet> = HashMap::new();

    // // Read the barcodes from the barcode file
    // let mut barcodes = io::read_barcodes(barcode_files);

    // open cram for reading
    let mut reader = cram::io::reader::Builder::default()
        .build_from_path(cram_file)
        .unwrap();
    let header = reader.read_header().expect("bad header");

    // fill the droplets struct
    let mut records = reader.records(&header);
    while let Some(record) = records.next() {
        let forward = record.expect("Error reading record");
        let reverse = records.next().unwrap().expect("Error reading record");

        let fw_barcodes = forward.tags().get(&Tag::CELL_BARCODE_ID);
        let rv_barcodes = reverse.tags().get(&Tag::CELL_BARCODE_ID);
        n_reads_pairs += 1;
        assert_eq!(fw_barcodes, rv_barcodes);
        match rv_barcodes {
            Some(barcodes) => {
                let bc: String = reformat_value_string(BCValue(barcodes.clone()).to_string());
                if droplets.contains_key(&bc) {
                    droplets.get_mut(&bc).unwrap().n_reads += 1;
                } else {
                    droplets.insert(bc.clone(), Droplet { n_reads: 1 });
                }
            }
            _ => {
                trace!("Found read in cram with no barcodes. Skipping.");
            }
        }
    }

    let mut droplets_vec: Vec<(&String, &Droplet)> = droplets.iter().collect();
    droplets_vec.sort_by(|a, b| b.1.n_reads.cmp(&a.1.n_reads));
    // trace!("Dump of droplets hashmap: {:?}", droplets_vec);

    // // write a table with the summary statistics
    // // create output file
    let mut output_file = output.clone();
    output_file.set_extension("counts.txt");
    info!(
        "Iterated through {} read pairs. Writing results to {}",
        n_reads_pairs,
        &output_file.display()
    );
    let buffer = Writer::from_path(&output_file);
    let mut buffer = match buffer {
        Ok(buffer) => buffer,
        Err(_error) => {
            error!("Could not create output file {}", &output_file.display());
            std::process::exit(1)
        }
    };
    buffer
        .write_record(&["barcodes", "n_reads"])
        .expect("Error writing to file");
    for (barcodes, droplet) in droplets.iter() {
        buffer
            .write_record(&[&barcodes, &droplet.n_reads.to_string()])
            .expect("Error writing to file");
    }
    info!("Done!");
    // TODO write a table with general quality statistics
    // q30%, %valid barcodes, quality in barcodes, ...
    // with the index and a kraken.tsv (or other software) we should be able to get abundance stats
}

pub fn reformat_value_string(s: String) -> String {
    // "String(\"E3-D7-H10-A5\")" -> "E3-D7-H10-A5"
    let s = s
        .trim_start_matches("String")
        .trim_matches(|c| c == '"' || c == '(' || c == ')' || c == '\\');
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reformat_value_string() {
        let s = "String(\"E3-D7-H10-A5\")".to_string();
        assert_eq!(reformat_value_string(s), "E3-D7-H10-A5".to_string());
    }
}
