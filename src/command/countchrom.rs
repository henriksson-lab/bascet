use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use rust_htslib::bam::record::Cigar;
use rust_htslib::bam::record::Record as BamRecord;
use rust_htslib::bam::Read;
use rust_htslib::htslib::uint;

use crate::fileformat::new_anndata::SparseMatrixAnnDataBuilder;

use super::determine_thread_counts_1;

pub const DEFAULT_PATH_TEMP: &str = "temp";

#[derive(Args)]
pub struct CountChromCMD {
    #[arg(short = 'i', value_parser)]
    /// BAM or CRAM file; sorted, indexed? unless cell_id's given, no need for sorting
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]
    /// Full path to file to store in
    pub path_out: PathBuf,

    #[arg(long = "min-matching", value_parser, default_value = "0")]
    /// Minimum M-bases to be considered
    pub min_matching: u32,

    #[arg(long = "remove-duplicates", value_parser, default_value = "false")]
    /// Remove reads for a cell if they are duplicates
    pub remove_duplicates: bool,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl CountChromCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        let num_threads_total = determine_thread_counts_1(self.num_threads_total)?;
        println!("Using threads {}", num_threads_total);

        //TODO Can check that input file is sorted via header

        CountChrom::run(&CountChrom {
            path_in: self.path_in.clone(),
            path_out: self.path_out.clone(),
            num_threads: num_threads_total,
            min_matching: self.min_matching,
            remove_duplicates: self.remove_duplicates
        })
        .unwrap();

        log::info!("CountChrom has finished succesfully");
        Ok(())
    }
}

type Cellid = Vec<u8>;

pub struct CountChrom {
    pub path_in: std::path::PathBuf,
    pub path_out: std::path::PathBuf,
    pub num_threads: usize,
    pub min_matching: u32,
    pub remove_duplicates: bool
}
impl CountChrom {
    /// Run the algorithm
    pub fn run(params: &CountChrom) -> anyhow::Result<()> {
        let mut cnt_mat = SparseMatrixAnnDataBuilder::new();

        //Read BAM/CRAM. This is a multithreaded reader already, so no need for separate threads.
        //cannot be TIRF; if we divide up reads we risk double counting
        let mut bam = rust_htslib::bam::Reader::from_path(&params.path_in)?;

        //Activate multithreaded reading
        bam.set_threads(params.num_threads).unwrap();

        //Keep track of last chromosome seen (assuming that file is sorted)
        let mut last_chr: Vec<u8> = Vec::new();

        //Map cellid -> count. Note that we do not have a list of cellid's at start; we need to harmonize this later
        let mut map_cell_count: BTreeMap<Cellid, uint> = BTreeMap::new();
        let mut map_cell_unclassified_count: BTreeMap<u32, uint> = BTreeMap::new();

        //To remove doublets, keep track of last position
        let mut map_cell_lastread: HashMap<Cellid, i64> = HashMap::new();

        let mut num_reads = 0;

        //Transfer all records
        let mut record = BamRecord::new();
        while let Some(_r) = bam.read(&mut record) {
            //let record = record.expect("Failed to parse record");
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Figure out the cell barcode. In one format, this is before the first :
            //TODO support read name as a TAG
            let read_name = record.qname();
            let mut splitter = read_name.split(|b| *b == b':');
            let cell_id = splitter
                .next()
                .expect("Could not parse cellID from read name");

            //Check if the read mapped
            let flags = record.flags();
            if flags & 0x4 == 0 {
                //Count this as a mapping read

                let header = bam.header();
                let chr = header.tid2name(record.tid() as u32);

                //Check if we now work on a new chromosome
                if chr != last_chr {
                    //Clear set of last read position
                    map_cell_lastread.clear();

                    //Store counts for this cell
                    if !map_cell_count.is_empty() {
                        //Only empty the first loop

                        if !last_chr.is_empty() {
                            //Do not store empty feature
                            let feature_index = cnt_mat.get_or_create_feature(&last_chr.to_vec());
                            cnt_mat
                                .add_cell_counts_per_cell_name(feature_index, &mut map_cell_count);
                        }

                        //println!("{:?}", map_cell_count);

                        //Clear buffers, move to the next cell
                        map_cell_count.clear();
                        last_chr = chr.to_vec();
                    }
                }


                //Remove duplicate reads
                let rpos = record.pos();
                let lastpos = map_cell_lastread.get(cell_id);
                let count_read = if let Some(lastpos) = lastpos {
                    if rpos==*lastpos && params.remove_duplicates {
                        //Skip this read
                        false
                    } else {
                        true
                    }
                } else {
                    true
                };
                    
                if count_read {
                    //Update position of last read for this cell
                    map_cell_lastread.insert(cell_id.to_vec(), rpos);

                    //Get number of matching bases
                    let mut num_matching = 0;
                    let cigar = record.cigar();
                    for cigar_part in cigar.iter() {
                        if let Cigar::Match(x) = cigar_part {
                            num_matching += x;
                        }
                    }

                    //Filter out reads that don't match well enough
                    if num_matching >= params.min_matching {
                        //Count this read as mapping
                        let values = map_cell_count.entry(cell_id.to_vec()).or_insert(0);
                        *values += 1;
                    } else {
                        //Count non-mapping reads
                        let cell_index = cnt_mat.get_or_create_cell(&cell_id);
                        *map_cell_unclassified_count.entry(cell_index).or_insert(0) += 1;
                    }
                }                

            } else {
                //Count non-mapping reads
                let cell_index = cnt_mat.get_or_create_cell(&cell_id);
                *map_cell_unclassified_count.entry(cell_index).or_insert(0) += 1;
            }

            //Keep track of where we are
            num_reads += 1;
            if num_reads % 1000000 == 0 {
                println!("Processed {} reads", num_reads);
            }
        }

        //Store counts for this cell
        //Need to check this at the end as well
        if !map_cell_count.is_empty() {
            //Only empty the first loop
            let feature_index = cnt_mat.get_or_create_feature(&last_chr.to_vec());
            cnt_mat.add_cell_counts_per_cell_name(feature_index, &mut map_cell_count);
        }

        //Store unclassified counts
        for (cell_index, counter) in map_cell_unclassified_count {
            cnt_mat.add_unclassified(cell_index, counter);
        }

        //Save count matrix
        cnt_mat.save_to_anndata(&params.path_out).unwrap();

        Ok(())
    }
}

/*
 *
 * Currently uses only 200% CPU.
 *
 * Speed optimization: need to read chunks and count in separate threads. should study how their reader is implemented
 *
 */
