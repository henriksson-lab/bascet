use anyhow::{Context, Result};
use clap::Args;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use noodles::sam::alignment::RecordBuf as BamRecord;
use noodles::{bgzf, csi::binning_index::index::reference_sequence::bin::Chunk};
use tracing::info;

use super::determine_thread_counts_1;
use crate::utils::{BedTabixIndexer, atomic_temp_path, publish_atomic_output};

pub const DEFAULT_PATH_TEMP: &str = "temp";
pub const DEFAULT_THREADS: usize = 5;
type NoodlesBamReader = noodles::bam::io::Reader<noodles::bgzf::io::MultithreadedReader<File>>;

#[derive(Args)]
pub struct Bam2FragmentsCMD {
    #[arg(short = 'i', value_parser)]
    /// BAM file; sorted, indexed? unless cell_id's given, no need for sorting
    pub path_in: PathBuf,

    #[arg(short = 'o', value_parser)]
    /// Full path to file to store in
    pub path_out: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    //Not used, but kept here for consistency with other commands
    pub path_tmp: PathBuf,

    //Thread settings
    #[arg(short = '@', value_parser = clap::value_parser!(usize))]
    num_threads_total: Option<usize>,
}
impl Bam2FragmentsCMD {
    /// Run the commandline option
    pub fn try_execute(&mut self) -> Result<()> {
        let num_threads_total = determine_thread_counts_1(self.num_threads_total)?;
        info!("Using threads {}", num_threads_total);

        //TODO Can check that input file is sorted via header

        Bam2Fragments::run(&Bam2Fragments {
            path_input: self.path_in.clone(),
            path_tmp: self.path_tmp.clone(),
            path_output: self.path_out.clone(),
            num_threads: num_threads_total,
        })
        .unwrap();

        info!("Bam2Fragments has finished succesfully");
        Ok(())
    }
}

/**
 *
 * as input, take total count matrix, pick features that are within a certain percentile. randomize and subset these further to get a good list!
 *
 *
 */

pub struct Bam2Fragments {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub num_threads: usize,
}
impl Bam2Fragments {
    /// Run the algorithm
    pub fn run(params: &Bam2Fragments) -> anyhow::Result<()> {
        //Read BAM. This is a multithreaded reader already, so no need for separate threads.
        let (mut bam, header) = open_bam_reader(&params.path_input, params.num_threads)?;
        let ref_names: Vec<Vec<u8>> = header
            .reference_sequences()
            .iter()
            .map(|(name, _)| {
                let name: &[u8] = name.as_ref();
                name.to_vec()
            })
            .collect();

        //Save a "Fragments.tsv", bgzip-format and build the tabix index from
        //the exact BGZF virtual offsets as each BED record is written.
        let path_tmp = atomic_temp_path(&params.path_output);
        let outfile = File::create(&path_tmp)
            .with_context(|| format!("could not open output file {}", path_tmp.display()))?;
        let mut writer = bgzf::io::Writer::new(outfile);
        let mut indexer = BedTabixIndexer::new();
        writer.write_all(b"#CHR\tFROM\tTO\tCELLID\tCNT\tUMI\n")?; // UMI is optional; what works with Signac?

        //Transfer all records
        let mut record = BamRecord::default();
        while bam.read_record_buf(&header, &mut record)? > 0 {
            // https://samtools.github.io/hts-specs/SAMv1.pdf

            //Only keep mapping reads
            let flags = record.flags();
            if !flags.is_unmapped() {
                /*
                println!("{:?} ",record);
                println!("{:?} ",record.pos());
                println!("{:?} ",record.mpos());
                */

                //Figure out the cell barcode. In one format, this is before the first :
                //TODO support read name as a TAG
                let read_name: &[u8] = record.name().expect("missing read name").as_ref();
                let mut splitter = read_name.split(|b| *b == b':');
                let cell_id = splitter
                    .next()
                    .expect("Could not parse cellID from read name");

                let tid = record
                    .reference_sequence_id()
                    .expect("mapped read missing reference sequence id");
                let chr = &ref_names[tid];

                //Get left-most mapping position
                let startpos = record
                    .alignment_start()
                    .map(|pos| pos.get() - 1)
                    .unwrap_or(0);

                //mpos();
                //From samtools specification: "1-based leftmost mapping POSition of the first CIGAR operation that “consumes” a reference base". ==> This is any of MDN=I
                //If POS is 0, no assumptions can be made about RNAME and CIGAR"

                //Figure the end-position from the CIGAR
                let endpos = record
                    .alignment_end()
                    .map(|pos| pos.get())
                    .unwrap_or(startpos);

                //TODO: future option is to split read by S* to handle splicing.
                //Note that resorting is then needed. but the local nature suggests that a priority queue can be used along with other tricks

                //Write the BED record
                let record_start_position = writer.virtual_position();
                writer.write_all(chr)?;
                write!(&mut writer, "\t{}\t{}\t", startpos, endpos)?;
                writer.write_all(cell_id)?;
                write!(&mut writer, "\t1\t\n")?; //Leaving space for a future UMI here
                let record_end_position = writer.virtual_position();

                let chr = std::str::from_utf8(chr).with_context(|| {
                    format!(
                        "reference sequence name for tid {tid} is not UTF-8 and cannot be tabix-indexed"
                    )
                })?;
                indexer.add_record(
                    chr,
                    startpos,
                    endpos,
                    Chunk::new(record_start_position, record_end_position),
                )?;
            }
        }
        writer.finish()?;
        //Tabix-index the output file to prepare it for loading
        info!("Indexing final output file");
        indexer
            .write_to_path(tabix_index_path(&path_tmp))
            .with_context(|| format!("failed to write tabix index for {}", path_tmp.display()))?;
        publish_atomic_output(&path_tmp, &params.path_output)?;
        publish_atomic_output(
            tabix_index_path(&path_tmp),
            tabix_index_path(&params.path_output),
        )?;

        Ok(())
    }
}

fn open_bam_reader(
    path: &PathBuf,
    num_threads: usize,
) -> anyhow::Result<(NoodlesBamReader, noodles::sam::Header)> {
    let file = File::open(path)?;
    let worker_count = NonZeroUsize::new(num_threads.max(1)).unwrap_or(NonZeroUsize::MIN);
    let bgzf_reader = noodles::bgzf::io::MultithreadedReader::with_worker_count(worker_count, file);
    let mut reader = noodles::bam::io::Reader::from(bgzf_reader);
    let header = reader.read_header()?;
    Ok((reader, header))
}

fn tabix_index_path(p: &PathBuf) -> PathBuf {
    PathBuf::from(format!("{}.tbi", p.display()))
}
