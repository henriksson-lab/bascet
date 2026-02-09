use crate::{
    bounded_parser
};

use bascet_core::{
    *,
};
use bascet_derive::Budget;

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use std::{
    path::{Path, PathBuf}
};
use tracing::{info, warn};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;

pub const DEFAULT_PATH_TEMP: &str = "temp";

use crate::fileformat::new_anndata::SparseMatrixAnnDataBuilder;


#[derive(Args)]
pub struct KrakenCMD {
    #[arg(
        short = 'i',
        long = "in",
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(
        long = "out-raw",
        help = "Raw KRAKEN2 output file"
    )]
    pub path_out_raw: PathBuf,

    #[arg(
        long = "out-matrix",
        help = "Output count matrix" 
    )]
    pub path_out_matrix: PathBuf,
    
    #[arg(
        long = "temp",
        help = "Temp directory; must exist already"
    )]
    pub path_temp: PathBuf,

    #[arg(
        short = 'd',
        long = "db", 
        help = "KRAKEN2 index to use"
    )]
    pub path_db: PathBuf,

    #[arg(
        short = '@',
        long = "threads",
        help = "Total threads to use (defaults to std::threads::available parallelism)",
        value_name = "2..",
        value_parser = bounded_parser!(BoundedU64<2, { u64::MAX }>),
    )]
    total_threads: Option<BoundedU64<2, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-read",
        help = "Number of reader threads",
        value_name = "1.. (default is 1)", // 50% of total threads
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(1),
        value_parser = clap::value_parser!(ByteSize),
    )]
    total_mem: ByteSize,

    #[arg(
        long = "sizeof-stream-buffer",
        help = "Total stream buffer size.",
        value_name = "100%",
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_buffer: Option<ByteSize>,

    #[arg(
        long = "sizeof-stream-arena",
        help = "Stream arena buffer size [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_stream_arena: ByteSize,

}

#[derive(Budget, Debug)]
struct KrakenBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 ) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,
    
    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl KrakenCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        //Validate that a KRAKEN2 db has been given
        if self.path_db.is_dir() {
            let file_taxo = self.path_db.join("taxo.k2d");
            if !file_taxo.is_file() {
                anyhow::bail!("Specified database path is not a KRAKEN2 database (directory misses files, e.g., taxo.k2d)");
            }
        } else {
            anyhow::bail!("Specified database path is not a KRAKEN2 database (not a directory)");
        }

        let budget = KrakenBudget::builder()
            .threads(self.total_threads.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to determine available parallelism, using 2 threads");
                        2
                    })
                    .try_into()
                    .unwrap_or_else(|e| {
                        warn!(error = %e, "Failed to convert parallelism to valid thread count, using 2 threads");
                        2.try_into().unwrap()
                    })
            }))
            .memory(self.total_mem)
            .maybe_numof_threads_read(self.numof_threads_read)
            //.maybe_numof_threads_writebam(self.numof_threads_writebam)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();

        info!(
            using = %budget,
            input_path = ?self.path_in,
            path_out_raw = ?self.path_out_raw,
            "Starting KRAKEN2"
        );


        /////////////////////////////////////////////////////////////////////////////////////   
        // Set up named pipes
        let path_pipe_r12 = self.path_temp.join("fifo_r12.fq");
        nix::unistd::mkfifo(&path_pipe_r12, nix::sys::stat::Mode::S_IRWXU).expect("Failed to create pipe"); /////////////////////// TODO put all of this + cleanup in a class

        ///////////////////////////////////////////////////////////////////////////////////// 
        // Start KRAKEN2
        let num_threads = budget.threads.get();
        let mut proc_aligner = create_kraken_process(
                &self.path_db, 
                &path_pipe_r12,
                &self.path_out_raw,
                num_threads
        ).expect("Failed to start KRAKEN");        

        ///////////////////////////////////////////////////////////////////////////////////// 
        // All threads are now set up. Send all readpairs to KRAKEN2.
        // Note that KRAKEN2 requires interleaved reads as paired-end mode reads one file at a file, blocking the pipe!
        super::AlignCMD::write_tirp_to_interleaved_fq(
            self.path_in.path().path(),
            &path_pipe_r12,
            //&path_pipe_r2,
            budget.numof_threads_read,
            self.sizeof_stream_arena,
            budget.sizeof_stream_buffer,
        ).expect("Failed to create pipe writer");

        //Wait until process done
        info!("Waiting for KRAKEN2 process to finish");
        proc_aligner.wait().unwrap(); ////////////////////////// TODO: should watch this process for abnormal exit, possibly panic. need to do in parallel to write_tirp_to_fq

        //Clean up: remove pipes
        std::fs::remove_file(path_pipe_r12)?;

        //Generate matrix
        info!("Generating KRAKEN2 matrix");

        let params = KrakenMatrix {
            path_tmp: self.path_temp.clone(),
            path_input: self.path_out_raw.clone(),
            path_output: self.path_out_matrix.clone(),
        };
        KrakenMatrix::run(&Arc::new(params))?;

        info!(
            "All KRAKEN2 steps complete"
        );

        //Move temp files to their right positions

        Ok(())
    }
}









///
/// Generate KRAKEN2 command
/// 
pub fn create_kraken_process<P> (
    path_db: &P,
    path_r12: &P, 
    path_out_raw: &P,
    num_threads: u64,
) -> Result<std::process::Child> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);
    let path_db = format!("{}",path_db.as_ref().as_os_str().to_str().expect("os str"));
    let path_r12 = format!("{}",path_r12.as_ref().as_os_str().to_str().expect("os str"));
    let path_out_raw = format!("{}",path_out_raw.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "--db", &path_db,
        "--threads", &num_threads,
        "--output",&path_out_raw,
        "--interleaved",
        &path_r12, 
    ];

    let proc_cmd = std::process::Command::new("kraken2")
        .args(args)
        .spawn()?;
    Ok(proc_cmd)
}






#[derive(Args)]
pub struct KrakenMatrixCMD {
    // Input bascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
}
impl KrakenMatrixCMD {
    /// Run the commandline option.
    /// This one takes a KRAKEN output-file, and outputs a taxonomy count matrix
    pub fn try_execute(&mut self) -> Result<()> {
        let params = KrakenMatrix {
            path_tmp: self.path_tmp.clone(),
            path_input: self.path_in.clone(),
            path_output: self.path_out.clone(),
        };

        let _ = KrakenMatrix::run(&Arc::new(params));

        log::info!("Kraken has finished succesfully");
        Ok(())
    }
}

///
/// KRAKEN count matrix constructor.
/// 
pub struct KrakenMatrix {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
}
impl KrakenMatrix {
    /// Run the algorithm
    pub fn run(params: &Arc<KrakenMatrix>) -> anyhow::Result<()> {
        //Prepare matrix that we will store into
        let mut mm = SparseMatrixAnnDataBuilder::new();

        //Open input file
        let file_in = File::open(&params.path_input).unwrap();
        let bufreader = BufReader::new(&file_in);

        //Counter for how many times each taxid has been seen for one cell
        let mut taxid_counter = BTreeMap::new();
        //let mut map_unclassified_counter= BTreeMap::new();
        let mut unclassified_counter = 0;

        //Loop through all reads; group by cell
        let mut last_cellid = None;
        for (_index, rline) in bufreader.lines().enumerate() {
            //////////// should be a plain list of features
            if let Ok(line) = rline {
                ////// when is this false??

                //Divide the row
                let mut splitter_line = line.split("\t");
                let is_categorized = splitter_line.next().unwrap();

                //Figure out what cell this is
                let readname = splitter_line.next().unwrap();
                let mut splitter_cellid = readname.split(":");
                let cellid = Some(splitter_cellid.next().unwrap().to_string());

                //If this is a new cell, then store everything we have so far in the count matrix
                if last_cellid != cellid {
                    //Store if there is a previous cell. Could skip this "if", if we read first line before starting. TODO
                    if let Some(last_cellid_s) = last_cellid {
                        let cell_index = mm.get_or_create_cell(last_cellid_s.as_bytes());

                        //Add taxid counts for last cell
                        mm.add_cell_counts_per_feature_index(cell_index, &mut taxid_counter);
                        //mm.add_feature_counts(cell_index, &mut taxid_counter);
                        //map_unclassified_counter.insert(last_cellid_s.clone(), unclassified_counter);
                        mm.add_unclassified(cell_index, unclassified_counter);

                        //Reset counters
                        taxid_counter.clear();
                        unclassified_counter = 0;
                    }
                    //Move to track the next cell
                    last_cellid = cellid;
                }

                if is_categorized == "C" {
                    //Classified read
                    let taxid_s = splitter_line.next().unwrap();
                    let taxid: u32 = taxid_s
                        .parse()
                        .expect(format! {"Failed to parse taxon id: -{}-", line}.as_str());

                    //Count this taxon id. Note, we count to taxonomyID+1 as 0 is also in use (top level)
                    let values = taxid_counter.entry(taxid + 1).or_insert(0);
                    *values += 1;
                } else if is_categorized == "U" {
                    //Unclassified read. Keep track of how many
                    unclassified_counter += 1;
                }
            } else {
                anyhow::bail!("Failed to read one line of input");
            }
        }

        //Need to also add counts for the last cell
        if let Some(last_cellid_s) = last_cellid {
            let cell_index = mm.get_or_create_cell(last_cellid_s.as_bytes());
            mm.add_cell_counts_per_feature_index(cell_index, &mut taxid_counter);

            mm.add_unclassified(cell_index, unclassified_counter);
        }

        //        C       BASCET_D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129

        //Compress KRAKEN taxonomy to generate normal column names etc; this makes the output more compatible
        //with regular count matrices
        mm.compress_feature_column("taxid_")?;

        //Save the final count matrix
        println!("Storing count table to {}", params.path_output.display());
        mm.save_to_anndata(&params.path_output)
            .expect("Failed to save to HDF5 file");

        Ok(())
    }
}

/*

 Note: column 1 = taxid 0
 rust sprs counts from 0

*/

/*

Example data

C       BASCET_D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129
C       BASCET_D2_F5_H7_C10::902        28384   257     0:56 1:11 0:14 28384:9 0:4 A:129
C       BASCET_D2_F5_H7_C10::902        1783272 257     0:11 2:3 1:26 2:10 1783272:6 0:16 9606:3 0:19 A:129
C       BASCET_D2_F5_H7_C10::903        2026187 257     0:29 2026187:8 86661:30 2026187:23 86661:4 A:129
C       BASCET_D2_F5_H7_C10::903        2026187 257     86661:33 2026187:4 86661:5 2026187:23 86661:29 A:129
C       BASCET_D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       BASCET_D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       BASCET_D2_F5_H7_C10::905        1386    257     1386:75 0:19 A:129
C       BASCET_D2_F5_H7_C10::905        1386    257     0:3 1386:76 0:15 A:129

https://software.cqls.oregonstate.edu/updates/docs/kraken2/MANUAL.html#standard-kraken-output-format

1. "C"/"U": a one letter code indicating that the sequence was either classified or unclassified.
2. The sequence ID, obtained from the FASTA/FASTQ header.
3. The taxonomy ID Kraken 2 used to label the sequence; this is 0 if the sequence is unclassified.
4. The length of the sequence in bp. In the case of paired read data, this will be a string containing the lengths of the two sequences in bp, separated by a pipe character, e.g. "98|94".
5. A space-delimited list indicating the LCA mapping of each k-mer in the sequence(s). For example, "562:13 561:4 A:31 0:1 562:3" would indicate that:

the first 13 k-mers mapped to taxonomy ID #562
the next 4 k-mers mapped to taxonomy ID #561
the next 31 k-mers contained an ambiguous nucleotide
the next k-mer was not in the database
the last 3 k-mers mapped to taxonomy ID #562

Note that paired read data will contain a "|:|" token in this list to indicate the end of one read and the beginning of another.

*/


