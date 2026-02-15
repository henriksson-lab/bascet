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
    io::{BufRead, BufReader}, path::{Path, PathBuf}, process::Stdio
};
use tracing::{info, warn};

pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct KmcReadsCMD {
    #[arg(
        short = 'i',
        long = "in",
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output file without suffix"
    )]
    pub path_out: PathBuf,
    
    #[arg(
        long = "ci",
        help = "KMC -ci"
    )]
    pub kmc_ci: Option<u64>,
    
    #[arg(
        long = "temp",
        help = "Temp directory; must exist already"
    )]
    pub path_temp: PathBuf,

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
        value_name = "1.. (default is 20% of threads)", // 50% of total threads
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-kmc",
        help = "Number of KMC threads",
        value_name = "1.. (default is 80% of threads)", 
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_kmc: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        short = 'm',
        long = "memory",
        help = "Total memory budget",
        default_value_t = ByteSize::gib(8),
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

    #[arg(
        long = "sizeof-kmc-mem",
        help = "... [Advanced: changing this will impact performance and stability]",
        hide_short_help = true,
        //default_value_t = DEFAULT_SIZEOF_ARENA,
        value_parser = clap::value_parser!(ByteSize),
    )]
    sizeof_kmc_mem: Option<ByteSize>,

}

#[derive(Budget, Debug)]
struct KmcBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.2) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[threads(TKmc, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 * 0.8) as u64).unwrap())] 
    numof_threads_kmc: BoundedU64<1, { u64::MAX }>,
    
    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.1) as u64))] /// TODO some minimum size. need 1gb at least
    sizeof_stream_buffer: ByteSize,

    #[mem(MKmc, |_, total_mem| bytesize::ByteSize((total_mem as f64 * 0.9) as u64))]
    sizeof_kmc_mem: ByteSize,
}

impl KmcReadsCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        let budget = KmcBudget::builder()
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
            .maybe_numof_threads_kmc(self.numof_threads_kmc)
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .maybe_sizeof_kmc_mem(self.sizeof_kmc_mem)
            .build();

        budget.validate();

        info!(
            using = %budget,
            input_path = ?self.path_in,
            path_out = ?self.path_out,
            "Starting kmer counter"
        );







        info!("wtf2????");

        /////////////////////////////////////////////////////////////////////////////////////   
        // Set up named pipes
        let path_pipe_r12 = self.path_temp.join("fifo_r12.fq");
        nix::unistd::mkfifo(&path_pipe_r12, nix::sys::stat::Mode::S_IRWXU).expect("Failed to create pipe"); /////////////////////// TODO put all of this + cleanup in a class

        ///////////////////////////////////////////////////////////////////////////////////// 
        // Start KMC
        let mut proc_aligner = create_kmc_process(
                &path_pipe_r12,
                &self.path_out,
                &self.path_temp,
                &budget,
                &self
        ).expect("Failed to start kmer counter");        
        info!("wtf3????");

        ///////////////////////////////////////////////////////////////////////////////////// 
        // All threads are now set up. Send all readpairs to KRAKEN2.
        // Note that KRAKEN2 requires interleaved reads as paired-end mode reads one file at a file, blocking the pipe!
        super::AlignCMD::write_tirp_to_interleaved_fq(
            self.path_in.path().path(),
            &path_pipe_r12,
            budget.numof_threads_read,
            self.sizeof_stream_arena,
            budget.sizeof_stream_buffer,
        )?;

        //Wait until process done
        info!("Waiting for kmer counter to finish");

        {
            let out = proc_aligner.stderr.unwrap();
            let buf_reader = BufReader::new(out);
            let mut lines = buf_reader.lines();
            while let Some(Ok(line)) = lines.next() {
                info!("KMC: {}", line);
            }
        }


//        proc_aligner.wait().unwrap(); ////////////////////////// TODO: should watch this process for abnormal exit, possibly panic. need to do in parallel to write_tirp_to_fq

        //Clean up: remove pipes
        std::fs::remove_file(path_pipe_r12)?;

        info!(
            "All steps complete"
        );

        //Move temp files to their right positions

        Ok(())
    }
}



///
/// Generate KMC3 command
/// 
/// Impossible! kmc cannot handle named pipes
/// 
fn create_kmc_process<P> (
    path_in: &P,
    path_out: &P,
    path_temp: &P,
    budget: &KmcBudget,
    cmd: &KmcReadsCMD
) -> Result<std::process::Child> where P: AsRef<Path> {
    info!("wtf????!!");
    let path_in = format!("{}",path_in.as_ref().as_os_str().to_str().expect("os str"));
    let path_out = format!("{}",path_out.as_ref().as_os_str().to_str().expect("os str"));
    let path_temp = format!("{}",path_temp.as_ref().as_os_str().to_str().expect("os str"));

    info!("wtf????");

    let mem_gb = budget.sizeof_kmc_mem.as_gb().round() as u64;
    if mem_gb==0 {
        panic!("Need at least 1gb mem for KMC")
    }

    let mut args = vec![
        format!("-k{}",31),  ///////////////// make this a param
        format!("-m{}",mem_gb),
        format!("-t{}",budget.numof_threads_kmc.get()),
    ];
    if let Some(val) = cmd.kmc_ci {
        args.push(format!("-ci{}",val));
    }

    args.push(path_in);
    args.push(path_out);
    args.push(path_temp);

    info!("Starting KMC {:?}", args);

    let proc_cmd = std::process::Command::new("kmc")  // /data/henlab/software/bin/kmc
        .args(args)
//        .stderr(Stdio::piped())
//        .stdout(Stdio::piped())
        .spawn()?;

    Ok(proc_cmd)
}


/*
 * 
 * New strat: run KMC3 for each cell
 * 
 * OR: metagraph for each cell, output contigs, concatenate all contigs into one file.
 * this appears to be safer as KMC is pretty unstable
 * 
 * 
 * 
 * 
 * 
 */



