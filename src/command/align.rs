use crate::{
    bounded_parser
};

use bascet_core::{
    attr::{meta::*, sequence::*, quality::*},
    *,
};
use bascet_derive::Budget;
use bascet_io::{codec, parse, tirp};

use anyhow::Result;
use bounded_integer::BoundedU64;
use bytesize::*;
use clap::Args;
use clio::InputPath;
use std::{
    io::{BufReader, BufWriter, Write}, path::{Path, PathBuf}, process::Stdio
};
use tracing::{info, warn};


//   bascet pipefq input.tirp "bwa R1.fq R2.fq "  .... but this also gives a shitty BAM output. shall we convert directly and avoid samtools?
//   bascet pipefq input.tirp output.bam "bwa R1.fq R2.fq out.bam"   <--- these files will be replaced with names of pipes
//   STAR might read input files twice .. can it be a named pipe at all?


// TODO control threads for star. overallocate?

#[derive(Args)]
pub struct AlignCMD {
    #[arg(
        short = 'i',
        long = "in",
        //num_args = 1..,
        //value_delimiter = ',',
        help = "List of input files (comma-separated). Assumed to be sorted by cell id in descending order."
    )]
    pub path_in: InputPath,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output file for unsored BAM"
    )]
    pub path_out_unsorted: PathBuf,

    #[arg(
        short = 's',
        long = "sorted",
        help = "Output file for sorted BAM"
    )]
    pub path_out_sorted: PathBuf,

    #[arg(
        long = "temp",
        help = "Temp directory; must exist already"
    )]
    pub path_temp: PathBuf,


    #[arg(
        short = 'g',
        long = "genome",
        //num_args = 1..,
        //value_delimiter = ',',
        help = "Genome to use"
    )]
    pub path_genome: InputPath,

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
        value_name = "1.. (50%)",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_read: Option<BoundedU64<1, { u64::MAX }>>,

    #[arg(
        long = "numof-threads-writebam",
        help = "Number of threads threads for writing the unsorted BAM file",
        value_name = "1.. (50%)",
        value_parser = bounded_parser!(BoundedU64<1, { u64::MAX }>),
    )]
    numof_threads_writebam: Option<BoundedU64<1, { u64::MAX }>>,

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

    #[arg(
        long = "aligner",
        help = "The command to send the data to",
        hide_short_help = true,
    )]
    aligner: String,


}

#[derive(Budget, Debug)]
struct AlignBudget {
    #[threads(Total)]
    threads: BoundedU64<2, { u64::MAX }>,

    #[mem(Total)]
    memory: ByteSize,

    #[threads(TRead, |total_threads: u64, _| bounded_integer::BoundedU64::new((total_threads as f64 ) as u64).unwrap())]
    numof_threads_read: BoundedU64<1, { u64::MAX }>,

    #[threads(TWrite, |_, _| bounded_integer::BoundedU64::new(1).unwrap())]
    numof_threads_write: BoundedU64<1, 1>,

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl AlignCMD {
    pub fn try_execute(&mut self) -> Result<()> {


        //We should use 1 thread in, x threads out, the rest for the aligner. BAM is not compressed, so one thread might be enough (avoid copying)
        let budget = AlignBudget::builder()
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
            .maybe_numof_threads_read(BoundedU64::new(1))
            .maybe_numof_threads_write(BoundedU64::new(1))
            .maybe_sizeof_stream_buffer(self.sizeof_stream_buffer)
            .build();

        budget.validate();

        info!(
            using = %budget,
            input_path = ?self.path_in,
            unsorted_output_path = ?self.path_out_unsorted,
            sorted_output_path = ?self.path_out_sorted,
            "Starting Align"
        );


        /////////////////////////////////////////////////////////////////////////////////////    /////// TODO delete these pipes!
        // Set up named pipes
        let path_pipe_r1 = self.path_temp.join("fifo_r1.fq");
        let path_pipe_r2 = self.path_temp.join("fifo_r2.fq");
        nix::unistd::mkfifo(&path_pipe_r1, nix::sys::stat::Mode::S_IRWXU)?;
        nix::unistd::mkfifo(&path_pipe_r2, nix::sys::stat::Mode::S_IRWXU)?;
        
        

        ///////////////////////////////////////////////////////////////////////////////////// 
        // Start the aligner
        let num_threads = 8;
        let mut proc_aligner = if self.aligner=="STAR" {
            prep_star(
                self.path_genome.path().path().to_str().expect("could not get genome path"),
                &path_pipe_r1,
                &path_pipe_r2,
                num_threads
            )
        } else if self.aligner=="BWA" {
            prep_bwa(
                &self.path_genome.path().path(), //.to_str().expect("could not get genome path"),
                &path_pipe_r1,
                &path_pipe_r2,
                num_threads
            )
        } else {
            panic!("Aligner argument is invalid");
        }?;
        let aligner_stdout = proc_aligner.stdout.take().expect("Failed to get stdout");
        let reader_aligner_stdout = BufReader::new(aligner_stdout);

        ///////////////////////////////////////////////////////////////////////////////////// 
        // Convert SAM => BAM and store unsorted to disk
        let numof_threads_writebam = if let Some(t) = self.numof_threads_writebam {
            t.get()
        } else {
            1
        };
        let mut proc_samtobam = proc_sam_to_bam(
            numof_threads_writebam as usize,
            &self.path_out_unsorted
        )?;
        let writer_tagbam = BufWriter::new(proc_samtobam.stdin.take().expect("could not open samtobam"));

        ///////////////////////////////////////////////////////////////////////////////////// 
        // This thread takes the output SAM data, and adds proper tags to it
//        let writer_tagbam = BufWriter::new(std::fs::File::create("/husky/henriksson/atrandi/v6_251128_jyoti_mock_bulk/foo.sam")?);  ///////temp
        let handle_tagbam = budget.spawn::<TWrite, _, _>(0 as u64, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread"); 
            info!(thread = thread_name, "Starting tag-bam worker");
 
            sam_add_bc_tag(
                writer_tagbam,
                reader_aligner_stdout
            ).expect("Failed to run BAM tagger");            
        });

        ///////////////////////////////////////////////////////////////////////////////////// 
        // All threads are now set up. Send all readpairs to the aligner
        write_tirp_to_fq(
            self.path_in.path().path(),
            &path_pipe_r1,
            &path_pipe_r2,
            budget.numof_threads_read,
            self.sizeof_stream_arena,
            budget.sizeof_stream_buffer,
        )?;

        //Wait for the output BAM to have been converted
        info!("Waiting for aligner process to finish");
        handle_tagbam.join().unwrap();
        //Wait for sam2bam writer as well
        proc_samtobam.wait()?;

        //Clean up: remove pipes
        std::fs::remove_file(path_pipe_r1)?;
        std::fs::remove_file(path_pipe_r2)?;

        //Sort the bam file
        info!("Sorting BAM file");
        sort_bam(
            self.path_out_unsorted.to_str().expect("error getting unsorted path"), 
            self.path_out_sorted.to_str().expect("error getting sorted path"), 
            budget.threads.get() as usize
        ).expect("Failed to sort output");

        //Index the bam file
        info!("Indexing BAM file");
        index_bam(
            self.path_out_sorted.to_str().expect("error getting unsorted path"), 
        ).expect("Failed to index output");

        info!(
            "All alignment steps complete"
//            "input_file" => self.path_in.path().path()
//            "output_file" => self.pa
        );

        //Move temp files to their right positions

        Ok(())
    }
}





/// 
/// Read SAM content, and output SAM content with added barcode tags
/// 
pub fn sam_add_bc_tag<W,R>(
    writer: W,
    reader: R
) -> Result<()> 
where W:Sized + std::io::Write, R:std::io::BufRead {

    let mut writer = BufWriter::new(writer);

    for line in reader.lines() {
        let line = line.unwrap();

        //println!("{}", line);

        if line.starts_with("@") {
            //This is a header line
            writeln!(writer, "{}", line).unwrap();
        } else {
            //This is a read that need to be mangled
            let (cell_id, umi) = crate::fileformat::bam::readname_to_cell_umi(line.as_bytes());

            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\tCB:Z:")?;
            writer.write_all(cell_id)?;

            if !umi.is_empty() {
                //The SAM specification does not allow empty tags. The UMI can be empty
                writer.write_all(b"\tUB:Z:")?;
                writer.write_all(umi)?;
            }
            writer.write_all(b"\n")?;

            //Typical 10x read
            //A00689:440:HNTNGDRXY:1:1232:23882:9157	0	chr1	629349	3	89M1S	*	0	0	AAACTTCCTACCACTCACCCTAGCATTACTTATATGATATGTCTCCATACCCATTACAATCTCCAGCATTCCCCCTCAAACCTTAAAAAA	FFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF	NH:i:2	HI:i:1	AS:i:83	nM:i:2	RG:Z:lib1:0:1:HNTNGDRXY:1	RE:A:I	xf:i:0	CR:Z:ACGACTTAGTATTGTG	CY:Z:FFFFFFFFFFFFFFFF	CB:Z:ACGACTTAGTATTGTG-1	UR:Z:TAGGCAGAAGCT	UY:Z:FFFFFFFFFFFF	UB:Z:TAGGCAGAAGCT
            //Thus add this:
            //CB:Z:ACGACTTAGTATTGTG-1
            //UB:Z:TAGGCAGAAGCT
        }
    }
    Ok(())
}



///
/// Create a samtools process, converting sam to bam
/// 
pub fn proc_sam_to_bam<P>(
    num_threads: usize,
    path_bam: P
) -> Result<std::process::Child> where P: AsRef<Path> {

    
    let path_bam = format!("{}",path_bam.as_ref().as_os_str().to_str().expect("os str"));

    let num_threads = format!("{}",num_threads);

    let args = vec![
        "view",
        "-",
        "-S", //Input is SAM format
        "-b", //Output bam
        "-@", &num_threads,   //Parallel compression
        "-o", &path_bam,
    ];

    let proc_cmd = std::process::Command::new("samtools")
        .args(args)
        .stdin(Stdio::piped())  
        .spawn()?;

    Ok(proc_cmd)
}




///
/// Generate STAR command. Output will be on stdout
/// 
pub fn prep_star<P> (
    path_genome: &str,
    path_r1: &P, //&str,
    path_r2: &P,
    num_threads: usize,
//    out_filename_prefix: String, // ./STARlog/${TASK_ID}_   STARlog dir must be made and removed as well. temp must be removed
) -> Result<std::process::Child> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);

    let path_r1 = format!("{}",path_r1.as_ref().as_os_str().to_str().expect("os str"));
    let path_r2 = format!("{}",path_r2.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "--genomeDir", path_genome,
        "--readFilesIn",  &path_r1, &path_r2,
        "--runThreadN", &num_threads,
        "--outSAMtype", "SAM", //  this implies unsorted
        "--outSAMunmapped","Within",
        "--outSAMattributes","Standard",
        "--outStd","SAM",
//      paste("--outTmpDir",star_temp_dir),   //   star_temp_dir <- "STARlog/_STARtmp.${TASK_ID}"
//        "--outFileNamePrefix ./STARlog/${TASK_ID}_",
    ];

    let proc_cmd = std::process::Command::new("STAR")
        .args(args)
        .stdout(Stdio::piped())  
        .spawn()?;
    Ok(proc_cmd)
}



///
/// Generate BWA command. Output will be on stdout
/// 
pub fn prep_bwa<P>(
    path_genome: &P,
    path_r1: P,
    path_r2: P,
    num_threads: usize,
) -> Result<std::process::Child> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);

    let path_genome = format!("{}",path_genome.as_ref().as_os_str().to_str().expect("os str"));
    let path_r1 = format!("{}",path_r1.as_ref().as_os_str().to_str().expect("os str"));
    let path_r2 = format!("{}",path_r2.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "mem",
        "-t", &num_threads,
        &path_genome,
        &path_r1, &path_r2,
    ];

    let proc_cmd = std::process::Command::new("bwa")   
        .args(args)
        .stdout(Stdio::piped())  
        .spawn()?;

    Ok(proc_cmd)
}



///
/// Sort a given BAM file, to a new a file
/// 
pub fn sort_bam<P>(
    path_in: P,
    path_out: P,
    num_threads: usize,
) -> Result<()> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);

    let path_in = format!("{}",path_in.as_ref().as_os_str().to_str().expect("os str"));
    let path_out = format!("{}",path_out.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "sort",
        "-@", &num_threads,
        &path_in,
        "-o",&path_out,
    ];

    let _proc_out = std::process::Command::new("samtools")
        .args(args)
        .status()
        .expect("failed to run samtools sort");
    Ok(())
}
 


///
/// Index a given BAM file
/// 
pub fn index_bam(
    path_in: &str,
) -> Result<()> {
    let args = vec![
        "index",
        path_in,
    ];

    let _proc_out = std::process::Command::new("samtools")
        .args(args)
        .status()
        .expect("failed to run samtools index");

    Ok(())
}
 

///
/// Get a TIRP, stream to fastq
/// 
pub fn write_tirp_to_fq<P>(
    path_in: P,
    path_r1: P,
    path_r2: P,
    num_threads: BoundedU64<1, { u64::MAX }>, // budget.numof_threads_read
    sizeof_stream_arena: ByteSize,
    sizeof_stream_buffer: ByteSize,
) -> Result<()> where P: AsRef<Path> {

    ///////////////////////////////////////////////////////////////////////////////////// 
    // Streamer from input TIRP
    let decoder = codec::BBGZDecoder::builder()
        .with_path(path_in)
        .countof_threads(num_threads)
        .build();
    let parser = parse::Tirp::builder().build();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .sizeof_decode_arena(sizeof_stream_arena)
        .sizeof_decode_buffer(sizeof_stream_buffer)
        .build();

    let mut query = stream.query::<tirp::Record>();

    //let file_r1 = std::fs::File::create(&path_r1)?;

    let mut writer_r1 = BufWriter::new(std::fs::File::create(&path_r1)?);  //blocks until reader ready; so open reader first
    let mut writer_r2 = BufWriter::new(std::fs::File::create(&path_r2)?);
    println!("Sending read pairs");
    let mut num_read:u64 = 0;
    loop {
        match query.next_into::<tirp::Record>() {
            Ok(Some(record)) => {

                let record_id = *record.get_ref::<Id>();
                let record_r1 = *record.get_ref::<R1>();
                let record_r2 = *record.get_ref::<R2>();
                let record_q1 = *record.get_ref::<Q1>();
                let record_q2 = *record.get_ref::<Q2>();
                let record_umi = *record.get_ref::<Umi>();

                fn write_read_bascetfq<W>(
                    writer: &mut W,
                    record_id: &[u8], 
                    record_read: &[u8],
                    record_qual: &[u8],
                    record_umi: &[u8],
                    num_read: u64
                ) -> Result<()> where W: Write {
                    writer.write_all(b"@BASCET_")?;
                    writer.write_all(record_id)?;
                    writer.write_all(b":")?;
                    writer.write_all(record_umi)?;
                    writer.write_all(b":")?;
                    writer.write_all(format!("{}", num_read).as_bytes())?;
                    
                    writer.write_all(b"\n")?;
                    writer.write_all(record_read)?;
                    writer.write_all(b"\n+\n")?;
                    writer.write_all(record_qual)?;
                    //for _i in 0..record_read.len() {
                    //    writer.write_all(b"F")?; //or get qual from record ///////////// TODO
                    //}
                    writer.write_all(b"\n")?;
                    Ok(())
                }

                write_read_bascetfq(
                    &mut writer_r1,
                    &record_id,
                    &record_r1,
                    &record_q1,
                    &record_umi,
                    num_read
                )?;

                write_read_bascetfq(
                    &mut writer_r2,
                    &record_id,
                    &record_r2,
                    &record_q2,
                    &record_umi,
                    num_read
                )?;
                
                
                //What about Q? it might not be used at all. but could output as an option
                /*
                Encoding: BWA-MEM defaults to Phred+33, which is standard for Illumina data
                @SEQ_ID
                GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
                +
                !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
                */
                num_read += 1;
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        };
    }
    info!("All readpairs sent");

    //Ensure data is properly pushed out
    writer_r1.flush()?;
    writer_r2.flush()?;
    drop(writer_r1);
    drop(writer_r2);
    
    Ok(())
 }


 //TODO: single-end reads


