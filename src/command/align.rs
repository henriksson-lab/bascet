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
    fs::File, io::{BufReader, BufWriter, Write}, path::{Path, PathBuf}, process::Stdio
};
use tracing::{info, warn};


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
        short = 'u',
        long = "unsorted",
        help = "Output file for unsorted BAM"
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
    pub path_genome: PathBuf, //File might not exist, so use regular PathBuf

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
        long = "numof-threads-writebam",
        help = "Number of threads threads for writing the unsorted BAM file",
        value_name = "1.. (default is 1)",  // 50% of total threads
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
    numof_threads_writebam: BoundedU64<1, { u64::MAX }>,  // max 1

    #[mem(MBuffer, |_, total_mem| bytesize::ByteSize(total_mem))]
    sizeof_stream_buffer: ByteSize,
}

impl AlignCMD {
    pub fn try_execute(&mut self) -> Result<()> {

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
            .maybe_numof_threads_read(self.numof_threads_read)
            .maybe_numof_threads_writebam(self.numof_threads_writebam)
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
        nix::unistd::mkfifo(&path_pipe_r1, nix::sys::stat::Mode::S_IRWXU).expect("Previous pipe file exists in temp dir; stopping");
        nix::unistd::mkfifo(&path_pipe_r2, nix::sys::stat::Mode::S_IRWXU).expect("Previous pipe file exists in temp dir; stopping");
        
        

        ///////////////////////////////////////////////////////////////////////////////////// 
        // Start the aligner
        let num_threads = budget.threads.get();
        let path_aln_temp = self.path_temp.join("_align_tmp");
        std::fs::create_dir(&path_aln_temp).expect("Failed to create tempdir");
        let mut proc_aligner = if self.aligner=="STAR" {

            prep_star(
                &self.path_genome,
                &path_pipe_r1,
                &path_pipe_r2,
                &path_aln_temp,
                num_threads
            )
        } else if self.aligner=="BWA" {
            prep_bwa(
                &self.path_genome,
                &path_pipe_r1,
                &path_pipe_r2,
                num_threads
            )
        } else if self.aligner=="bowtie2" {
            prep_bowtie2(
                &self.path_genome,
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
        let handle_tagbam = budget.spawn::<TWrite, _, _>(0 as u64, move || {
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unknown thread"); 
            info!(thread = thread_name, "Starting tag-bam worker"); 
            
            sam_add_bc_tag(
                writer_tagbam,
                reader_aligner_stdout
            ).expect("Failed to run BAM tagger"); 

//            pipe_to_screen(&mut reader_aligner_stdout).unwrap();         
        });


        ///////////////////////////////////////////////////////////////////////////////////// 
        // All threads are now set up. Send all readpairs to the aligner
        let mut writer_r1 = BufWriter::new(std::fs::File::create(&path_pipe_r1)?);  //blocks until reader ready; so open reader first
        let mut writer_r2 = BufWriter::new(std::fs::File::create(&path_pipe_r2)?);

        AlignCMD::write_tirp_to_2fq(
            self.path_in.path().path(),
            &mut writer_r1, 
            &mut writer_r2, 
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
            &self.path_out_unsorted,
            &self.path_out_sorted,
            &self.path_temp,
            budget.threads.get()
        ).expect("Failed to sort output");

        //Index the bam file
        info!("Indexing BAM file");
        index_bam(
            self.path_out_sorted.to_str().expect("error getting unsorted path"), 
        ).expect("Failed to index output");

        //Clean up temp files
        std::fs::remove_dir_all(&path_aln_temp).expect("Failed to remove tempdir");

        info!(
            "All alignment steps complete"
//            "input_file" => self.path_in.path().path()
//            "output_file" => self.pa
        );

        //Move temp files to their right positions

        Ok(())
    }




    ///
    /// Get a TIRP, stream to fastq
    /// 
    pub fn write_tirp_to_2fq<P>(
        path_in: P,
        writer_r1: &mut BufWriter<impl Write>,
        writer_r2: &mut BufWriter<impl Write>,
        num_threads: BoundedU64<1, { u64::MAX }>, 
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

        println!("Sending read pairs");
        let mut num_read:u64 = 0;
        loop {
            match query.next_into::<tirp::Record>() {
                Ok(Some(record)) => {
                    //println!("one read pair");

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
                        writer.write_all(b"\n")?;
                        Ok(())
                    }

                    write_read_bascetfq(
                        writer_r1,
                        &record_id,
                        &record_r1,
                        &record_q1,
                        &record_umi,
                        num_read
                    )?;

                    write_read_bascetfq(
                        writer_r2,
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
                    println!("read pairs {}", num_read);

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
        info!("All readpairs flushed");
        
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
/// Pipe to screen, for debugging
/// 
pub fn pipe_to_screen<R>(
    reader: &mut R
) -> Result<()> 
where R:std::io::BufRead {
    let mut buf = vec![0; 1024];
    loop {
        let len = reader.read(&mut buf).expect("could not read");
        let s = String::from_utf8(buf[0..len].to_vec()).unwrap();
        info!(line = s, "input");
        if len==0 {
            break;
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
    path_genome: &P,
    path_r1: &P, 
    path_r2: &P,
    path_aln_temp: &P,
    num_threads: u64,
) -> Result<std::process::Child> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);

    let path_out_prefix = PathBuf::from(path_aln_temp.as_ref()).join("STAR_");
    let path_out_prefix = format!("{}",path_out_prefix.as_os_str().to_str().expect("os str"));

    let path_genome = format!("{}",path_genome.as_ref().as_os_str().to_str().expect("os str"));
    let path_r1 = format!("{}",path_r1.as_ref().as_os_str().to_str().expect("os str"));
    let path_r2 = format!("{}",path_r2.as_ref().as_os_str().to_str().expect("os str"));
    let path_aln_temp = format!("{}",path_aln_temp.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "--genomeDir", &path_genome,
        "--readFilesIn",  &path_r1, &path_r2,
        "--runThreadN", &num_threads,
        "--outSAMtype", "SAM", //  this implies unsorted
        "--outSAMunmapped","Within",
        "--outSAMattributes","Standard",
        "--outStd","SAM",
        "--outTmpDir",&path_aln_temp,
        "--outFileNamePrefix",&path_out_prefix,
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
    path_genome: P,
    path_r1: P,
    path_r2: P,
    num_threads: u64,
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
/// Generate Bowtie2 command. Output will be on stdout
/// 
pub fn prep_bowtie2<P>(
    path_genome: P,
    path_r1: P,
    path_r2: P,
    num_threads: u64,
) -> Result<std::process::Child> where P: AsRef<Path> {

    let log_name = format!("./temp_bt.log");
    let log = File::create(log_name).expect("failed to open log");

//    let out_name = format!("./temp_bt.out");
//    let out = File::create(out_name).expect("failed to open log");

    let num_threads = format!("{}",num_threads);
    let path_genome = format!("{}",path_genome.as_ref().as_os_str().to_str().expect("os str"));
    let path_r1 = format!("{}",path_r1.as_ref().as_os_str().to_str().expect("os str"));
    let path_r2 = format!("{}",path_r2.as_ref().as_os_str().to_str().expect("os str"));

    let args = vec![
        "-x", &path_genome,
        "--threads", &num_threads,
        "--reorder", //Guarantees that output SAM records are printed in an order corresponding to the order of the reads in the original input file, even when -p is set greater than 1
        "--very-sensitive-local", //recommended setting for mapping out human reads; https://www.cell.com/cell-reports-methods/fulltext/S2667-2375(25)00254-1
        "-1", &path_r1, 
        "-2", &path_r2,
        "-S", "/home/mahogny/github/bascet/temp/wtf.sam"
    ];

    let proc_cmd = std::process::Command::new("bowtie2")   
        .args(args)
        .stderr(log)
//        .stdout(out)
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
    path_temp: P,
    num_threads: u64,
) -> Result<()> where P: AsRef<Path> {
    let num_threads = format!("{}",num_threads);

    let path_in = format!("{}",path_in.as_ref().as_os_str().to_str().expect("os str"));
    let path_out = format!("{}",path_out.as_ref().as_os_str().to_str().expect("os str"));

    let path_temp_prefix = PathBuf::from(path_temp.as_ref()).join("sort");
    let path_temp_prefix = format!("{}",path_temp_prefix.as_os_str().to_str().expect("os str"));

    let args = vec![
        "sort",
        "-@", &num_threads,
        &path_in,
        "-T", &path_temp_prefix,
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
 


 //TODO: single-end reads


