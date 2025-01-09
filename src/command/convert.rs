use anyhow::bail;
use std::fs::File;
use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashSet;
use bgzip::{write::BGZFMultiThreadWriter, BGZFError, Compression};


use crate::fileformat::{shard::{CellID, ReadPair}, CellUMI};
use crate::fileformat::DetectedFileformat;
use crate::fileformat::try_get_cells_in_file;


pub struct ConvertFileParams {

    pub include_cells: Option<Vec<CellID>>,

    pub path_in: Vec<std::path::PathBuf>,
    //pub path_tmp: std::path::PathBuf,
    pub path_out: Vec<std::path::PathBuf>,

}




pub struct ConvertFile { //////////////// Can instead be "convert"
}
impl ConvertFile {


    pub fn run(
        params: &Arc<ConvertFileParams>
    ) -> anyhow::Result<()> {
   //     let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
//        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

 //       thread_pool: &threadpool::ThreadPool,
 



        //Get full list of cells, or use provided list. possibly subset to cells present to avoid calls later?
        let include_cells = if let Some(p) = &params.include_cells {
            p.clone()
        } else {

            let mut all_cells: HashSet<CellID> = HashSet::new();
            for p in &params.path_in {
                if let Some(cells) = try_get_cells_in_file(&p).expect("Failed to parse input file") {
                    all_cells.extend(cells);
                } else {
                    //TODO
                    //make this file just stream the content
                }
            }
            let all_cells: Vec<CellID> = all_cells.iter().cloned().collect();
            all_cells
        };


        //Ideally we design functions to convert from [X] -> [Y]
        //If using channels, easy to set up separately

        //Main thread can control which cell to pull out
        

        for p in &params.path_out {
            let writer = BascetFastqWriter::new(&p).expect("Could not open output fastq file");




        }





        //// General threaded reader system; for each cell, call one funct
        //// should call a function 

        

        //// Loop over all cells
        /// 
        /// todo: if cell list provided, need to wait for a reader to have streamed them all
        for cell_id in include_cells {
            
        }




        Ok(())
    }



    
}







pub struct BascetFastqWriter {

    pub writer: BGZFMultiThreadWriter<File>

}
impl BascetFastqWriter {

    pub fn new(path: &PathBuf) -> anyhow::Result<BascetFastqWriter>{
        let out_buffer = File::create(&path).expect("Failed to create fastq.gz output file");
        let writer = BGZFMultiThreadWriter::new(out_buffer, Compression::default());
    
        Ok(BascetFastqWriter {
            writer: writer
        })
    }

    pub fn write_reads_for_cell(&mut self, cell_id:&CellID, list_reads: &Arc<Vec<ReadPair>>) {
        let mut read_num = 0;
        for rp in list_reads.iter() {

            write_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 1),
                &rp.r1,
                &rp.q1
            ).unwrap();

            write_fastq_read(
                &mut self.writer,
                &make_fastq_readname(read_num, &cell_id, &rp.umi, 2),
                &rp.r2,
                &rp.q2
            ).unwrap();

            read_num+=1;
        }
    }
   
}


////////// Write one FASTQ read
fn write_fastq_read<W: std::io::Write>(
    writer: &mut W,
    head: &Vec<u8>,
    seq:&Vec<u8>,
    qual:&Vec<u8>
) -> Result<(), BGZFError> {
    writer.write_all(head.as_slice())?;
    writer.write_all(seq.as_slice())?;
    writer.write_all(b"+\n")?;
    writer.write_all(&qual.as_slice())?;
    Ok(())
}


//// Format FASTQ read names
fn make_fastq_readname(
    read_num: u32, 
    cell_id: &CellID, 
    cell_umi: &CellUMI, 
    illumna_read_index: u32
) -> Vec<u8> {
    // typical readname from a random illumina library from miseq, @M03699:250:000000000-DT36J:1:1102:5914:5953 1:N:0:GACGAGATTA+ACATTATCCT
    let name=format!("BASCET_{}:{}:{} {}", 
        cell_id, 
        String::from_utf8(cell_umi.clone()).unwrap(), 
        read_num, 
        illumna_read_index);
    name.as_bytes().to_vec()  //TODO best if we can avoid making a String
}