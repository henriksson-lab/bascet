


/* 

use rust_htslib::bam::record::Aux;

pub fn create_new_bam(
    fname: &Path
// num_threads
// compression level
) -> anyhow::Result<bam::Writer> {

    let file_format = detect_bam_file_format(fname)?;

    let mut header = bam::Header::new();
    header.push_comment("Debarcoded by Bascet".as_bytes());

    let mut writer = bam::Writer::from_path(fname, &header, file_format).unwrap();

    _ = writer.set_threads(5);  //  need we also give a pool? https://docs.rs/rust-htslib/latest/rust_htslib/bam/struct.Writer.html#method.set_threads
    _ = writer.set_compression_level(bam::CompressionLevel::Fastest);  //or no compression, do later ; for user to specify

    Ok(writer)
}

*/



/* 

/////////////////////////////////// Writer to tagged BAM file
fn create_writer_thread(
    outfile: &PathBuf,
    thread_pool: &threadpool::ThreadPool
) -> anyhow::Result<Arc<Sender<Option<ListReadWithBarcode>>>> {

    let outfile = outfile.clone();

    //Limit how many chunks can be in pipe
    let (tx, rx) = crossbeam::channel::bounded::<Option<ListReadWithBarcode>>(100);  
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));

    thread_pool.execute(move || {
        // Open cram output file
        println!("Creating output file: {}",outfile.display());
        let mut writer = create_new_bam(&outfile).expect("failed to create bam-like file");

        // Write reads
        let mut n_written=0;
        while let Ok(Some(list_pairs)) = rx.recv() {
            for (bam_cell, hits_names) in list_pairs.iter() {
                let reverse_record=&bam_cell.reverse_record;
                let forward_record=&bam_cell.forward_record;

                write_records_pair_to_bamlike(
                    &mut writer,
                    forward_record,
                    reverse_record,
                    &hits_names
                );

                if n_written%100000 == 0 {
                    println!("written to {:?} -- {:?}",outfile, n_written);
                }
                n_written = n_written + 1;
            }

            
        }
    });
    Ok(tx)
}

*/


