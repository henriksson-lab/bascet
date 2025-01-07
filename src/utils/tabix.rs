



aim to remove dependency on bgzip and tabix!!!!




        // https://github.com/zaeleus/noodles/blob/master/noodles-tabix/examples/tabix_write.rs
        // noodles writing index while writing bed-like file
        // noodles can read, and get virtual position, https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/reader.rs
        // multithreaded reader can also get virtual position https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/multithreaded_reader.rs






        pipe to: 
        let mut writer = noodles_bgzf::MultithreadedWriter::new(writer);




//w: Write








fn generate_tabix_index(p: &PathBuf){


    let mut indexer = tabix::index::Indexer::default();
    indexer.set_header(csi::binning_index::index::header::Builder::bed().build());

    let mut start_position = writer.virtual_position();


            // multithreaded reader can also get virtual position https://github.com/zaeleus/noodles/blob/master/noodles-bgzf/src/multithreaded_reader.rs

            for () {


    .....




    let end_position = writer.virtual_position();
    let chunk = Chunk::new(start_position, end_position);

    indexer.add_record(reference_sequence_name, start, end, chunk)?;

}



    let index = indexer.build();

    let index_dst = format!("{DST}.tbi");
    let mut writer = File::create(index_dst).map(tabix::io::Writer::new)?;
    writer.write_index(&index)?;





}       


