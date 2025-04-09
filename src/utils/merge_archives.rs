use std::{
    fs::{self, File},
    io::BufWriter, path::PathBuf,
};

use zip::{ZipArchive, ZipWriter};

pub fn merge_archives<P>(destination: &P, sources: &Vec<P>) -> anyhow::Result<()>
where
    P: AsRef<std::path::Path>,
{
    let file_destination = File::create(&destination).unwrap();
    let mut bufwriter_destination = BufWriter::new(&file_destination);
    let mut zipwriter_destination = ZipWriter::new(&mut bufwriter_destination);

    for source in sources {
        let file_source = File::open(&source).unwrap();
        let mut archive_source = ZipArchive::new(&file_source).unwrap();

        for i in 0..archive_source.len() {
            let file = archive_source.by_index(i)?;
            zipwriter_destination.raw_copy_file(file)?;
        }
    }
    zipwriter_destination.finish().unwrap();

    Ok(())
}

use std::ffi::OsString;

/// Take multiple zip-files, and merge into a new one. Then delete the source zip files
pub fn merge_archives_and_delete<P>(destination: &P, sources: &Vec<P>) -> anyhow::Result<()>
where
    P: AsRef<std::path::Path>,
{

    //Merge into a temp-file first, to make it easy to tell if the write was complete or not
    let mut destination_temp = OsString::from(destination.as_ref().as_os_str());
    destination_temp.push("_");
    let destination_temp = PathBuf::from(destination_temp);

    let file_destination = File::create(&destination_temp).unwrap();
    let mut bufwriter_destination = BufWriter::new(&file_destination);
    let mut zipwriter_destination = ZipWriter::new(&mut bufwriter_destination);

    let mut num_out = 0;
    for source in sources {
        println!("{}", source.as_ref().display());
        let file_source = File::open(&source).unwrap();
        let mut archive_source = ZipArchive::new(&file_source).unwrap();

        for i in 0..archive_source.len() {

            if num_out%100 == 0 {
                println!("{} {} / {} ", source.as_ref().display(), i, archive_source.len());
            }

            let file = archive_source.by_index(i)?;
            zipwriter_destination.raw_copy_file(file)?;
            num_out += 1;
        }

        fs::remove_file(source).unwrap();
    }
    println!("Finishing merged zip");
    zipwriter_destination.finish().unwrap();

    //Move file from temp-file to real file, signifying that the file is now complete
    std::fs::rename(destination_temp, destination)?;

    Ok(())
}
