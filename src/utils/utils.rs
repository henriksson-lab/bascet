use std::fs::{remove_file, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;

pub fn concat_files_two<P>(p_path: &P, q_path: &P, cat_path: &P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let cat_file = File::create(cat_path)?;
    let mut cat_writer = BufWriter::new(cat_file);

    let p_file = File::open(p_path)?;
    let p_size = p_file.metadata()?.len();
    let mut p_reader = BufReader::with_capacity(p_size as usize, p_file);
    let _ = io::copy(&mut p_reader, &mut cat_writer)?;

    let q_file = File::open(q_path)?;
    let q_size = q_file.metadata()?.len();
    let mut q_reader = BufReader::with_capacity(q_size as usize, q_file);
    let _ = io::copy(&mut q_reader, &mut cat_writer)?;

    cat_writer.flush()?;

    Ok(())
}

pub fn concat_files_vec<P>(sources: &Vec<P>, cat_path: &P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    // Delete the existing cat file before writing
    if cat_path.as_ref().exists() {
        remove_file(cat_path)?;
    }

    let cat_file = File::create(cat_path)?;
    let mut cat_writer = BufWriter::new(cat_file);

    for source in sources {
        let input = File::open(source)?;
        let size = input.metadata()?.len();
        let mut reader = BufReader::with_capacity(size as usize, input);
        let _ = io::copy(&mut reader, &mut cat_writer)?;
    }

    cat_writer.flush()?;

    Ok(())
}
