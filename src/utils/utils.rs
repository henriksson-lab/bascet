use std::{
    fs::{DirEntry, File},
    io::{self, BufReader, BufWriter, Write},
    path::Path,
};

pub fn concat_files_two<P>(p_path: &P, q_path: &P, cat_path: &P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let cat_file = File::create(cat_path)?;
    let mut cat_writer = BufWriter::new(cat_file);

    let p_file = File::open(p_path)?;
    let mut p_reader = BufReader::new(p_file);
    io::copy(&mut p_reader, &mut cat_writer)?;

    let q_file = File::open(p_path)?;
    let mut q_reader = BufReader::new(q_file);
    io::copy(&mut q_reader, &mut cat_writer)?;

    cat_writer.flush()?;
    Ok(())
}
pub fn concat_files_vec<P>(sources: &Vec<P>, cat_path: &P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let cat_file = File::create(cat_path)?;
    let mut cat_writer = BufWriter::new(cat_file);

    for source in sources {
        let input = File::open(source)?;
        let mut reader = BufReader::new(input);
        io::copy(&mut reader, &mut cat_writer)?;
    }
    cat_writer.flush()?;
    Ok(())
}
