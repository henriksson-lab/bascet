use std::fs::File;
use std::io;

#[cfg(unix)]
pub fn file_write_exact_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

// NOTE also untested
#[cfg(windows)]
pub fn file_write_exact_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut total = 0;
    while total < buf.len() {
        let n = file.seek_write(&buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        total += n;
    }
    Ok(())
}