use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;

use bytesize::ByteSize;

pub fn file_copy_range(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    len: u64,
) -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        file_copy_range_linux(src, src_offset, dst, dst_offset, len)
    }

    #[cfg(not(target_os = "linux"))]
    {
        file_copy_range_fallback(src, src_offset, dst, dst_offset, len)
    }
}

#[cfg(target_os = "linux")]
fn file_copy_range_linux(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    len: u64,
) -> io::Result<u64> {
    use nix::fcntl::copy_file_range;
    use std::os::fd::BorrowedFd;

    // SAFETY: Caller must ensure file is valid for the duration of this call
    let src_fd = unsafe { BorrowedFd::borrow_raw(src.as_raw_fd()) };
    let dst_fd = unsafe { BorrowedFd::borrow_raw(dst.as_raw_fd()) };

    let mut src_off = src_offset as i64;
    let mut dst_off = dst_offset as i64;

    match copy_file_range(
        src_fd,
        Some(&mut src_off),
        dst_fd,
        Some(&mut dst_off),
        len as usize,
    ) {
        Ok(n) => Ok(n as u64),
        Err(nix::errno::Errno::EXDEV | nix::errno::Errno::ENOSYS) => {
            file_copy_range_fallback(src, src_offset, dst, dst_offset, len)
        }
        Err(e) => Err(io::Error::from_raw_os_error(e as i32)),
    }
}

fn file_copy_range_fallback(
    src: &File,
    src_offset: u64,
    dst: &File,
    dst_offset: u64,
    len: u64,
) -> io::Result<u64> {
    const BUFFER_SIZE: usize = ByteSize::kib(64).as_u64() as usize;

    let mut src = src;
    let mut dst = dst;

    src.seek(SeekFrom::Start(src_offset))?;
    dst.seek(SeekFrom::Start(dst_offset))?;

    let mut buffer = [0u8; BUFFER_SIZE];
    let mut remaining = len;
    let mut total_copied: u64 = 0;

    while remaining > 0 {
        let to_read = (remaining as usize).min(BUFFER_SIZE);
        let bytes_read = src.read(&mut buffer[..to_read])?;

        if bytes_read == 0 {
            break;
        }

        dst.write_all(&buffer[..bytes_read])?;
        remaining -= bytes_read as u64;
        total_copied += bytes_read as u64;
    }

    Ok(total_copied)
}
