pub struct ThreadState<W>
where
    W: std::io::Seek + std::io::Write,
{
    pub zip_writer: std::cell::UnsafeCell<zip::ZipWriter<std::io::BufWriter<W>>>,
}

unsafe impl<W> Send for ThreadState<W> where W: std::io::Seek + std::io::Write {}
unsafe impl<W> Sync for ThreadState<W> where W: std::io::Seek + std::io::Write {}

pub type DefaultThreadState = ThreadState<std::fs::File>;
impl<W> ThreadState<W>
where
    W: std::io::Seek + std::io::Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            zip_writer: std::cell::UnsafeCell::new(zip::ZipWriter::new(std::io::BufWriter::new(
                writer,
            ))),
        }
    }
}
