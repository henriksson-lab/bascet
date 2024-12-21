pub struct ThreadState<W>
where
    W: std::io::Seek + std::io::Write,
{
    pub zip_writer: std::cell::UnsafeCell<zip::ZipWriter<std::io::BufWriter<W>>>,
    pub temp_path: std::sync::Arc<std::path::PathBuf>,
}

unsafe impl<W> Send for ThreadState<W> where W: std::io::Seek + std::io::Write {}
unsafe impl<W> Sync for ThreadState<W> where W: std::io::Seek + std::io::Write {}

pub type DefaultThreadState = ThreadState<std::fs::File>;
impl<W> ThreadState<W>
where
    W: std::io::Seek + std::io::Write,
{
    pub fn new(writer: W, temp_path: std::path::PathBuf) -> Self {
        Self {
            zip_writer: std::cell::UnsafeCell::new(zip::ZipWriter::new(std::io::BufWriter::new(
                writer,
            ))),
            temp_path: std::sync::Arc::new(temp_path),
        }
    }
}
