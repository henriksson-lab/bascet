use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    sync::Mutex,
};

use zip::ZipWriter;

pub struct Threading {
    pub zip_writer: Mutex<ZipWriter<BufWriter<File>>>,
}

unsafe impl Send for Threading {}
unsafe impl Sync for Threading {}

impl Threading {
    pub fn new(file_archive: File) -> Self {
        Self {
            zip_writer: Mutex::new(ZipWriter::new(BufWriter::new(file_archive))),
        }
    }
}
