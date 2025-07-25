use std::{
    fs::File,
    io::{BufReader, Read},
    sync::Arc,
};

use rust_htslib::htslib;

use zip::{HasZipMetadata, ZipArchive};

use crate::{
    common::{self},
    io::{self, BascetFile, BascetStream, BascetStreamToken, BascetStreamTokenBuilder},
    log_critical, log_info,
};

pub struct Stream<T> {
    inner_archive: ZipArchive<std::fs::File>,
    inner_files: Vec<String>,
    inner_files_cursor: usize,

    worker_threadpool: threadpool::ThreadPool,

    _marker_t: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::zip::File) -> Self {
        let path = file.file_path();
        let file = File::open(path).unwrap();
        let archive = ZipArchive::new(file).unwrap();
        let files: Vec<String> = archive
            .file_names()
            .filter(|n| n.ends_with("fa"))
            .map(|s| String::from(s))
            .collect();

        Stream::<T> {
            inner_archive: archive,
            inner_files: files,
            inner_files_cursor: 0,

            worker_threadpool: threadpool::ThreadPool::new(1),

            _marker_t: std::marker::PhantomData,
        }
    }
}

// impl<T> Drop for Stream<T> {
//     fn drop(&mut self) {

//     }
// }

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetStreamToken + Send + 'static,
{
    fn next_cell(&mut self) -> anyhow::Result<Option<T>> {
        let archive = &mut self.inner_archive;

        if self.inner_files_cursor >= self.inner_files.len() {
            return Ok(None);
        }

        // println!("{}", self.inner_files_cursor);
        let mut file = archive
            .by_name(&self.inner_files[self.inner_files_cursor])
            .unwrap();

        self.inner_files_cursor += 1;

        let path = file.get_metadata().file_name_sanitized();
        let id = path
            .parent()
            .unwrap()
            .file_stem()
            .unwrap()
            .as_encoded_bytes();

        let mut builder = T::builder().add_cell_id_owned(id.to_vec());
        let mut cursor = 0;
        let mut buffer: Vec<u8> = Vec::new();

        if let Ok(bytes_read) = file.read_to_end(&mut buffer) {
            match bytes_read {
                0 => {
                    let token = builder.build();
                    return Ok(Some(token));
                }
                _ => {
                    while let Some(next_pos) =
                        memchr::memchr(common::U8_CHAR_FASTA_IDEN, &buffer[cursor..])
                    {
                        // next record exists
                        let line =
                            match memchr::memchr(common::U8_CHAR_FASTA_IDEN, &buffer[cursor..]) {
                                Some(eor) => &buffer[cursor..(cursor + eor).saturating_sub(1)],
                                None => &buffer[cursor..],
                            };

                        let seq = match memchr::memchr(common::U8_CHAR_NEWLINE, line) {
                            Some(record_seq_start) => &line[record_seq_start + 1..],
                            None => {
                                cursor += next_pos + 1;

                                continue;
                            }
                        };
                        builder = builder.add_sequence_owned(seq.to_vec());
                        cursor += next_pos + 1;
                    }

                    let token = builder.build();
                    return Ok(Some(token));
                }
            }
        }
        Err(anyhow::anyhow!("Read error"))
    }

    fn set_reader_threads(mut self, n_threads: usize) -> Self {
        self
    }
}
