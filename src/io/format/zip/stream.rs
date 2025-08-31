use rust_htslib::htslib;
use std::{
    fs::File,
    io::{BufReader, Read},
    sync::Arc,
};
use zip::{HasZipMetadata, ZipArchive};

use crate::{
    io::traits::{
        BascetCell, BascetCellBuilder, BascetFile, BascetStream, CellOwnedIdBuilder,
        CellOwnedUnpairedReadBuilder,
    },
    log_critical, log_info,
};

pub struct Stream<C> {
    inner_archive: ZipArchive<std::fs::File>,
    inner_files: Vec<String>,
    inner_files_cursor: usize,
    inner_worker_threadpool: threadpool::ThreadPool,
    _marker: std::marker::PhantomData<C>,
}

impl<T> Stream<T> {
    pub fn new(file: &crate::io::format::zip::Input) -> Result<Self, crate::runtime::Error> {
        let path = file.path();

        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Err(crate::runtime::Error::file_not_found(path)),
        };

        let archive = match ZipArchive::new(file) {
            Ok(a) => a,
            Err(e) => {
                return Err(crate::runtime::Error::file_not_valid(
                    path,
                    Some(format!("Failed to read zip archive: {}", e)),
                ))
            }
        };

        let files: Vec<String> = archive
            .file_names()
            .filter(|n| n.ends_with("fa"))
            .map(|s| String::from(s))
            .collect();

        Ok(Stream::<T> {
            inner_archive: archive,
            inner_files: files,
            inner_files_cursor: 0,
            inner_worker_threadpool: threadpool::ThreadPool::new(1),
            _marker: std::marker::PhantomData,
        })
    }
}

impl<C> BascetStream<C> for Stream<C>
where
    C: BascetCell + 'static,
    C::Builder: BascetCellBuilder<Cell = C> + CellOwnedIdBuilder + CellOwnedUnpairedReadBuilder,
{
    fn next_cell(&mut self) -> Result<Option<C>, crate::runtime::Error> {
        let archive = &mut self.inner_archive;

        if self.inner_files_cursor >= self.inner_files.len() {
            return Ok(None);
        }

        let mut file = match archive.by_name(&self.inner_files[self.inner_files_cursor]) {
            Ok(f) => f,
            Err(e) => {
                return Err(crate::runtime::Error::parse_error(
                    "zip_archive",
                    Some(format!("Failed to read file from archive: {}", e)),
                ))
            }
        };

        self.inner_files_cursor += 1;

        let path = file.get_metadata().file_name_sanitized();

        let parent = match path.parent() {
            Some(p) => p,
            None => {
                return Err(crate::runtime::Error::parse_error(
                    "zip_file_path",
                    Some("File has no parent directory"),
                ))
            }
        };

        let file_stem = match parent.file_stem() {
            Some(stem) => stem,
            None => {
                return Err(crate::runtime::Error::parse_error(
                    "zip_file_path",
                    Some("Parent directory has no file stem"),
                ))
            }
        };

        let id = file_stem.as_encoded_bytes();
        let mut builder = C::builder().set_owned_id(id.to_vec());
        let mut cursor = 0;
        let mut buffer: Vec<u8> = Vec::new();

        match file.read_to_end(&mut buffer) {
            Ok(bytes_read) => match bytes_read {
                0 => {
                    let token = builder.build();
                    return Ok(Some(token));
                }
                _ => {
                    while let Some(next_pos) =
                        memchr::memchr(crate::common::U8_CHAR_FASTA_IDEN, &buffer[cursor..])
                    {
                        let line = match memchr::memchr(
                            crate::common::U8_CHAR_FASTA_IDEN,
                            &buffer[cursor..],
                        ) {
                            Some(eor) => &buffer[cursor..(cursor + eor).saturating_sub(1)],
                            None => &buffer[cursor..],
                        };

                        let seq = match memchr::memchr(crate::common::U8_CHAR_NEWLINE, line) {
                            Some(record_seq_start) => &line[record_seq_start + 1..],
                            None => {
                                cursor += next_pos + 1;
                                continue;
                            }
                        };

                        builder = builder.push_owned_unpaired_read(seq.to_vec());
                        cursor += next_pos + 1;
                    }

                    let token = builder.build();
                    return Ok(Some(token));
                }
            },
            Err(e) => {
                return Err(crate::runtime::Error::parse_error(
                    "zip_file_read",
                    Some(format!("Failed to read file contents: {}", e)),
                ))
            }
        }
    }
}
