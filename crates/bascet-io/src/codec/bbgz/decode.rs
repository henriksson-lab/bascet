use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use crossbeam::channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender};
use libdeflater::Decompressor;

use bascet_core::{
    Decode, DecodeResult,
    channel::{OrderedDenseReceiver, OrderedDenseSender},
};

use crate::{BBGZExtra, BBGZHeaderBase, BBGZTrailer, codec::bbgz::MARKER_EOF};

pub struct BBGZDecoder {
    inner_reader_handle: Option<JoinHandle<()>>,
    inner_worker_handles: Vec<JoinHandle<()>>,
    inner_result_rx: OrderedDenseReceiver<BBGZDecodeResult, 4096>,
    inner_decoded: Vec<u8>,
    inner_decoded_cursor: usize,
    inner_sizeof_alloc: usize,
    inner_eof: bool,
    inner_cancel: Arc<AtomicBool>,
}

struct BBGZDecodeJob {
    seq: usize,
    compressed: Vec<u8>,
    trailer_crc32: u32,
    trailer_isize: u32,
}

type BBGZDecodeResult = anyhow::Result<Vec<u8>>;

#[bon::bon]
impl BBGZDecoder {
    #[builder]
    pub fn new<P: AsRef<Path>>(
        with_path: P,
        #[builder(default = BoundedU64::const_new::<1>())] countof_threads: BoundedU64<
            1,
            { u64::MAX },
        >,
    ) -> Self {
        let file = File::open(with_path.as_ref()).unwrap_or_else(|err| {
            panic!(
                "failed to open BBGZ input {}: {err}",
                with_path.as_ref().display()
            )
        });

        let worker_count = countof_threads.get() as usize;
        let sizeof_block = ByteSize::kib(64);
        let sizeof_alloc =
            ((countof_threads.get() * sizeof_block.as_u64()) / (size_of::<u8>() as u64)) as usize;
        let cancel = Arc::new(AtomicBool::new(false));
        let (job_tx, job_rx) = crossbeam::channel::bounded(worker_count * 4);
        let (result_tx, result_rx) = bascet_core::channel::ordered_dense::<_, 4096>();

        let reader_handle =
            spawn_reader(file, job_tx.clone(), result_tx.clone(), Arc::clone(&cancel));
        let worker_handles = spawn_workers(worker_count, job_rx, result_tx, Arc::clone(&cancel));

        Self {
            inner_reader_handle: Some(reader_handle),
            inner_worker_handles: worker_handles,
            inner_result_rx: result_rx,
            inner_decoded: Vec::new(),
            inner_decoded_cursor: 0,
            inner_sizeof_alloc: sizeof_alloc,
            inner_eof: false,
            inner_cancel: cancel,
        }
    }
}

impl Decode for BBGZDecoder {
    fn sizeof_target_alloc(&self) -> usize {
        self.inner_sizeof_alloc
    }

    fn decode_into<B: AsMut<[u8]>>(&mut self, mut buf: B) -> DecodeResult {
        if self.inner_eof {
            return DecodeResult::Eof;
        }

        let out = buf.as_mut();
        if out.is_empty() {
            return DecodeResult::Decoded(0);
        }

        while self.inner_decoded_cursor == self.inner_decoded.len() {
            self.inner_decoded.clear();
            self.inner_decoded_cursor = 0;

            match self.inner_result_rx.recv() {
                Ok(Ok(decoded)) => {
                    if decoded.is_empty() {
                        continue;
                    }
                    self.inner_decoded = decoded;
                }
                Ok(Err(err)) => return DecodeResult::Error(err),
                Err(_) => {
                    self.inner_eof = true;
                    return DecodeResult::Eof;
                }
            }
        }

        let available = self.inner_decoded.len() - self.inner_decoded_cursor;
        let n = available.min(out.len());
        out[..n].copy_from_slice(
            &self.inner_decoded[self.inner_decoded_cursor..self.inner_decoded_cursor + n],
        );
        self.inner_decoded_cursor += n;
        DecodeResult::Decoded(n)
    }
}

impl Drop for BBGZDecoder {
    fn drop(&mut self) {
        self.inner_cancel.store(true, Ordering::Release);

        if let Some(handle) = self.inner_reader_handle.take() {
            let _ = handle.join();
        }

        while let Some(handle) = self.inner_worker_handles.pop() {
            let _ = handle.join();
        }
    }
}

fn spawn_reader(
    file: File,
    job_tx: Sender<BBGZDecodeJob>,
    result_tx: OrderedDenseSender<BBGZDecodeResult, 4096>,
    cancel: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("BBGZRead@0".to_string())
        .spawn(move || {
            let mut reader = BufReader::new(file);
            let mut seq = 0;

            while !cancel.load(Ordering::Acquire) {
                let job = match read_next_job(&mut reader, seq) {
                    Ok(Some(job)) => job,
                    Ok(None) => break,
                    Err(err) => {
                        result_tx.send(seq, Err(err));
                        break;
                    }
                };

                let mut pending_job = job;
                loop {
                    match job_tx.send_timeout(pending_job, Duration::from_millis(100)) {
                        Ok(()) => break,
                        Err(SendTimeoutError::Timeout(returned_job)) => {
                            if cancel.load(Ordering::Acquire) {
                                return;
                            }
                            pending_job = returned_job;
                            continue;
                        }
                        Err(SendTimeoutError::Disconnected(_)) => return,
                    }
                }

                seq += 1;
            }
        })
        .unwrap()
}

fn spawn_workers(
    count: usize,
    job_rx: Receiver<BBGZDecodeJob>,
    result_tx: OrderedDenseSender<BBGZDecodeResult, 4096>,
    cancel: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    (0..count)
        .map(|idx| {
            let thread_job_rx = job_rx.clone();
            let thread_result_tx = result_tx.clone();
            let thread_cancel = Arc::clone(&cancel);

            std::thread::Builder::new()
                .name(format!("BBGZDecode@{idx}"))
                .spawn(move || {
                    let mut decompressor = Decompressor::new();

                    loop {
                        let job = match thread_job_rx.recv_timeout(Duration::from_millis(100)) {
                            Ok(job) => job,
                            Err(RecvTimeoutError::Timeout) => {
                                if thread_cancel.load(Ordering::Acquire) {
                                    break;
                                }
                                continue;
                            }
                            Err(RecvTimeoutError::Disconnected) => break,
                        };

                        let seq = job.seq;
                        let result = decode_job(&mut decompressor, job);
                        thread_result_tx.send(seq, result);
                    }
                })
                .unwrap()
        })
        .collect()
}

fn read_next_job<R: Read>(reader: &mut R, seq: usize) -> anyhow::Result<Option<BBGZDecodeJob>> {
    let mut base = [0u8; BBGZHeaderBase::SSIZE];
    match read_exact_or_eof(reader, &mut base)? {
        ReadStatus::Eof => return Ok(None),
        ReadStatus::Read => {}
    }

    validate_base_header(&base)?;

    let xlen = u16::from_le_bytes([base[10], base[11]]) as usize;
    let mut extra = vec![0u8; xlen];
    reader.read_exact(&mut extra)?;

    let bsize = find_bsize(&extra)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("BBGZ block size overflow"))?;
    let header_len = BBGZHeaderBase::SSIZE + xlen;
    if bsize < header_len + BBGZTrailer::SSIZE {
        return Err(anyhow::anyhow!(
            "invalid BBGZ block size: block={bsize}, header={header_len}"
        ));
    }

    let rest_len = bsize - header_len;
    let mut rest = vec![0u8; rest_len];
    reader.read_exact(&mut rest)?;

    if is_eof_marker(&base, &extra, &rest) {
        return Ok(None);
    }

    let compressed_len = rest_len - BBGZTrailer::SSIZE;
    let trailer = BBGZTrailer::from_bytes(&rest[compressed_len..])
        .map_err(|_| anyhow::anyhow!("invalid BBGZ trailer"))?;
    let trailer_isize = trailer.ISIZE;
    let trailer_crc32 = trailer.CRC32;
    rest.truncate(compressed_len);

    Ok(Some(BBGZDecodeJob {
        seq,
        compressed: rest,
        trailer_crc32,
        trailer_isize,
    }))
}

fn decode_job(decompressor: &mut Decompressor, job: BBGZDecodeJob) -> anyhow::Result<Vec<u8>> {
    let expected_len = job.trailer_isize as usize;
    let mut decoded = vec![0; expected_len];
    let decoded_len = decompressor
        .deflate_decompress(&job.compressed, &mut decoded)
        .map_err(|err| anyhow::anyhow!("BBGZ deflate decompression failed: {err}"))?;

    if decoded_len != expected_len {
        return Err(anyhow::anyhow!(
            "BBGZ ISIZE mismatch: trailer={expected_len}, decoded={decoded_len}"
        ));
    }

    let actual_crc = crc32fast::hash(&decoded);
    if actual_crc != job.trailer_crc32 {
        return Err(anyhow::anyhow!(
            "BBGZ CRC mismatch: trailer={:#010x}, decoded={:#010x}",
            job.trailer_crc32,
            actual_crc
        ));
    }

    Ok(decoded)
}

enum ReadStatus {
    Read,
    Eof,
}

fn read_exact_or_eof<R: Read>(reader: &mut R, mut buf: &mut [u8]) -> anyhow::Result<ReadStatus> {
    let mut seen = 0;
    while !buf.is_empty() {
        match reader.read(buf) {
            Ok(0) if seen == 0 => return Ok(ReadStatus::Eof),
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => {
                seen += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(ReadStatus::Read)
}

fn validate_base_header(base: &[u8; BBGZHeaderBase::SSIZE]) -> anyhow::Result<()> {
    if base[0] != 0x1f || base[1] != 0x8b {
        return Err(anyhow::anyhow!(
            "invalid BBGZ magic: got ({:#04x}, {:#04x})",
            base[0],
            base[1]
        ));
    }
    if base[2] != 8 {
        return Err(anyhow::anyhow!(
            "invalid BBGZ compression method: {}",
            base[2]
        ));
    }
    if base[3] & 0x04 == 0 {
        return Err(anyhow::anyhow!("BBGZ block is missing gzip FEXTRA flag"));
    }
    Ok(())
}

fn find_bsize(extra: &[u8]) -> anyhow::Result<usize> {
    let mut cursor = 0;
    while cursor < extra.len() {
        if cursor + BBGZExtra::SSIZE > extra.len() {
            return Err(anyhow::anyhow!("truncated BBGZ extra field header"));
        }

        let si1 = extra[cursor];
        let si2 = extra[cursor + 1];
        let len = u16::from_le_bytes([extra[cursor + 2], extra[cursor + 3]]) as usize;
        let data_start = cursor + BBGZExtra::SSIZE;
        let data_end = data_start + len;
        if data_end > extra.len() {
            return Err(anyhow::anyhow!("truncated BBGZ extra field data"));
        }

        if (si1, si2) == (b'B', b'C') {
            if len != 2 {
                return Err(anyhow::anyhow!("invalid BBGZ BC field length: {len}"));
            }
            return Ok(u16::from_le_bytes([extra[data_start], extra[data_start + 1]]) as usize);
        }

        cursor = data_end;
    }

    Err(anyhow::anyhow!("BBGZ block is missing BC extra field"))
}

fn is_eof_marker(base: &[u8; BBGZHeaderBase::SSIZE], extra: &[u8], rest: &[u8]) -> bool {
    if base.len() + extra.len() + rest.len() != MARKER_EOF.len() {
        return false;
    }

    base == &MARKER_EOF[..BBGZHeaderBase::SSIZE]
        && extra == &MARKER_EOF[BBGZHeaderBase::SSIZE..BBGZHeaderBase::SSIZE + extra.len()]
        && rest == &MARKER_EOF[BBGZHeaderBase::SSIZE + extra.len()..]
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{File, remove_file},
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{BBGZHeader, BBGZWriter, Compression};

    use super::*;

    #[test]
    fn decodes_bbgz_with_custom_id_extra() {
        let path = std::env::temp_dir().join(format!(
            "bascet-bbgz-decode-test-{}.bbgz",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        {
            let output = File::create(&path).unwrap();
            let mut writer = BBGZWriter::builder()
                .countof_threads(BoundedU64::const_new::<1>())
                .compression_level(Compression::fastest())
                .with_writer(output)
                .build();

            let mut header = BBGZHeader::new();
            unsafe {
                header.add_extra_unchecked(b"ID", b"cell_1".to_vec());
            }

            let mut block = writer.begin(header);
            block.write_all(b"ACGT\nTGCA\n").unwrap();
            block.flush().unwrap();
            drop(block);
            drop(writer);
        }

        let mut decoder = BBGZDecoder::builder()
            .with_path(&path)
            .countof_threads(BoundedU64::const_new::<1>())
            .build();
        let mut buf = vec![0u8; 64];
        let n = match decoder.decode_into(&mut buf) {
            DecodeResult::Decoded(n) => n,
            DecodeResult::Eof => panic!("unexpected eof"),
            DecodeResult::Error(err) => panic!("{err}"),
        };
        assert_eq!(&buf[..n], b"ACGT\nTGCA\n");

        match decoder.decode_into(&mut buf) {
            DecodeResult::Eof => {}
            DecodeResult::Decoded(n) => panic!("expected eof, decoded {n} bytes"),
            DecodeResult::Error(err) => panic!("{err}"),
        }

        remove_file(path).unwrap();
    }
}
