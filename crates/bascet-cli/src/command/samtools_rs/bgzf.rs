// BGZF (Blocked GZip Format) reader and writer.
// Spec: https://samtools.github.io/hts-specs/SAMv1.pdf §4.1
//
// Each BGZF block is a gzip member (RFC 1952) with FLG.FEXTRA set and one
// extra subfield: SI1='B' SI2='C' SLEN=2 BSIZE=block_size-1. The deflate
// stream is raw (no zlib wrapper). EOF is signalled by a 28-byte empty block.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{self, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, Sender, bounded};

/// Maximum uncompressed payload per block. Same as htslib's `BGZF_BLOCK_SIZE`:
/// chosen so the worst-case compressed block fits in 65536 bytes (uint16 BSIZE).
pub const BLOCK_SIZE: usize = 0xff00;

const HEADER_LEN: usize = 18; // 12-byte gzip header + 6-byte BC extra subfield.
const TRAILER_LEN: usize = 8; // CRC32 + ISIZE.
const MAX_BLOCK_SIZE: usize = 0x10000;
const PARALLEL_IO_BUFFER_SIZE: usize = 8 * 1024 * 1024;

/// The 28-byte BGZF EOF marker: an empty block. htslib writes this on close.
const EOF_BLOCK: [u8; 28] = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

pub struct Reader<R: Read> {
    inner: BufReader<R>,
    block: Vec<u8>,
    pos: usize,
    saw_eof_block: bool,
    exhausted: bool,
}

impl<R: Read> Reader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: BufReader::new(inner),
            block: Vec::with_capacity(BLOCK_SIZE),
            pos: 0,
            saw_eof_block: false,
            exhausted: false,
        }
    }

    fn fill_block(&mut self) -> io::Result<()> {
        self.block.clear();
        self.pos = 0;

        let mut header = [0u8; HEADER_LEN];
        match self.inner.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.exhausted = true;
                if !self.saw_eof_block {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "BGZF stream ended without EOF block",
                    ));
                }
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        if header[0] != 0x1f || header[1] != 0x8b || header[2] != 0x08 || header[3] != 0x04 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a BGZF block (bad gzip magic / flags)",
            ));
        }
        let xlen = u16::from_le_bytes([header[10], header[11]]) as usize;
        if xlen != 6 || header[12] != b'B' || header[13] != b'C' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing BGZF BC extra subfield",
            ));
        }
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let block_size = bsize + 1;
        if block_size < HEADER_LEN + TRAILER_LEN || block_size > MAX_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BGZF block size out of range",
            ));
        }

        let cdata_len = block_size - HEADER_LEN - TRAILER_LEN;
        let mut cdata = vec![0u8; cdata_len];
        self.inner.read_exact(&mut cdata)?;
        let mut trailer = [0u8; TRAILER_LEN];
        self.inner.read_exact(&mut trailer)?;
        let expected_crc = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        let isize = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]) as usize;

        if isize == 0 && cdata_len == 2 {
            // Empty block — EOF marker (or a benign empty block mid-stream).
            self.saw_eof_block = true;
            return Ok(());
        }
        // A non-EOF block resets the flag — EOF must be the *last* block.
        self.saw_eof_block = false;

        let raw = RawBlock {
            cdata,
            expected_crc,
            expected_isize: isize as u32,
        };
        self.block = inflate_block(&raw)?;
        Ok(())
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.block.len() {
                let n = (self.block.len() - self.pos).min(dst.len());
                dst[..n].copy_from_slice(&self.block[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.exhausted {
                return Ok(0);
            }
            self.fill_block()?;
        }
    }
}

pub struct Writer<W: Write> {
    inner: W,
    buf: Vec<u8>,
    out_block: Vec<u8>,
    level: u8,
    finished: bool,
    /// Compressed file offset of each emitted BGZF block, in order.
    /// `block_offsets[i]` is the byte offset (in the compressed stream)
    /// where block `i` begins. Used to compute virtual offsets for BAI/CSI.
    block_offsets: Vec<u64>,
    current_offset: u64,
}

impl<W: Write> Writer<W> {
    pub fn new(inner: W, level: u8) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(BLOCK_SIZE),
            out_block: Vec::with_capacity(MAX_BLOCK_SIZE),
            level,
            finished: false,
            block_offsets: Vec::new(),
            current_offset: 0,
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        encode_block(&self.buf, self.level, &mut self.out_block)?;
        self.inner.write_all(&self.out_block)?;
        self.block_offsets.push(self.current_offset);
        self.current_offset += self.out_block.len() as u64;
        self.buf.clear();
        Ok(())
    }

    /// Flush remaining bytes, write the BGZF EOF block, and return the inner writer.
    pub fn finish(self) -> io::Result<W> {
        let (inner, _offsets) = self.finish_with_offsets()?;
        Ok(inner)
    }

    /// Like `finish` but also returns per-block compressed offsets for BAI/CSI.
    pub fn finish_with_offsets(mut self) -> io::Result<(W, Vec<u64>)> {
        self.flush_block()?;
        self.inner.write_all(&EOF_BLOCK)?;
        self.inner.flush()?;
        self.finished = true;
        let offsets = std::mem::take(&mut self.block_offsets);
        let inner = unsafe { std::ptr::read(&self.inner) };
        std::mem::forget(self);
        Ok((inner, offsets))
    }
}

impl<W: Write> Write for Writer<W> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        if src.is_empty() {
            return Ok(0);
        }
        let space = BLOCK_SIZE - self.buf.len();
        let n = space.min(src.len());
        self.buf.extend_from_slice(&src[..n]);
        if self.buf.len() == BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_block()?;
        self.inner.flush()
    }
}

impl<W: Write> Drop for Writer<W> {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort: write any pending data + EOF block.
            // Errors here would be lost; prefer calling .finish() explicitly.
            let _ = self.flush_block();
            let _ = self.inner.write_all(&EOF_BLOCK);
        }
    }
}

/// BGZF reader that parallelises inflate across worker threads.
///
/// Architecture: a single reader thread parses block boundaries (header
/// + cdata + trailer) sequentially — the BGZF layout doesn't allow
/// concurrent boundary-finding because each header gives the block's
/// length. It ships raw blocks down `block_tx` with a monotonic seq number.
/// `threads` workers pull (seq, raw), inflate + verify CRC32, send to
/// `decoded_tx`. The Read impl pulls (seq, payload) and reorders via a
/// min-heap so the byte stream is identical to single-threaded `Reader`.
pub struct ParallelReader {
    decoded_rx: Receiver<(u64, Vec<u8>)>,
    pending: BinaryHeap<Reverse<(u64, Vec<u8>)>>,
    next_seq: u64,
    current: Vec<u8>,
    pos: usize,
    exhausted: bool,
    /// Errors raised by the reader or worker threads; surfaced to the
    /// consumer when the channel closes.
    shared_err: Arc<Mutex<Option<io::Error>>>,
    reader: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

struct RawBlock {
    cdata: Vec<u8>,
    expected_crc: u32,
    expected_isize: u32,
}

impl ParallelReader {
    pub fn new<R: Read + Send + 'static>(inner: R, threads: usize) -> Self {
        let threads = threads.max(1);
        // crossbeam-channel is MPMC and lock-free in the fast path — no
        // Mutex<Receiver> contention even with dozens of workers.
        let (block_tx, block_rx) = bounded::<(u64, RawBlock)>(threads * 2);
        let (decoded_tx, decoded_rx) = bounded::<(u64, Vec<u8>)>(threads * 2);
        let shared_err = Arc::new(Mutex::new(None::<io::Error>));

        let reader_err = Arc::clone(&shared_err);
        let reader = std::thread::spawn(move || {
            if let Err(e) = run_inflate_reader(inner, block_tx) {
                let mut slot = reader_err.lock().expect("poisoned err slot");
                if slot.is_none() {
                    *slot = Some(e);
                }
            }
        });

        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let rx = block_rx.clone();
            let tx = decoded_tx.clone();
            let err = Arc::clone(&shared_err);
            workers.push(std::thread::spawn(move || {
                if let Err(e) = run_inflate_worker(rx, tx) {
                    let mut slot = err.lock().expect("poisoned err slot");
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                }
            }));
        }
        // Drop our refs so workers/reader exit when the consumer drops.
        drop(decoded_tx);
        drop(block_rx);

        Self {
            decoded_rx,
            pending: BinaryHeap::new(),
            next_seq: 0,
            current: Vec::new(),
            pos: 0,
            exhausted: false,
            shared_err,
            reader: Some(reader),
            workers,
        }
    }

    fn fill_current(&mut self) -> io::Result<()> {
        loop {
            if let Some(Reverse((seq, _))) = self.pending.peek() {
                if *seq == self.next_seq {
                    let Reverse((_, bytes)) = self.pending.pop().unwrap();
                    self.next_seq += 1;
                    self.current = bytes;
                    self.pos = 0;
                    return Ok(());
                }
            }
            match self.decoded_rx.recv() {
                Ok(item) => self.pending.push(Reverse(item)),
                Err(_) => {
                    if let Some(e) = self.shared_err.lock().expect("poisoned").take() {
                        return Err(e);
                    }
                    self.exhausted = true;
                    return Ok(());
                }
            }
        }
    }
}

impl Read for ParallelReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.current.len() {
                let n = (self.current.len() - self.pos).min(dst.len());
                dst[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.exhausted {
                return Ok(0);
            }
            self.fill_current()?;
            if self.exhausted && self.current.is_empty() {
                return Ok(0);
            }
        }
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        // Drop the consumer side before joining workers. This matters when a caller reads only
        // a prefix of the stream: workers may already be blocked sending decoded blocks into a
        // full channel, and joining while `decoded_rx` is still alive would deadlock.
        let (_tx, rx) = bounded::<(u64, Vec<u8>)>(0);
        drop(std::mem::replace(&mut self.decoded_rx, rx));

        // Workers exit once their send fails, then drop their `block_rx` handles. That lets the
        // reader thread stop if it is blocked sending raw blocks.
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
        if let Some(h) = self.reader.take() {
            let _ = h.join();
        }
    }
}

fn run_inflate_reader<R: Read>(inner: R, tx: Sender<(u64, RawBlock)>) -> io::Result<()> {
    let mut inner = BufReader::with_capacity(PARALLEL_IO_BUFFER_SIZE, inner);
    let mut seq: u64 = 0;
    let mut saw_eof_block = false;
    loop {
        let mut header = [0u8; HEADER_LEN];
        match inner.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                if !saw_eof_block {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "BGZF stream ended without EOF block",
                    ));
                }
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        if header[0] != 0x1f || header[1] != 0x8b || header[2] != 0x08 || header[3] != 0x04 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a BGZF block (bad gzip magic / flags)",
            ));
        }
        let xlen = u16::from_le_bytes([header[10], header[11]]) as usize;
        if xlen != 6 || header[12] != b'B' || header[13] != b'C' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing BGZF BC extra subfield",
            ));
        }
        let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
        let block_size = bsize + 1;
        if block_size < HEADER_LEN + TRAILER_LEN || block_size > MAX_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BGZF block size out of range",
            ));
        }
        let cdata_len = block_size - HEADER_LEN - TRAILER_LEN;
        let mut cdata = vec![0u8; cdata_len];
        inner.read_exact(&mut cdata)?;
        let mut trailer = [0u8; TRAILER_LEN];
        inner.read_exact(&mut trailer)?;
        let expected_crc = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        let expected_isize = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);

        if expected_isize == 0 && cdata_len == 2 {
            // Empty/EOF block — don't ship to workers. Continue in case more
            // blocks follow (BGZF allows concatenated streams).
            saw_eof_block = true;
            continue;
        }
        // Any non-empty block resets EOF — EOF must be the last block we see.
        saw_eof_block = false;

        let raw = RawBlock {
            cdata,
            expected_crc,
            expected_isize,
        };
        if tx.send((seq, raw)).is_err() {
            // Consumer dropped — stop reading.
            return Ok(());
        }
        seq += 1;
    }
}

fn run_inflate_worker(rx: Receiver<(u64, RawBlock)>, tx: Sender<(u64, Vec<u8>)>) -> io::Result<()> {
    loop {
        let Ok((seq, raw)) = rx.recv() else {
            return Ok(());
        };
        let payload = inflate_block(&raw)?;
        if tx.send((seq, payload)).is_err() {
            return Ok(());
        }
    }
}

fn inflate_block(raw: &RawBlock) -> io::Result<Vec<u8>> {
    // zune-inflate is ~2× faster than miniz_oxide on typical BGZF block sizes.
    let mut decoder = zune_inflate::DeflateDecoder::new_with_options(
        &raw.cdata,
        zune_inflate::DeflateOptions::default()
            .set_size_hint(raw.expected_isize as usize)
            .set_limit(raw.expected_isize as usize),
    );
    let payload = decoder
        .decode_deflate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("inflate: {e:?}")))?;
    if payload.len() != raw.expected_isize as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "BGZF ISIZE mismatch",
        ));
    }
    if crc32fast::hash(&payload) != raw.expected_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "BGZF CRC32 mismatch",
        ));
    }
    Ok(payload)
}

/// BGZF writer that parallelises the deflate compression across worker
/// threads while preserving block order. Output bytes are byte-identical to
/// `Writer` for the same input + level (each block is compressed
/// independently, so worker count never affects the bytes).
///
/// Architecture: the producer (Write impl) buffers up to BLOCK_SIZE bytes
/// and ships full blocks down `block_tx` with a monotonic sequence number.
/// `threads` workers pull (seq, payload), compress to (seq, encoded), and
/// send to the writer thread. The writer thread holds a min-heap keyed by
/// seq so it can emit blocks strictly in input order.
pub struct ParallelWriter {
    block_tx: Option<Sender<(u64, Vec<u8>)>>,
    pending: Vec<u8>,
    seq: u64,
    workers: Vec<JoinHandle<()>>,
    writer: Option<JoinHandle<io::Result<Vec<u64>>>>,
    finished: bool,
}

impl ParallelWriter {
    /// Spawn `threads` compression workers + 1 writer thread.
    /// `threads` < 1 is treated as 1.
    pub fn new<W: Write + Send + 'static>(inner: W, level: u8, threads: usize) -> Self {
        let threads = threads.max(1);
        // Bound channels to keep memory under control: at most ~threads blocks
        // in flight on each side, so peak memory ≈ 4 * threads * BLOCK_SIZE.
        let (block_tx, block_rx) = bounded::<(u64, Vec<u8>)>(threads * 2);
        let (out_tx, out_rx) = bounded::<(u64, Vec<u8>)>(threads * 2);

        let mut workers = Vec::with_capacity(threads);
        for _ in 0..threads {
            let rx = block_rx.clone();
            let tx = out_tx.clone();
            workers.push(std::thread::spawn(move || {
                compression_worker(rx, tx, level)
            }));
        }
        drop(out_tx); // writer thread holds the only remaining sender via channel.
        drop(block_rx); // workers hold the only remaining receivers.

        let writer =
            std::thread::spawn(move || -> io::Result<Vec<u64>> { ordering_writer(inner, out_rx) });

        Self {
            block_tx: Some(block_tx),
            pending: Vec::with_capacity(BLOCK_SIZE),
            seq: 0,
            workers,
            writer: Some(writer),
            finished: false,
        }
    }

    fn dispatch_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut block = Vec::with_capacity(BLOCK_SIZE);
        std::mem::swap(&mut block, &mut self.pending);
        self.pending.reserve(BLOCK_SIZE);
        let seq = self.seq;
        self.seq += 1;
        self.block_tx
            .as_ref()
            .expect("dispatch after finish")
            .send((seq, block))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "BGZF worker channel closed"))
    }

    /// Flush remaining bytes, drain workers, write the BGZF EOF block, and
    /// join all threads. Returns the writer thread's final I/O result.
    pub fn finish(self) -> io::Result<()> {
        self.finish_with_offsets().map(|_| ())
    }

    /// Like `finish` but also returns per-block compressed offsets for BAI/CSI.
    pub fn finish_with_offsets(mut self) -> io::Result<Vec<u64>> {
        self.dispatch_pending()?;
        drop(self.block_tx.take());
        for h in self.workers.drain(..) {
            h.join().expect("BGZF compression worker panicked");
        }
        let writer = self.writer.take().expect("finish called twice");
        let result = writer.join().expect("BGZF writer thread panicked");
        self.finished = true;
        result
    }
}

impl Write for ParallelWriter {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        if src.is_empty() {
            return Ok(0);
        }
        let space = BLOCK_SIZE - self.pending.len();
        let n = space.min(src.len());
        self.pending.extend_from_slice(&src[..n]);
        if self.pending.len() == BLOCK_SIZE {
            self.dispatch_pending()?;
        }
        Ok(n)
    }

    /// `flush` here means "ship any pending block to the workers" — it does
    /// NOT round-trip through the OS, since the workers are still draining
    /// in the background. To force everything out and close the BGZF stream,
    /// call `finish`.
    fn flush(&mut self) -> io::Result<()> {
        self.dispatch_pending()
    }
}

impl Drop for ParallelWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Best-effort flush + join. Errors are swallowed (Drop can't return).
        let _ = self.dispatch_pending();
        drop(self.block_tx.take());
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
        if let Some(w) = self.writer.take() {
            let _ = w.join();
        }
    }
}

fn compression_worker(rx: Receiver<(u64, Vec<u8>)>, tx: Sender<(u64, Vec<u8>)>, level: u8) {
    loop {
        let Ok((seq, payload)) = rx.recv() else {
            return;
        };
        let mut encoded = Vec::with_capacity(MAX_BLOCK_SIZE);
        encode_block(&payload, level, &mut encoded)
            .expect("BGZF encode_block failed (block exceeded 64KB after deflate?)");
        if tx.send((seq, encoded)).is_err() {
            return;
        }
    }
}

fn ordering_writer<W: Write>(mut inner: W, rx: Receiver<(u64, Vec<u8>)>) -> io::Result<Vec<u64>> {
    let mut next: u64 = 0;
    let mut pending: BinaryHeap<Reverse<(u64, Vec<u8>)>> = BinaryHeap::new();
    let mut block_offsets: Vec<u64> = Vec::new();
    let mut current_offset: u64 = 0;
    while let Ok(item) = rx.recv() {
        pending.push(Reverse(item));
        while let Some(Reverse((seq, _))) = pending.peek() {
            if *seq != next {
                break;
            }
            let Reverse((_, bytes)) = pending.pop().unwrap();
            inner.write_all(&bytes)?;
            block_offsets.push(current_offset);
            current_offset += bytes.len() as u64;
            next += 1;
        }
    }
    debug_assert!(pending.is_empty(), "writer exited with pending blocks");
    inner.write_all(&EOF_BLOCK)?;
    inner.flush()?;
    Ok(block_offsets)
}

fn encode_block(payload: &[u8], level: u8, out: &mut Vec<u8>) -> io::Result<()> {
    debug_assert!(payload.len() <= BLOCK_SIZE);
    let compressed = miniz_oxide::deflate::compress_to_vec(payload, level);
    let block_size = HEADER_LEN + compressed.len() + TRAILER_LEN;
    if block_size > MAX_BLOCK_SIZE {
        // Worst-case overflow: payload is incompressible *and* expands.
        // Spec response is to fall back to a "store" block, but with payload
        // capped at BLOCK_SIZE (0xff00) miniz_oxide level>=1 won't exceed
        // 0x10000. Bail loudly if we ever hit this so we notice.
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "BGZF block exceeds 64KB after compression",
        ));
    }

    out.clear();
    out.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x04]); // gzip magic, deflate, FEXTRA
    out.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // MTIME = 0
    out.extend_from_slice(&[0x00, 0xff]); // XFL=0, OS=unknown
    out.extend_from_slice(&[0x06, 0x00]); // XLEN = 6
    out.extend_from_slice(&[b'B', b'C']); // SI1, SI2
    out.extend_from_slice(&[0x02, 0x00]); // SLEN = 2
    out.extend_from_slice(&((block_size - 1) as u16).to_le_bytes()); // BSIZE
    out.extend_from_slice(&compressed);
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(payload: &[u8], level: u8) -> Vec<u8> {
        let mut compressed = Vec::new();
        {
            let mut w = Writer::new(&mut compressed, level);
            w.write_all(payload).unwrap();
            w.finish().unwrap();
        }
        let mut decompressed = Vec::new();
        let mut r = Reader::new(Cursor::new(&compressed));
        r.read_to_end(&mut decompressed).unwrap();
        decompressed
    }

    #[test]
    fn roundtrip_empty() {
        assert_eq!(roundtrip(&[], 6), Vec::<u8>::new());
    }

    #[test]
    fn roundtrip_small() {
        let p = b"hello, world\n".repeat(100);
        assert_eq!(roundtrip(&p, 6), p);
    }

    #[test]
    fn roundtrip_multiblock() {
        // Force >1 block: 200 KiB of pseudo-random-ish data.
        let p: Vec<u8> = (0u32..200_000)
            .map(|i| (i.wrapping_mul(1103515245).wrapping_add(12345) >> 16) as u8)
            .collect();
        assert_eq!(roundtrip(&p, 6), p);
    }

    #[test]
    fn roundtrip_block_boundary() {
        // Exactly one block worth.
        let p = vec![0xab; BLOCK_SIZE];
        assert_eq!(roundtrip(&p, 6), p);
    }

    #[test]
    fn roundtrip_levels() {
        let p = b"AAAACCCCGGGGTTTTNNNN".repeat(1000);
        for level in [0u8, 1, 6, 9] {
            assert_eq!(roundtrip(&p, level), p, "level {level}");
        }
    }

    #[test]
    fn read_test_bam_then_reencode_roundtrip() {
        // Read the canned test BAM with our Reader, get uncompressed bytes A.
        // Compress A with our Writer, decompress, get B. A must equal B.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/small_unsorted.bam");
        let bytes = std::fs::read(path).unwrap();
        let mut a = Vec::new();
        Reader::new(Cursor::new(&bytes))
            .read_to_end(&mut a)
            .unwrap();
        assert!(!a.is_empty(), "test BAM decompressed to nothing");
        // Sanity: BAM magic.
        assert_eq!(&a[..4], b"BAM\x01");
        let b = roundtrip(&a, 6);
        assert_eq!(a, b);
    }

    #[test]
    fn parallel_reader_byte_identical_to_single_threaded() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/data/medium_unsorted.bam"
        );
        let bytes = std::fs::read(path).unwrap();

        let mut single = Vec::new();
        Reader::new(Cursor::new(bytes.clone()))
            .read_to_end(&mut single)
            .unwrap();

        for threads in [1usize, 2, 4, 8] {
            let mut parallel = Vec::new();
            ParallelReader::new(Cursor::new(bytes.clone()), threads)
                .read_to_end(&mut parallel)
                .unwrap();
            assert_eq!(parallel, single, "threads={threads}");
        }
    }

    #[test]
    fn parallel_reader_propagates_truncated_stream_error() {
        // Strip the EOF block and watch parallel reader raise UnexpectedEof.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/small_unsorted.bam");
        let mut bytes = std::fs::read(path).unwrap();
        // EOF_BLOCK is 28 bytes at the tail; remove it.
        bytes.truncate(bytes.len() - 28);
        let err = ParallelReader::new(Cursor::new(bytes), 2)
            .read_to_end(&mut Vec::new())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn dropping_parallel_reader_after_prefix_does_not_deadlock() {
        let payload: Vec<u8> = (0u32..1_000_000)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut encoded = Vec::new();
        {
            let mut w = Writer::new(&mut encoded, 1);
            w.write_all(&payload).unwrap();
            w.finish().unwrap();
        }

        let mut r = ParallelReader::new(Cursor::new(encoded), 4);
        let mut prefix = vec![0; 1024];
        r.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix, &payload[..1024]);
        drop(r);
    }

    #[test]
    fn parallel_writer_byte_identical_to_single_threaded() {
        // Per-block compression is independent of worker count, so the
        // bytes must match the single-threaded Writer for any thread count.
        let payload: Vec<u8> = (0u32..200_000)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();

        let mut single = Vec::new();
        {
            let mut w = Writer::new(&mut single, 6);
            w.write_all(&payload).unwrap();
            w.finish().unwrap();
        }
        for threads in [1usize, 2, 4, 8] {
            let mut buf = Vec::new();
            // ParallelWriter takes ownership of its inner W; use a shared Vec
            // via a helper Write that borrows our buffer.
            struct TakeVec<'a>(&'a mut Vec<u8>);
            impl<'a> Write for TakeVec<'a> {
                fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                    self.0.extend_from_slice(b);
                    Ok(b.len())
                }
                fn flush(&mut self) -> io::Result<()> {
                    Ok(())
                }
            }
            // ParallelWriter requires Send + 'static, so use an owned Vec
            // and pull bytes back via a channel.
            let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
            struct ChanWriter(std::sync::mpsc::Sender<Vec<u8>>);
            impl Write for ChanWriter {
                fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                    self.0
                        .send(b.to_vec())
                        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "send"))?;
                    Ok(b.len())
                }
                fn flush(&mut self) -> io::Result<()> {
                    Ok(())
                }
            }
            let w = ParallelWriter::new(ChanWriter(tx), 6, threads);
            // Inner writes happen on the writer thread, drained via rx after finish.
            let mut w = w;
            w.write_all(&payload).unwrap();
            w.finish().unwrap();
            while let Ok(chunk) = rx.recv() {
                buf.extend_from_slice(&chunk);
            }
            assert_eq!(buf, single, "threads={threads} produced different bytes");
            // Sanity: round-trip back through Reader.
            let mut dec = Vec::new();
            Reader::new(Cursor::new(&buf))
                .read_to_end(&mut dec)
                .unwrap();
            assert_eq!(dec, payload);
            let _ = &mut buf; // silence unused mut warning
        }
    }

    #[test]
    fn rejects_truncated_stream_without_eof_block() {
        // A valid BGZF block but no EOF marker.
        let mut compressed = Vec::new();
        encode_block(b"abc", 6, &mut compressed).unwrap();
        let mut r = Reader::new(Cursor::new(&compressed));
        let err = r.read_to_end(&mut Vec::new()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
