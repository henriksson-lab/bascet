use std::{
    ffi::c_int,
    io::{Seek, Write},
    sync::Arc,
    thread::JoinHandle,
};

use bascet_core::{
    channel::{OrderedReceiver, OrderedSender},
    ArenaPool, DEFAULT_SIZEOF_BUFFER,
};

use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use cloudflare_zlib_sys as zlib;
use crossbeam::channel::{Receiver, Sender};
use flate2::Compression;

use crate::{
    codec::bbgz::{BBGZHeader, MAX_SIZEOF_BLOCK, MAX_SIZEOF_BLOCKusize, MARKER_EOF},
    BBGZCompressedBlock, BBGZRawBlock, BBGZTrailer, BBGZWriteBlock,
};

pub struct BBGZCompressionJob {
    pub header: BBGZHeader,
    pub raw: BBGZRawBlock,
}

pub struct BBGZCompressionResult {
    pub header: BBGZHeader,
    pub raw: BBGZRawBlock,
    pub compressed: BBGZCompressedBlock,
}

pub struct BBGZWriter {
    pub(crate) inner_raw_allocator: Arc<ArenaPool<u8>>,
    pub(crate) inner_compression_allocator: Arc<ArenaPool<u8>>,

    pub(crate) inner_compression_key: usize,
    pub(crate) inner_compression_level: Compression,
    pub(crate) inner_compression_tx: Sender<(usize, BBGZCompressionJob)>,
    pub(crate) inner_compression_workers: Vec<JoinHandle<()>>,

    pub(crate) inner_write_worker: JoinHandle<()>,
}

#[bon::bon]
impl BBGZWriter {
    #[builder]
    pub fn new<W>(
        with_writer: W,
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_raw_buffer: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_compression_buffer: ByteSize,
        #[builder(default = BoundedU64::const_new::<1>())] countof_threads: BoundedU64<
            1,
            { u64::MAX },
        >,
        #[builder(default = Compression::fast())] compression: Compression,
    ) -> Self
    where
        W: Write + Seek + Send + 'static,
    {
        let raw_allocator = Arc::new(ArenaPool::new(sizeof_raw_buffer, MAX_SIZEOF_BLOCK));
        // NOTE: compression workers calculate the size.
        // This should in theory be == deflate_compress_bound(raw.buf.len());
        // deflate_compress_bound can be slightly larger than input, so add headroom
        let compression_allocator =
            Arc::new(ArenaPool::new(sizeof_compression_buffer, MAX_SIZEOF_BLOCK));
        let compression_lvl = compression;

        let (compression_tx, compression_rx) = crossbeam::channel::unbounded();
        let (write_tx, write_rx) = bascet_core::channel::ordered::<BBGZCompressionResult, 16384>();

        let compression_workers = Self::spawn_compression_workers(
            Arc::clone(&compression_allocator),
            compression_rx,
            compression_lvl,
            write_tx,
            countof_threads,
        );

        let write_worker = Self::spawn_write_worker(with_writer, write_rx);

        return Self {
            inner_raw_allocator: raw_allocator,
            inner_compression_allocator: compression_allocator,
            inner_compression_key: 0,
            inner_compression_tx: compression_tx,
            inner_compression_workers: compression_workers,
            inner_compression_level: compression_lvl,
            inner_write_worker: write_worker,
        };
    }

    pub fn begin<'a>(&'a mut self, header: BBGZHeader) -> BBGZWriteBlock<'a> {
        BBGZWriteBlock::new(self, header)
    }

    pub(crate) fn alloc_raw(&mut self) -> BBGZRawBlock {
        // NOTE: usize_MAX_SIZEOF_BLOCK is the max LEN. alloc allocates n SLOTS.
        let buf = self.inner_raw_allocator.alloc(MAX_SIZEOF_BLOCKusize);
        BBGZRawBlock { buf, crc32: None }
    }

    /// SAFETY must ensure contracts for writing a block are met, i.e.: atomic writes only (no splitting across boundaries)
    pub(crate) unsafe fn submit_compress(&mut self, header: BBGZHeader, raw: BBGZRawBlock) {
        let _ = self.inner_compression_tx.send((
            self.inner_compression_key,
            BBGZCompressionJob {
                header: header,
                raw: raw,
            },
        ));
        self.inner_compression_key += 1;
    }

    fn spawn_compression_workers(
        compression_alloc: Arc<ArenaPool<u8>>,
        compression_rx: Receiver<(usize, BBGZCompressionJob)>,
        compression_lvl: Compression,
        write_tx: OrderedSender<BBGZCompressionResult, 16384>,
        countof_threads: BoundedU64<1, { u64::MAX }>,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for idx in 0..countof_threads.get() {
            let thread_compression_alloc = Arc::clone(&compression_alloc);
            let thread_compression_rx = compression_rx.clone();
            let thread_write_tx = write_tx.clone();

            handles.push(
                std::thread::Builder::new()
                    .name(format!("BBGZCompression@{}", idx))
                    .spawn(move || {
                        loop {
                            let (k, job) = match thread_compression_rx.recv() {
                                Ok(v) => v,
                                Err(_) => break,
                            };

                            let mut raw = job.raw;
                            let crc32 = crc32fast::hash(raw.buf.as_slice());
                            raw.crc32 = Some(crc32);

                            let mut buf_compressed =
                                thread_compression_alloc.alloc(MAX_SIZEOF_BLOCKusize);

                            let buf_compressed = {
                                // Use raw zlib API for precise control over deflate output
                                // Z_SYNC_FLUSH produces: [huffman data, all BFINAL=0] [sync: 00 00 00 ff ff]
                                let out_slice = buf_compressed.as_mut_slice();
                                let in_slice = raw.buf.as_slice();

                                let total_out = unsafe {
                                    let mut stream: zlib::z_stream = std::mem::zeroed();

                                    // Initialize for raw deflate (negative windowBits = no zlib/gzip header)
                                    let ret = zlib::deflateInit2(
                                        &mut stream,
                                        compression_lvl.level() as c_int,
                                        zlib::Z_DEFLATED,
                                        -15, // raw deflate, window size 32KB
                                        8,   // default memory level
                                        zlib::Z_DEFAULT_STRATEGY,
                                    );
                                    assert_eq!(ret, zlib::Z_OK, "deflateInit2 failed: {}", ret);

                                    stream.next_in = in_slice.as_ptr() as *mut u8;
                                    stream.avail_in = in_slice.len() as zlib::uInt;
                                    stream.next_out = out_slice.as_mut_ptr();
                                    stream.avail_out = out_slice.len() as zlib::uInt;

                                    // Compress with Z_SYNC_FLUSH to get sync marker and BFINAL=0
                                    let ret = zlib::deflate(&mut stream, zlib::Z_FULL_FLUSH);
                                    assert!(ret == zlib::Z_OK || ret == zlib::Z_STREAM_END,
                                        "deflate failed: {}", ret);

                                    let total_out = stream.total_out as usize;
                                    zlib::deflateEnd(&mut stream);
                                    total_out
                                };

                                // Verify sync marker is present
                                let sync_marker = &out_slice[total_out - 4..total_out];
                                if sync_marker != [0x00, 0x00, 0xff, 0xff] {
                                    eprintln!("[WARN] Unexpected sync marker: {:02x?}", sync_marker);
                                }

                                // Append empty fixed Huffman block with BFINAL=1: 03 00
                                // Structure: [huffman data] [sync: 00 00 00 ff ff] [terminator: 03 00]
                                // For merging: strip last 2 bytes from intermediate blocks
                                out_slice[total_out] = 0x03;
                                out_slice[total_out + 1] = 0x00;

                                unsafe { buf_compressed.truncate(total_out + 2) }
                            };

                            let sync_marker = &buf_compressed.as_slice()[(buf_compressed.len() - 6)..buf_compressed.len()];
                            if sync_marker != [0x00, 0x00, 0xff, 0xff, 0x03, 0x00] {
                                eprintln!("[WARN] Unexpected sync marker: {:02x?}", sync_marker);
                            }

                            let compressed = BBGZCompressedBlock {
                                buf: buf_compressed,
                            };
                            let job_result = BBGZCompressionResult {
                                header: job.header,
                                raw: raw,
                                compressed: compressed,
                            };

                            thread_write_tx.send(k, job_result);
                        }
                    })
                    .unwrap(),
            );
        }
        handles
    }

    fn spawn_write_worker<W>(
        mut writer: W,
        mut write_rx: OrderedReceiver<BBGZCompressionResult, 16384>,
    ) -> JoinHandle<()>
    where
        W: Write + Seek + Send + 'static,
    {
        std::thread::Builder::new()
            .name("BBGZWrite@0".to_string())
            .spawn(move || {
                loop {
                    let res = match write_rx.recv() {
                        Ok(r) => r,
                        Err(_) => break,
                    };

                    let mut header = res.header;
                    let raw = res.raw;
                    let compressed = res.compressed;
                    let trailer = unsafe {
                        BBGZTrailer::new(
                            // SAFETY: we set the crc32 to Some(crc32) when compressing
                            raw.crc32.unwrap_unchecked(),
                            raw.buf.len() as u32,
                        )
                    };

                    let _ = header.write_with_csize(&mut writer, compressed.buf.len());
                    let _ = writer.write_all(&compressed.buf.as_slice());
                    let _ = trailer.write_with(&mut writer);
                }

                let _ = writer.write_all(&MARKER_EOF);
                let _ = writer.flush();
            })
            .unwrap()
    }
}

impl Drop for BBGZWriter {
    fn drop(&mut self) {
        drop(std::mem::replace(
            &mut self.inner_compression_tx,
            crossbeam::channel::unbounded().0,
        ));

        while let Some(handle) = self.inner_compression_workers.pop() {
            handle.join().ok();
        }

        // Join the write worker after compression workers finish
        // This ensures all compressed data is written before dropping
        let write_handle = std::mem::replace(
            &mut self.inner_write_worker,
            std::thread::spawn(|| {}),
        );
        write_handle.join().ok();
    }
}
