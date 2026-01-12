use std::{
    io::{Seek, Write},
    sync::{
        Arc,
    },
    thread::JoinHandle,
    time::UNIX_EPOCH,
};

use bascet_core::{
    ArenaPool, DEFAULT_SIZEOF_BUFFER, channel::{OrderedReceiver, OrderedSender}
};

use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use crossbeam::channel::{Receiver, Sender};
use libdeflater::{CompressionLvl, Compressor};

use crate::{
    bbgz::{
        usize_MAX_SIZEOF_BLOCK, BBGZHeader, Compression, MARKER_EOF,
        MAX_SIZEOF_BLOCK,
    },
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
    pub(crate) inner_compression_level: CompressionLvl,
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
        #[builder(default = Compression::fastest())] compression: Compression,
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
        let compression_lvl = CompressionLvl::from(compression);

        let (compression_tx, compression_rx) = 
            crossbeam::channel::unbounded();
        let (write_tx, write_rx) = 
            bascet_core::channel::ordered::<BBGZCompressionResult, 4096>();

        let compression_workers = Self::spawn_compression_workers(
            Arc::clone(&compression_allocator),
            compression_rx,
            compression_lvl,
            write_tx,
            countof_threads,
        );

        let write_worker = Self::spawn_write_worker(
            with_writer,
            write_rx
        );

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
        let buf = self.inner_raw_allocator.alloc(usize_MAX_SIZEOF_BLOCK - 1);
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
        compression_lvl: CompressionLvl,
        write_tx: OrderedSender<BBGZCompressionResult, 4096>,
        countof_threads: BoundedU64<1, { u64::MAX }>,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for idx in 0..countof_threads.get() {
            let thread_compression_alloc = Arc::clone(&compression_alloc);
            let thread_compression_rx = compression_rx.clone();
            let thread_write_tx = write_tx.clone();
            let mut thread_compressor = Compressor::new(compression_lvl);

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
                                thread_compression_alloc.alloc(usize_MAX_SIZEOF_BLOCK);

                            let buf_compressed = unsafe {
                                // SAFETY: we always allocate as many bytes as uncompressed data needs therefore this cannot fail
                                let sizeof_alloc = thread_compressor
                                    .deflate_compress(
                                        raw.buf.as_slice(),
                                        buf_compressed.as_mut_slice(),
                                    )
                                    .unwrap_unchecked();
                                buf_compressed.truncate(sizeof_alloc)
                            };

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
        mut write_rx: OrderedReceiver<BBGZCompressionResult, 4096>,
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

                    let _ = header.write_header(&mut writer, compressed.buf.len());
                    let _ = writer.write_all(&compressed.buf.as_slice());
                    let _ = trailer.write_trailer(&mut writer);
                }

                let _ = writer.write_all(MARKER_EOF);
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
    }
}
