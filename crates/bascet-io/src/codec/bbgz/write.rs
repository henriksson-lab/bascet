use std::{
    io::{Seek, Write},
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use bascet_core::{
    ArenaPool, ArenaSlice, DEFAULT_SIZEOF_BUFFER,
    channel::{OrderedDenseReceiver, OrderedDenseSender},
};

use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use crossbeam::channel::{Receiver, Sender};
use flate2::{Compress as FlateCompress, FlushCompress, Status};

use crate::{
    BBGZTrailer, BBGZWriteBlock, Compression,
    codec::bbgz::{
        BBGZHeader, MARKER_EOF, MAX_SIZEOF_BLOCK, MAX_SIZEOF_BLOCKusize, MAX_SIZEOF_RAW_BLOCKusize,
    },
};

pub struct BBGZCompressionJob {
    pub header: BBGZHeader,
    pub raw: ArenaSlice<u8>,
}

pub struct BBGZCompressionResult {
    pub header: BBGZHeader,
    pub compressed: ArenaSlice<u8>,
    pub crc32: u32,
    pub isize: usize,
}

struct BbgzCompressor {
    inner: FlateCompress,
}

impl BbgzCompressor {
    fn new(compression_level: Compression) -> Self {
        Self {
            inner: FlateCompress::new(
                flate2::Compression::new(compression_level.level() as u32),
                false,
            ),
        }
    }

    fn compress_into(
        &mut self,
        raw: &mut ArenaSlice<u8>,
        compression_alloc: &ArenaPool<u8>,
    ) -> ArenaSlice<u8> {
        let mut compressed = compression_alloc.alloc(MAX_SIZEOF_BLOCKusize);

        let compressed = {
            let slice_raw = raw.as_mut_slice();
            let slice_compressed = compressed.as_mut_slice();

            // SyncFlush finishes the current deflate block, then appends a
            // non-final empty stored block. zlib-rs implements that as:
            // [alignment bits] [LEN=0] [NLEN=!0], i.e. 00 00 ff ff after
            // byte alignment.
            let status = self
                .inner
                .compress(slice_raw, slice_compressed, FlushCompress::Sync)
                .expect("deflate failed");
            assert!(
                matches!(status, Status::Ok | Status::BufError),
                "unexpected deflate status: {status:?}"
            );
            assert_eq!(
                self.inner.total_in() as usize,
                slice_raw.len(),
                "deflate did not consume the full BBGZ block"
            );

            let total_out = self.inner.total_out() as usize;
            assert!(
                total_out + 2 <= slice_compressed.len(),
                "compressed BBGZ block exceeded output buffer"
            );
            self.inner.reset();

            // Append an empty final fixed-Huffman block. This emits no
            // uncompressed bytes, but makes this BBGZ block a complete deflate
            // stream. Shard merging depends on this exact contract: it strips
            // 03 00 from intermediate compressed payloads, keeps the SyncFlush
            // boundaries, and appends one final 03 00 to the merged payload.
            slice_compressed[total_out] = 0x03;
            slice_compressed[total_out + 1] = 0x00;

            unsafe { compressed.truncate(total_out + 2) }
        };

        compressed
    }
}

struct InFlightLimiter {
    available: Mutex<usize>,
    ready: Condvar,
}

impl InFlightLimiter {
    fn new(cap: usize) -> Self {
        Self {
            available: Mutex::new(cap.max(1)),
            ready: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> InFlightPermit {
        let mut available = self.available.lock().unwrap();
        while *available == 0 {
            available = self.ready.wait(available).unwrap();
        }
        *available -= 1;

        InFlightPermit {
            limiter: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut available = self.available.lock().unwrap();
        *available += 1;
        self.ready.notify_one();
    }
}

struct InFlightPermit {
    limiter: Arc<InFlightLimiter>,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

pub struct BBGZWriter {
    pub(crate) inner_raw_allocator: Arc<ArenaPool<u8>>,
    #[allow(unused)]
    pub(crate) inner_compression_allocator: Arc<ArenaPool<u8>>,

    pub(crate) inner_compression_key: usize,
    #[allow(unused)]
    pub(crate) inner_compression_level: Compression,
    pub(crate) inner_compression_tx: Sender<(usize, BBGZCompressionJob)>,
    pub(crate) inner_compression_workers: Vec<JoinHandle<()>>,

    pub(crate) inner_write_worker: JoinHandle<()>,
}

pub struct BBGZFinishHandle {
    inner: JoinHandle<()>,
}

impl BBGZFinishHandle {
    pub fn join(self) -> std::thread::Result<()> {
        self.inner.join()
    }
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
        #[builder(default = Compression::balanced())] compression_level: Compression,
        with_opt_raw_arena_pool: Option<Arc<ArenaPool<u8>>>,
        with_opt_compression_arena_pool: Option<Arc<ArenaPool<u8>>>,
        with_opt_rayon_pool: Option<Arc<rayon::ThreadPool>>,
    ) -> Self
    where
        W: Write + Seek + Send + 'static,
    {
        let raw_allocator = if let Some(arena_pool) = with_opt_raw_arena_pool {
            arena_pool
        } else {
            Arc::new(ArenaPool::new(sizeof_raw_buffer, MAX_SIZEOF_BLOCK))
        };
        let compression_allocator = if let Some(arena_pool) = with_opt_compression_arena_pool {
            arena_pool
        } else {
            Arc::new(ArenaPool::new(sizeof_compression_buffer, MAX_SIZEOF_BLOCK))
        };

        let effective_countof_threads = with_opt_rayon_pool
            .as_ref()
            .map(|pool| pool.current_num_threads())
            .unwrap_or_else(|| countof_threads.get() as usize)
            .max(1);
        let compression_queue_capacity = effective_countof_threads.saturating_mul(4).max(1);
        let (compression_tx, compression_rx) =
            crossbeam::channel::bounded(compression_queue_capacity);
        let (write_tx, write_rx) =
            bascet_core::channel::ordered_dense::<BBGZCompressionResult, 16384>();

        let compression_workers = if let Some(rayon_pool) = with_opt_rayon_pool {
            Self::spawn_rayon_compression_dispatcher(
                Arc::clone(&compression_allocator),
                compression_rx,
                compression_level,
                write_tx,
                rayon_pool,
            )
        } else {
            Self::spawn_compression_workers(
                Arc::clone(&compression_allocator),
                compression_rx,
                compression_level,
                write_tx,
                countof_threads,
            )
        };

        let write_worker = Self::spawn_write_worker(with_writer, write_rx);

        return Self {
            inner_raw_allocator: raw_allocator,
            inner_compression_allocator: compression_allocator,
            inner_compression_key: 0,
            inner_compression_tx: compression_tx,
            inner_compression_workers: compression_workers,
            inner_compression_level: compression_level,
            inner_write_worker: write_worker,
        };
    }

    pub fn begin<'a>(&'a mut self, header: BBGZHeader) -> BBGZWriteBlock<'a> {
        BBGZWriteBlock::new(self, header)
    }

    pub fn finish_async(self) -> BBGZFinishHandle {
        let handle = std::thread::Builder::new()
            .name("BBGZFinish@0".to_string())
            .spawn(move || drop(self))
            .unwrap();
        BBGZFinishHandle { inner: handle }
    }

    pub(crate) fn alloc_raw(&mut self) -> ArenaSlice<u8> {
        // Use the raw payload ceiling, not the container ceiling. Otherwise an
        // unlucky high-entropy block can compress larger than the remaining
        // BBGZ space and leave the ordered writer waiting for a failed block.
        let buf = self.inner_raw_allocator.alloc(MAX_SIZEOF_RAW_BLOCKusize);
        buf
    }

    /// SAFETY must ensure contracts for writing a block are met, i.e.: atomic writes only (no splitting across boundaries)
    pub(crate) unsafe fn submit_compress(&mut self, job: BBGZCompressionJob) {
        let _ = self
            .inner_compression_tx
            .send((self.inner_compression_key, job));
        self.inner_compression_key += 1;
    }

    fn spawn_compression_workers(
        compression_alloc: Arc<ArenaPool<u8>>,
        compression_rx: Receiver<(usize, BBGZCompressionJob)>,
        compression_level: Compression,
        write_tx: OrderedDenseSender<BBGZCompressionResult, 16384>,
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
                        let mut thread_compressor = BbgzCompressor::new(compression_level);

                        loop {
                            let (k, job) = match thread_compression_rx.recv() {
                                Ok(v) => v,
                                Err(_) => break,
                            };

                            let mut buf_raw = job.raw;
                            let crc32_raw = crc32fast::hash(buf_raw.as_slice());
                            let buf_compressed = thread_compressor
                                .compress_into(&mut buf_raw, &thread_compression_alloc);

                            let job_result = BBGZCompressionResult {
                                header: job.header,
                                compressed: buf_compressed,
                                crc32: crc32_raw,
                                isize: buf_raw.len(),
                            };

                            thread_write_tx.send(k, job_result);
                        }
                    })
                    .unwrap(),
            );
        }
        handles
    }

    fn spawn_rayon_compression_dispatcher(
        compression_alloc: Arc<ArenaPool<u8>>,
        compression_rx: Receiver<(usize, BBGZCompressionJob)>,
        compression_level: Compression,
        write_tx: OrderedDenseSender<BBGZCompressionResult, 16384>,
        rayon_pool: Arc<rayon::ThreadPool>,
    ) -> Vec<JoinHandle<()>> {
        let handle = std::thread::Builder::new()
            .name("BBGZCompressionDispatch@0".to_string())
            .spawn(move || {
                let inflight_limiter =
                    Arc::new(InFlightLimiter::new(rayon_pool.current_num_threads()));
                rayon_pool.scope(|scope| {
                    while let Ok((k, job)) = compression_rx.recv() {
                        let permit = inflight_limiter.acquire();
                        let task_compression_alloc = Arc::clone(&compression_alloc);
                        let task_write_tx = write_tx.clone();

                        scope.spawn(move |_| {
                            let _permit = permit;
                            let mut compressor = BbgzCompressor::new(compression_level);
                            let mut buf_raw = job.raw;
                            let crc32_raw = crc32fast::hash(buf_raw.as_slice());
                            let buf_compressed =
                                compressor.compress_into(&mut buf_raw, &task_compression_alloc);

                            let job_result = BBGZCompressionResult {
                                header: job.header,
                                compressed: buf_compressed,
                                crc32: crc32_raw,
                                isize: buf_raw.len(),
                            };

                            task_write_tx.send(k, job_result);
                        });
                    }
                });
            })
            .unwrap();

        vec![handle]
    }

    fn spawn_write_worker<W>(
        mut writer: W,
        mut write_rx: OrderedDenseReceiver<BBGZCompressionResult, 16384>,
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
                    let compressed = res.compressed;
                    let trailer = BBGZTrailer::new(res.crc32, res.isize.try_into().unwrap());

                    let _ = header.write_with_csize(&mut writer, compressed.len());
                    let _ = writer.write_all(&compressed.as_slice());
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
        let write_handle =
            std::mem::replace(&mut self.inner_write_worker, std::thread::spawn(|| {}));
        write_handle.join().ok();
    }
}
