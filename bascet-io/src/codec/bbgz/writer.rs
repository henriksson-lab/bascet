use std::{
    cell::UnsafeCell,
    io::{Seek, Write},
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

use bascet_core::{
    spinpark_loop::{self, SPINPARK_PARKS_BEFORE_WARN},
    ArenaPool, SendPtr,
};
use binrw::BinWrite;
use bounded_integer::{BoundedI32, BoundedU64};
use crossbeam::channel::{Receiver, Sender};
use libdeflater::{CompressionLvl, Compressor};

use crate::{
    bbgz::{usize_MAX_SIZEOF_BLOCK, usize_MIN_SIZEOF_BLOCK, BBGZHeader},
    BBGZTrailer,
};

pub struct BBGZBlockRaw {
    pub(crate) data: &'static [u8],
    pub(crate) crc32: u32,
}
#[repr(transparent)]
pub struct BBGZBlockCompressed {
    pub(crate) data: Vec<u8>,
}

type BBGZCompressionCell = UnsafeCell<MaybeUninit<(BBGZBlockRaw, BBGZBlockCompressed)>>;

pub struct BBGZWriter<W> {
    pub(crate) inner: W,
    pub(crate) inner_allocator: Arc<ArenaPool<u8>>,

    pub(crate) inner_compression_level: CompressionLvl,
    pub(crate) inner_compression_tx:
        Sender<(BBGZBlockRaw, SendPtr<(BBGZCompressionCell, AtomicBool)>)>,
    pub(crate) inner_compression_rx:
        Receiver<(BBGZBlockRaw, SendPtr<(BBGZCompressionCell, AtomicBool)>)>,
    pub(crate) inner_compression_workers: Vec<JoinHandle<()>>,
}

#[bon::bon]
impl<W> BBGZWriter<W> {
    #[builder]
    pub fn new(
        with_writer: W,
        #[builder(default = BoundedU64::const_new::<1>())] countof_threads: BoundedU64<
            1,
            { u64::MAX },
        >,
        #[builder(default = Compression::balanced())] compression: Compression,
    ) -> Result<Self, ()> {
        let (tx, rx) = crossbeam::channel::unbounded();
        let compression_lvl = CompressionLvl::from(compression);
        let compression_workers =
            Self::spawn_compression_workers(compression_lvl, rx.clone(), countof_threads);

        let bbgz = Self {
            inner: with_writer,
            inner_compression_level: compression_lvl,
            inner_compression_tx: tx,
            inner_compression_rx: rx,
            inner_compression_workers: compression_workers,
        };

        Ok(bbgz)
    }

    pub unsafe fn write_block(&mut self, block: &[u8], header: &mut BBGZHeader)
    where
        W: Write + Seek,
    {
        let sizeof_buf = block.len();
        let countof_blocks = (sizeof_buf + usize_MAX_SIZEOF_BLOCK - 1) / usize_MAX_SIZEOF_BLOCK;
        let opt_sizeof_blocks = ((sizeof_buf + countof_blocks - 1) / countof_blocks)
            .clamp(usize_MIN_SIZEOF_BLOCK, usize_MAX_SIZEOF_BLOCK);

        let chunks = block.chunks(opt_sizeof_blocks);
        let countof_blocks = chunks.len();

        let vec_collector: Vec<(BBGZCompressionCell, AtomicBool)> = (0..countof_blocks)
            .map(|_| {
                (
                    UnsafeCell::new(MaybeUninit::uninit()),
                    AtomicBool::new(false),
                )
            })
            .collect();

        let ptr = vec_collector.as_ptr();

        for (pid, chunk) in chunks.enumerate() {
            let block = BBGZBlockRaw {
                data: std::mem::transmute(chunk),
                crc32: 0, // Will be calculated in worker
            };
            let slot =
                SendPtr::new_unchecked(ptr.add(pid) as *mut (BBGZCompressionCell, AtomicBool));
            self.inner_compression_tx.send((block, slot));
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        for pid in 0..countof_blocks {
            let (slot, is_ready) = &vec_collector[pid];

            let mut spinpark_counter = 0;
            loop {
                if is_ready.load(Ordering::Relaxed) == true {
                    break;
                }
                spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                    &mut spinpark_counter,
                    "Consumer (BBGZ encode): waiting for data (compressor slow)",
                );
            }

            let (block_uncompressed, block_compressed) = (*slot.get()).assume_init_read();
            let trailer = BBGZTrailer::new(
                block_uncompressed.crc32,
                block_uncompressed.data.len() as u32,
            );

            header.write_header(&mut self.inner, block_compressed.data.len());
            self.inner.write_all(&block_compressed.data);
            trailer.write_trailer(&mut self.inner);
        }
    }

    fn spawn_compression_workers(
        compression_lvl: CompressionLvl,
        compression_rx: Receiver<(BBGZBlockRaw, SendPtr<(BBGZCompressionCell, AtomicBool)>)>,
        countof_threads: BoundedU64<1, { u64::MAX }>,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for idx in 0..countof_threads.get() {
            let thread_compression_rx = compression_rx.clone();
            let mut thread_compressor = Compressor::new(compression_lvl);

            handles.push(
                std::thread::Builder::new()
                    .name(format!("BBGZCompression@{}", idx))
                    .spawn(move || {
                        while let Ok((mut block_uncompressed, ptr_slot)) =
                            thread_compression_rx.recv()
                        {
                            let crc32 = crc32fast::hash(block_uncompressed.data);
                            block_uncompressed.crc32 = crc32;

                            let sizeof_alloc_needed = thread_compressor
                                .deflate_compress_bound(block_uncompressed.data.len());
                            let mut buf_compressed_assume_uninit: Vec<MaybeUninit<u8>> =
                                vec![MaybeUninit::uninit(); sizeof_alloc_needed];

                            let sizeof_alloc = {
                                // SAFETY: MaybeUninit is zero sized and has the same memory layout as u8
                                let buf_compressed_assume_uninit_u8 = unsafe {
                                    std::mem::transmute(buf_compressed_assume_uninit.as_mut_slice())
                                };
                                // SAFETY: we always allocate as many bytes as uncompressed data needs therefore this cannot fail
                                thread_compressor
                                    .deflate_compress(
                                        block_uncompressed.data,
                                        buf_compressed_assume_uninit_u8,
                                    )
                                    .unwrap()
                            };
                            buf_compressed_assume_uninit.truncate(sizeof_alloc);

                            let vec_compressed_assume_init_u8 = unsafe {
                                // SAFETY: MaybeUninit is zero sized and has the same memory layout as u8
                                std::mem::transmute::<Vec<MaybeUninit<u8>>, Vec<u8>>(
                                    buf_compressed_assume_uninit,
                                )
                            };

                            let block_compressed = BBGZBlockCompressed {
                                data: vec_compressed_assume_init_u8,
                            };

                            unsafe {
                                let (slot, is_ready) = &*ptr_slot.as_ref();
                                (*slot.get()).write((block_uncompressed, block_compressed));
                                is_ready.store(true, Ordering::Relaxed);
                            }
                        }
                    })
                    .unwrap(),
            );
        }
        handles
    }
}

impl<W> Drop for BBGZWriter<W> {
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
