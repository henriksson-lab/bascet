use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use bounded_integer::BoundedUsize;
use bytesize::ByteSize;
use rtrb::PushError;
use tracing::{error, warn};

use crate::stream::DEFAULT_COUNTOF_BUFFERS;
use crate::threading::spinpark_loop::{self, SPINPARK_COUNTOF_PARKS_BEFORE_WARN, SpinPark};
use crate::*;

pub(crate) enum StreamState {
    Aligned,
    Spanning(ArenaSlice<u8>),
}

pub struct Stream<P, D, C, M> {
    pub(crate) inner_decoder_allocator: Arc<ArenaPool<u8>>,
    pub(crate) inner_decoder_buffer_rx: rtrb::Consumer<ArenaSlice<u8>>,
    pub(crate) inner_decoder_thread: ManuallyDrop<JoinHandle<D>>,
    pub(crate) inner_decoder_flag_stop: Arc<AtomicBool>,

    pub(crate) inner_parser: P,

    pub(crate) inner_state: StreamState,
    pub(crate) inner_context: Option<C>,
    pub(crate) _phantom: std::marker::PhantomData<(C, M)>,
}

#[bon::bon]
impl<P, D, C, M> Stream<P, D, C, M>
where
    D: Decode + Send + 'static,
{
    #[builder]
    pub fn new(
        with_decoder: D,
        with_parser: P,
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_decode_buffer: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_ARENA)] sizeof_decode_arena: ByteSize,
        #[builder(default = DEFAULT_COUNTOF_BUFFERS)] countof_buffers: BoundedUsize<
            2,
            { usize::MAX },
        >,
        with_opt_decode_arena_pool: Option<Arc<ArenaPool<u8>>>,
    ) -> Self {
        let arc_decoder_arena_pool = if let Some(arena_pool) = with_opt_decode_arena_pool {
            arena_pool
        } else {
            Arc::new(ArenaPool::new(sizeof_decode_buffer, sizeof_decode_arena))
        };

        let arc_decoder_stop_flag = Arc::new(AtomicBool::new(false));
        let (handle, rx) = Self::spawn_decode_worker(
            with_decoder,
            countof_buffers,
            Arc::clone(&arc_decoder_stop_flag),
            Arc::clone(&arc_decoder_arena_pool),
        );
        Self {
            inner_decoder_allocator: Arc::clone(&arc_decoder_arena_pool),
            inner_decoder_buffer_rx: rx,
            inner_decoder_thread: ManuallyDrop::new(handle),
            inner_decoder_flag_stop: arc_decoder_stop_flag,
            inner_parser: with_parser,

            inner_state: StreamState::Aligned,
            inner_context: None,
            _phantom: std::marker::PhantomData,
        }
    }

    fn spawn_decode_worker(
        mut decoder: D,
        n_buffers: BoundedUsize<2, { usize::MAX }>,
        flag_shutdown: Arc<AtomicBool>,
        arena_pool: Arc<ArenaPool<u8>>,
    ) -> (JoinHandle<D>, rtrb::Consumer<ArenaSlice<u8>>) {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || {
            while likely_unlikely::likely(flag_shutdown.load(Ordering::Relaxed) == false) {
                let size = decoder.sizeof_target_alloc();
                let mut buffer = arena_pool.alloc(size);
                let decode_result = decoder.decode_into(buffer.as_mut_slice());

                let mut buffer = match decode_result {
                    DecodeResult::Decoded(bytes_written) => {
                        let buffer = unsafe { buffer.truncate(bytes_written) };
                        buffer
                    }
                    DecodeResult::Eof => {
                        flag_shutdown.store(true, Ordering::Relaxed);
                        continue;
                    }
                    DecodeResult::Error(e) => {
                        match e {
                            _ => error!(error = ?e, "Unhandled error"),
                        }
                        flag_shutdown.store(true, Ordering::Relaxed);
                        continue;
                    }
                };

                let mut spinpark_counter = 0;
                loop {
                    match tx.push(buffer) {
                        Ok(()) => break,
                        Err(PushError::Full(b)) => {
                            buffer = b;

                            if likely_unlikely::unlikely(
                                flag_shutdown.load(Ordering::Relaxed) == true,
                            ) {
                                break;
                            }

                            match spinpark_loop::spinpark_loop::<
                                100,
                                SPINPARK_COUNTOF_PARKS_BEFORE_WARN,
                            >(&mut spinpark_counter)
                            {
                                SpinPark::Warn => {
                                    warn!(source = "Stream::decode", "buffer full, consumer slow")
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            decoder
        });

        (handle, rx)
    }

    pub unsafe fn shutdown(mut self) {
        self.inner_decoder_flag_stop.store(true, Ordering::Relaxed);
        // HACK: make sure stop flag is read
        std::thread::sleep(std::time::Duration::from_secs(1));
        // self.inner_shutdown_barrier.wait();
        while let Ok(buffer) = self.inner_decoder_buffer_rx.pop() {
            drop(buffer);
        }
        self.inner_state = StreamState::Aligned;
        drop(self.inner_context.take());
    }
}

impl<P, D, C, M> Drop for Stream<P, D, C, M> {
    fn drop(&mut self) {
        // SAFETY: drop is only called once
        let handle = unsafe { ManuallyDrop::take(&mut self.inner_decoder_thread) };
        handle.join().expect("Couldn't join on the Decode thread");
    }
}
