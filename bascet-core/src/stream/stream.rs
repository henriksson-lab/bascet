use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;

use bounded_integer::BoundedUsize;
use bytesize::ByteSize;
use rtrb::PushError;

use crate::spinpark_loop::SPINPARK_PARKS_BEFORE_WARN;
use crate::stream::DEFAULT_COUNTOF_BUFFERS;
use crate::*;

pub(crate) enum StreamState {
    Aligned,
    Spanning(ArenaSlice<u8>),
}
pub(crate) enum StreamBufferState {
    Available(ArenaSlice<u8>),
    Eof,
    Error(()),
}

pub struct Stream<P, D, C, M> {
    pub(crate) inner_decoder_arena_pool: Arc<ArenaPool<u8>>,
    pub(crate) inner_decoder_buffer_rx: rtrb::Consumer<StreamBufferState>,
    pub(crate) inner_decoder_thread: ManuallyDrop<JoinHandle<D>>,
    pub(crate) inner_decoder_flag_stop: Arc<AtomicBool>,
    pub(crate) inner_shutdown_barrier: Arc<Barrier>,

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
    ) -> Result<Self, ()> {
        let arc_decoder_arena_pool = Arc::new(ArenaPool::new(sizeof_decode_buffer, sizeof_decode_arena).unwrap());
        let arc_decoder_stop_flag = Arc::new(AtomicBool::new(false));
        let arc_shutdown_barrier = Arc::new(Barrier::new(2));
        let (handle, rx) = Self::spawn_decode_worker(
            with_decoder,
            countof_buffers,
            Arc::clone(&arc_decoder_stop_flag),
            Arc::clone(&arc_shutdown_barrier),
            Arc::clone(&arc_decoder_arena_pool),
        );
        Ok(Self {
            inner_decoder_arena_pool: Arc::clone(&arc_decoder_arena_pool),
            inner_decoder_buffer_rx: rx,
            inner_decoder_thread: ManuallyDrop::new(handle),
            inner_decoder_flag_stop: arc_decoder_stop_flag,
            inner_shutdown_barrier: arc_shutdown_barrier,
            inner_parser: with_parser,

            inner_state: StreamState::Aligned,
            inner_context: None,
            _phantom: std::marker::PhantomData,
        })
    }

    fn spawn_decode_worker(
        mut decoder: D,
        n_buffers: BoundedUsize<2, { usize::MAX }>,
        stop_flag: Arc<AtomicBool>,
        shutdown_barrier: Arc<Barrier>,
        arena_pool: Arc<ArenaPool<u8>>,
    ) -> (JoinHandle<D>, rtrb::Consumer<StreamBufferState>) {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || {
            while stop_flag.load(Ordering::Relaxed) == false {
                let size = decoder.sizeof_target_alloc();
                let mut buffer = arena_pool.alloc(size);
                let decode_result = decoder.decode_into(buffer.as_mut_slice());

                let buffer_status = match decode_result {
                    DecodeResult::Decoded(bytes_written) => {
                        StreamBufferState::Available(unsafe { buffer.truncate(bytes_written) })
                    }
                    DecodeResult::Eof => {
                        stop_flag.store(true, Ordering::Relaxed);
                        StreamBufferState::Eof
                    }
                    DecodeResult::Error(e) => {
                        stop_flag.store(true, Ordering::Relaxed);
                        StreamBufferState::Error(e)
                    }
                };

                let mut item = buffer_status;
                let mut spinpark_counter = 0;
                loop {
                    match tx.push(item) {
                        Ok(_) => break,
                        Err(PushError::Full(i)) => {
                            item = i;

                            spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                                &mut spinpark_counter,
                                "Decoder (push): pushing buffer_status (buffer full, consumer slow)",
                            );

                            if likely_unlikely::unlikely(stop_flag.load(Ordering::Relaxed) == true) {
                                match &item {
                                    StreamBufferState::Eof | StreamBufferState::Error(_) => {
                                        // Keep waiting, must push these before thread exits
                                    }
                                    StreamBufferState::Available(_) => {
                                        shutdown_barrier.wait();
                                        break;
                                    }
                                }
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
        self.inner_shutdown_barrier.wait();
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
