use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use bounded_integer::BoundedUsize;
use bytesize::ByteSize;
use rtrb::PushError;

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
    pub(crate) inner_arena_pool: Arc<ArenaPool<u8>>,
    pub(crate) inner_buffer_rx: rtrb::Consumer<StreamBufferState>,
    pub(crate) inner_decoder_thread: ManuallyDrop<JoinHandle<D>>,
    pub(crate) inner_decoder_stop: Arc<AtomicBool>,

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
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_buffer: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_ARENA)] sizeof_arena: ByteSize,
        #[builder(default = DEFAULT_COUNTOF_BUFFERS)] countof_buffers: BoundedUsize<
            2,
            { usize::MAX },
        >,
    ) -> Result<Self, ()> {
        let arc_arena_pool = Arc::new(ArenaPool::new(sizeof_buffer, sizeof_arena).unwrap());
        let decoder_stop_flag = Arc::new(AtomicBool::new(false));
        let (handle, rx) = Self::spawn_decode_worker(
            with_decoder,
            countof_buffers,
            decoder_stop_flag.clone(),
            Arc::clone(&arc_arena_pool),
        );
        Ok(Self {
            inner_arena_pool: Arc::clone(&arc_arena_pool),
            inner_buffer_rx: rx,
            inner_decoder_thread: ManuallyDrop::new(handle),
            inner_decoder_stop: decoder_stop_flag,
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
        arena_pool: Arc<ArenaPool<u8>>,
    ) -> (JoinHandle<D>, rtrb::Consumer<StreamBufferState>) {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let size = decoder.sizeof_target_alloc();
                let mut buffer = arena_pool.alloc(size);
                let decode_result = decoder.decode_into(buffer.as_mut_slice());

                let buffer_status = match decode_result {
                    DecodeStatus::Decoded(bytes_written) => {
                        StreamBufferState::Available(unsafe { buffer.truncate(bytes_written) })
                    }
                    DecodeStatus::Eof => {
                        stop_flag.store(true, Ordering::Relaxed);
                        StreamBufferState::Eof
                    }
                    DecodeStatus::Error(e) => {
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
                            spinpark_loop::spinpark_loop::<100>(&mut spinpark_counter);
                        }
                    }
                }
            }

            decoder
        });

        (handle, rx)
    }
}

impl<P, D, C, M> Drop for Stream<P, D, C, M> {
    fn drop(&mut self) {
        self.inner_decoder_stop.store(true, Ordering::Relaxed);
        // SAFETY: drop is only called once
        let handle = unsafe { ManuallyDrop::take(&mut self.inner_decoder_thread) };
        handle.join().expect("Couldn't join on the Decode thread");
    }
}
