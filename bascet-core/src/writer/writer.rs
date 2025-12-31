use std::{io::Write, mem::ManuallyDrop, sync::{Arc, Barrier, atomic::{AtomicBool, Ordering}}, thread::JoinHandle};

use bounded_integer::BoundedUsize;
use bytesize::ByteSize;

use crate::{ArenaPool, ArenaSlice, DEFAULT_SIZEOF_ARENA, DEFAULT_SIZEOF_BUFFER, Encode, EncodeResult, writer::DEFAULT_COUNTOF_BUFFERS};

pub(crate) enum WriterEncodeBufferState {
    Available(ArenaSlice<u8>),
    Eof,
    Error(()),
}

pub struct Writer<S, E, W> {
    pub(crate) inner_serialiser: S,
    pub(crate) inner_serialiser_arena_pool: Arc<ArenaPool<u8>>,


    pub(crate) inner_encoder_arena_pool: Arc<ArenaPool<u8>>,
    pub(crate) inner_encoder_thread: ManuallyDrop<JoinHandle<E>>,
    
    pub(crate) inner_writer: W,
    
    pub(crate) inner_shutdown_flag: Arc<AtomicBool>,
    pub(crate) inner_shutdown_barrier: Arc<Barrier>,

}

#[bon::bon]
impl<S, E, W> Writer<S, E, W>
where
    E: Encode + Send + 'static,
    W: Write + Send + 'static
{
    #[builder]
    pub fn new(
        with_encoder: E,
        with_serialiser: S,
        with_writer: W,
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_encode_buffer: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_ARENA)] sizeof_encode_arena: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_BUFFER)] sizeof_serialise_buffer: ByteSize,
        #[builder(default = DEFAULT_SIZEOF_ARENA)] sizeof_serialise_arena: ByteSize,
        #[builder(default = DEFAULT_COUNTOF_BUFFERS)] countof_buffers: BoundedUsize<
            2,
            { usize::MAX },
        >,
    ) -> Result<Self, ()> {
        let arc_encoder_arena_pool = Arc::new(ArenaPool::new(sizeof_encode_buffer, sizeof_encode_arena).unwrap());
        let arc_serialiser_arena_pool = Arc::new(ArenaPool::new(sizeof_serialise_buffer, sizeof_serialise_arena).unwrap());
        let arc_encoder_stop_flag = Arc::new(AtomicBool::new(false));
        let arc_shutdown_barrier = Arc::new(Barrier::new(2));
        let (encode_handle, encode_rx) = Self::spawn_encode_worker(
            with_encoder,
            countof_buffers,
            Arc::clone(&arc_encoder_arena_pool),
            Arc::clone(&arc_encoder_stop_flag),
            Arc::clone(&arc_shutdown_barrier),
        );
        let write_handle = Self::spawn_write_worker(
            with_writer,
            encode_rx,
            Arc::clone(&arc_encoder_stop_flag),
            Arc::clone(&arc_shutdown_barrier),
        );
        Ok(Self {
            inner_serialiser: with_serialiser,
            inner_serialiser_arena_pool: Arc::clone(&arc_serialiser_arena_pool),
            inner_encoder_arena_pool: Arc::clone(&arc_encoder_arena_pool),
            inner_encoder_thread: ManuallyDrop::new(write_handle),
            inner_writer: with_writer,
            inner_shutdown_flag: arc_encoder_stop_flag,
            inner_shutdown_barrier: arc_shutdown_barrier,
        })
    }

    fn spawn_encode_worker(
        mut encoder: E,
        countof_buffers: BoundedUsize<2, { usize::MAX }>,
        arc_arena_pool: Arc<ArenaPool<u8>>,
        arc_shutdown_flag: Arc<AtomicBool>,
        arc_shutdown_barrier: Arc<Barrier>,
    ) -> (JoinHandle<E>, rtrb::Consumer<WriterEncodeBufferState>) {
        let (mut tx, rx) = rtrb::RingBuffer::new(countof_buffers.get());

        let handle = std::thread::spawn(move || {
            while arc_shutdown_flag.load(Ordering::Relaxed) == false {
                let size = encoder.sizeof_target_alloc();
                let mut buffer = arc_arena_pool.alloc(size);
                let encode_result = encoder.encode_into(buffer.as_mut_slice());

                let buffer_status = match encode_result {
                    EncodeResult::Encoded(bytes_written) => {
                        StreamBufferState::Available(unsafe { buffer.truncate(bytes_written) })
                    }
                    EncodeResult::Eof => {
                        arc_shutdown_flag.store(true, Ordering::Relaxed);
                        StreamBufferState::Eof
                    }
                    EncodeResult::Error(e) => {
                        arc_shutdown_flag.store(true, Ordering::Relaxed);
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

                            if arc_shutdown_flag.load(Ordering::Relaxed) == true {
                                match &item {
                                    StreamBufferState::Eof | StreamBufferState::Error(_) => {
                                        // Keep waiting, must push these before thread exits
                                    }
                                    StreamBufferState::Available(_) => {
                                        arc_shutdown_barrier.wait();
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            encoder
        });

        (handle, rx)
    }

    fn spawn_write_worker(
        mut writer: W,
        encode_rx: rtrb::Consumer<WriterEncodeBufferState>,
        arc_shutdown_flag: Arc<AtomicBool>,
        arc_shutdown_barrier: Arc<Barrier>,
    ) -> JoinHandle<W> {
        let handle = std::thread::spawn(move || {
            writer
        });
        handle
    }
}

// impl<P, D, C, M> Drop for Stream<P, D, C, M> {
//     fn drop(&mut self) {
//         // SAFETY: drop is only called once
//         let handle = unsafe { ManuallyDrop::take(&mut self.inner_decoder_thread) };
//         handle.join().expect("Couldn't join on the Decode thread");
//     }
// }