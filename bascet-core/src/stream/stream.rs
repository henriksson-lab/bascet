use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use bounded_integer::BoundedUsize;
use bytesize::ByteSize;
use rtrb::{PeekError, PushError};

use crate::{spinpark_loop, ArenaPool, ArenaSlice, DecodeStatus, ParseStatus};

enum BufferStatus {
    Available(ArenaSlice<u8>),
    Eof,
    Error(()),
}

enum StreamStatus {
    Aligned,
    Spanning(ArenaSlice<u8>),
}

pub struct Stream<D, P> {
    inner_arena_pool: Arc<ArenaPool<u8>>,
    inner_buffer_rx: rtrb::Consumer<BufferStatus>,
    inner_decoder_thread: ManuallyDrop<JoinHandle<D>>,
    inner_decoder_stop: Arc<AtomicBool>,

    inner_parser: P,
    inner_status: StreamStatus,
}

#[bon::bon]
impl<D, P> Stream<D, P>
where
    D: crate::Decode,
    D: Send + 'static,
{
    #[builder]
    pub fn new(
        decoder: D,
        parser: P,
        #[builder(default = ByteSize::mib(64))] //
        sizeof_buffer: ByteSize, //
        #[builder(default = ByteSize::mib(4))] //
        sizeof_arena: ByteSize, //
        #[builder(default = BoundedUsize::<2, { usize::MAX }>::new(2).unwrap())]
        n_buffers: BoundedUsize<2, { usize::MAX }>,
    ) -> Result<Self, ()> {
        let arc_arena_pool = Arc::new(ArenaPool::new(sizeof_buffer, sizeof_arena).unwrap());
        let decoder_stop_flag = Arc::new(AtomicBool::new(false));
        let (handle, rx) = Self::spawn_decode_worker(
            decoder,
            n_buffers,
            decoder_stop_flag.clone(),
            Arc::clone(&arc_arena_pool),
        );
        Ok(Self {
            inner_arena_pool: Arc::clone(&arc_arena_pool),
            inner_buffer_rx: rx,
            inner_decoder_thread: ManuallyDrop::new(handle),
            inner_decoder_stop: decoder_stop_flag,
            inner_parser: parser,
            inner_status: StreamStatus::Aligned,
        })
    }

    pub fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: Default,
        C: crate::Composite
            + crate::FromParsed<C::Attrs, <P as crate::Parse<crate::ArenaSlice<u8>, C::Kind>>::Item>
            + crate::FromBacking<
                <P as crate::Parse<crate::ArenaSlice<u8>, C::Kind>>::Item,
                C::Backing,
            >,
        P: crate::Parse<crate::ArenaSlice<u8>, C::Kind>,
    {
        self.next_with::<C, C::Attrs>()
    }

    pub fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: Default,
        C: crate::Composite
            + crate::FromParsed<A, <P as crate::Parse<crate::ArenaSlice<u8>, C::Kind>>::Item>
            + crate::FromBacking<
                <P as crate::Parse<crate::ArenaSlice<u8>, C::Kind>>::Item,
                C::Backing,
            >,
        P: crate::Parse<crate::ArenaSlice<u8>, C::Kind>,
    {
        let mut spinpark_counter = 0;

        loop {
            let buffer_status = match self.inner_buffer_rx.peek() {
                Err(PeekError::Empty) => {
                    spinpark_loop::spinpark_loop::<100>(&mut spinpark_counter);
                    continue;
                }
                Ok(status) => {
                    spinpark_counter = 0;
                    status
                }
            };

            match buffer_status {
                BufferStatus::Available(decoded) => {
                    let result =
                        match std::mem::replace(&mut self.inner_status, StreamStatus::Aligned) {
                            StreamStatus::Spanning(spanning_tail) => {
                                // println!("spanning parse");
                                let arena_pool = &self.inner_arena_pool;
                                self.inner_parser.parse_spanning::<C, A>(
                                    &spanning_tail,
                                    &decoded,
                                    |sizeof_span| arena_pool.alloc(sizeof_span),
                                )
                            }
                            StreamStatus::Aligned => {
                                // println!("aligned parse");
                                self.inner_parser.parse_aligned::<C, A>(&decoded)
                            }
                        };

                    match result {
                        ParseStatus::Full(cell) => {
                            return Ok(Some(cell));
                        }
                        ParseStatus::Partial => {
                            // Parser exhausted data
                            // SAFETY: unwrap is safe because if a partial is returned a decoded block MUST exist
                            //         because a block must have been peeked at before.
                            self.inner_status = StreamStatus::Spanning(ArenaSlice::clone(decoded));
                            self.inner_buffer_rx.pop().unwrap();
                            continue;
                        }
                        ParseStatus::Error(_) => {
                            // SAFETY: unwrap is safe because if an error is returned a decoded block MUST exist
                            //         because a block must have been peeked at before.
                            self.inner_status = StreamStatus::Spanning(ArenaSlice::clone(decoded));
                            self.inner_buffer_rx.pop().unwrap();
                            continue;
                        }

                        // SAFETY: returned only by parse_finish
                        ParseStatus::Finished => unreachable!(),
                    }
                }
                BufferStatus::Eof => {
                    // SAFETY: unwrap is safe because if EOF is returned a block MUST exist
                    //         because a block must have been peeked at before.
                    self.inner_buffer_rx.pop().unwrap();
                    match self.inner_parser.parse_finish::<C, A>() {
                        ParseStatus::Full(cell) => return Ok(Some(cell)),
                        ParseStatus::Finished => return Ok(None),
                        ParseStatus::Error(e) => return Err(e),

                        // SAFETY: parse_finish must always return the final cell as complete.
                        ParseStatus::Partial => unreachable!(),
                    }
                }
                BufferStatus::Error(e) => {
                    // self.inner_buffer_rx.pop();
                    return Err(*e);
                }
            }
        }
    }

    fn spawn_decode_worker(
        mut decoder: D,
        n_buffers: BoundedUsize<2, { usize::MAX }>,
        stop_flag: Arc<AtomicBool>,
        arena_pool: Arc<ArenaPool<u8>>,
    ) -> (JoinHandle<D>, rtrb::Consumer<BufferStatus>) {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let size = decoder.sizeof_target_alloc();
                let mut buffer = arena_pool.alloc(size);
                let decode_result = decoder.decode_into(buffer.as_mut_slice());

                let buffer_status = match decode_result {
                    DecodeStatus::Decoded(bytes_written) => {
                        BufferStatus::Available(unsafe { buffer.truncate(bytes_written) })
                    }
                    DecodeStatus::Eof => {
                        stop_flag.store(true, Ordering::Relaxed);
                        BufferStatus::Eof
                    }
                    DecodeStatus::Error(e) => {
                        stop_flag.store(true, Ordering::Relaxed);
                        BufferStatus::Error(e)
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

impl<D, P> Drop for Stream<D, P> {
    fn drop(&mut self) {
        self.inner_decoder_stop.store(true, Ordering::Relaxed);
        // SAFETY: drop is only called once
        let handle = unsafe { ManuallyDrop::take(&mut self.inner_decoder_thread) };
        handle.join().expect("Couldn't join on the Decode thread");
    }
}
