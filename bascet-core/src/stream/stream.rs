use std::{num::NonZero, thread::JoinHandle};

use rtrb::{PopError, PushError};

use crate::threading;

pub struct Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Block>,
{
    inner_buffer_rx: rtrb::Consumer<crate::DecodeStatus<D::Block, ()>>,
    inner_decoder_thread: Option<JoinHandle<()>>,
    inner_parser: P,
}

impl<D, P> Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Block>,
{
    pub fn new(decoder: D, parser: P) -> Self
    where
        D: Send + 'static,
        D::Block: Send,
    {
        Self::with::<2>(decoder, parser)
    }

    pub fn with<const N: usize>(decoder: D, parser: P) -> Self
    where
        D: Send + 'static,
        D::Block: Send,
    {
        let (handle, rx) = Self::spawn_decode_worker(decoder, NonZero::new(N).unwrap());
        Self {
            inner_buffer_rx: rx,
            inner_decoder_thread: Some(handle),
            inner_parser: parser,
        }
    }

    pub fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
        C: crate::Get<crate::RefCount>,
    {
        self.next_with::<C, C::Attrs>()
    }

    pub fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
        C: crate::Get<crate::RefCount>,
    {
        let mut spinpark_counter = 0;
        loop {
            match self.inner_buffer_rx.pop() {
                Err(PopError::Empty) => {
                    threading::spinpark_loop::<100>(&mut spinpark_counter);
                }
                Ok(decode_status) => {
                    spinpark_counter = 0;

                    match decode_status {
                        crate::DecodeStatus::Block(block) => {
                            match self.inner_parser.parse::<C, A>(block) {
                                crate::ParseStatus::Full(cell) => return Ok(Some(cell)),
                                crate::ParseStatus::Partial => continue,
                                crate::ParseStatus::Error(e) => return Err(e),
                            }
                        }
                        crate::DecodeStatus::Eof(leftover) => {
                            match self.inner_parser.parse_finish::<C, A>(leftover) {
                                crate::ParseStatus::Full(cell) => return Ok(Some(cell)),
                                super::ParseStatus::Error(e) => return Err(e),

                                // SAFETY: parse_finish must always return the final cell as complete.
                                crate::ParseStatus::Partial => unreachable!(),
                            }
                        }
                        crate::DecodeStatus::Error(e) => return Err(e),
                    }
                }
            }
        }
    }

    pub fn spawn_decode_worker(
        mut decoder: D,
        n_buffers: NonZero<usize>,
    ) -> (
        JoinHandle<()>,
        rtrb::Consumer<crate::DecodeStatus<D::Block, ()>>,
    )
    where
        D: Send + 'static,
        D::Block: Send,
    {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || loop {
            let decode_status = decoder.decode();
            let decode_break = match &decode_status {
                crate::DecodeStatus::Eof(_) | crate::DecodeStatus::Error(_) => true,
                crate::DecodeStatus::Block(_) => false,
            };

            let mut item = decode_status;
            let mut spinpark_counter = 0;
            loop {
                match tx.push(item) {
                    Ok(_) => break,
                    Err(PushError::Full(i)) => {
                        item = i;
                        threading::spinpark_loop::<100>(&mut spinpark_counter);
                    }
                }
            }

            if decode_break {
                break;
            }
        });

        (handle, rx)
    }
}

impl<D, P> Drop for Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Block>,
{
    fn drop(&mut self) {
        let handle = self.inner_decoder_thread.take().unwrap();
        handle.join().expect("Couldn't join on the Decode thread");
    }
}