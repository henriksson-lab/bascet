use std::{num::NonZero, thread::JoinHandle};

use rtrb::PushError;

use crate::threading;

pub struct Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Block>,
{
    inner_buffer_rx: rtrb::Consumer<Result<Option<D::Block>, ()>>,
    inner_decoder_thread: JoinHandle<()>,
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
        let (handle, rx) = Self::spawn_decode_worker(
            decoder, NonZero::new(N).unwrap()
        );
        Self {
            inner_buffer_rx: rx,
            inner_decoder_thread: handle,
            inner_parser: parser,
        }
    }

    pub fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
    {
        loop {
            let block = self.inner_buffer_rx.pop().ok();
            match block {
                Some(block) => {
                    if let Some(structured) = self.inner_parser.parse::<C, C::Attrs>(block)? {
                        return Ok(Some(structured));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
    {
        loop {
            let block = self.inner_buffer_rx.pop().ok()??;
            match block {
                Some(block) => {
                    if let Some(structured) = self.inner_parser.parse::<C, A>(block)? {
                        return Ok(Some(structured));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn spawn_decode_worker(
        mut decoder: D,
        n_buffers: NonZero<usize>,
    ) -> (JoinHandle<()>, rtrb::Consumer<Result<Option<D::Block>, ()>>)
    where
        D: Send + 'static,
        D::Block: Send,
    {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || loop {
            match decoder.decode() {
                Ok(Some(block)) => {
                    let mut item = Ok(Some(block));
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
                }
                Ok(None) => {
                    let _ = tx.push(Ok(None));
                    break;
                }
                Err(e) => {
                    let _ = tx.push(Err(e));
                    break;
                }
            }
        });

        (handle, rx)
    }
}
