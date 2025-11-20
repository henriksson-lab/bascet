use std::{num::NonZero, thread::JoinHandle};

use rtrb::{PeekError, PopError, PushError};

use crate::{DecodeStatus, ParseStatus, spinpark_loop};

pub struct Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Output>,
{
    inner_buffer_rx: rtrb::Consumer<DecodeStatus<D::Output, ()>>,
    inner_decoder_thread: Option<JoinHandle<()>>,
    inner_decoder_flg_reset_parser: bool,
    inner_parser: P,
}

impl<D, P> Stream<D, P>
where
    D: crate::Decode,
    P: crate::Parse<D::Output>,
{
    pub fn new(decoder: D, parser: P) -> Self
    where
        D: Send + 'static,
        D::Output: Send,
    {
        Self::with::<2>(decoder, parser)
    }

    pub fn with<const N: usize>(decoder: D, parser: P) -> Self
    where
        D: Send + 'static,
        D::Output: Send,
    {
        let (handle, rx) = Self::spawn_decode_worker(decoder, NonZero::new(N).unwrap());
        Self {
            inner_buffer_rx: rx,
            inner_decoder_thread: Some(handle),
            inner_decoder_flg_reset_parser: false,
            inner_parser: parser,
        }
    }

    pub fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite + Default + crate::Get<crate::RefCount>,
        C: crate::ParseFrom<C::Attrs, P::Output>,
    {
        self.next_with::<C, C::Attrs>()
    }

    pub fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite + Default + crate::Get<crate::RefCount>,
        C: crate::ParseFrom<A, P::Output>,
    {
        let mut spinpark_counter = 0;
        // dbg!("called next");
        loop {
            match self.inner_buffer_rx.peek() {
                Err(PeekError::Empty) => {
                    // dbg!(spinpark_counter);
                    spinpark_loop::spinpark_loop::<100>(&mut spinpark_counter);
                    continue;
                }
                Ok(decode_status) => {
                    // dbg!("decoded!");
                    spinpark_counter = 0;
                    if std::mem::replace(&mut self.inner_decoder_flg_reset_parser, false) {
                        self.inner_parser.parse_reset()?;
                    }
                    match decode_status {
                        DecodeStatus::Decoded(decoded) => {
                            match self.inner_parser.parse::<C, A>(*decoded) {
                                ParseStatus::Full(cell) => {
                                    // dbg!("returning cell!");
                                    return Ok(Some(cell));
                                }
                                ParseStatus::Partial => {
                                    // Parser exhausted data
                                    // println!("partial cell!");
                                    self.inner_buffer_rx.pop().unwrap();
                                    self.inner_decoder_flg_reset_parser = true;
                                    continue;
                                }
                                ParseStatus::Error(e) => {
                                    dbg!("error!", e);
                                    self.inner_buffer_rx.pop().unwrap();
                                    self.inner_decoder_flg_reset_parser = true;
                                    continue;
                                }
                            }
                        }
                        DecodeStatus::Eof => {
                            self.inner_buffer_rx.pop().unwrap();
                            match self.inner_parser.parse_finish::<C, A>() {
                                ParseStatus::Full(cell) => return Ok(Some(cell)),
                                ParseStatus::Error(e) => return Err(e),

                                // SAFETY: parse_finish must always return the final cell as complete.
                                ParseStatus::Partial => unreachable!(),
                            }
                        }
                        DecodeStatus::Error(e) => {
                            // self.inner_buffer_rx.pop();
                            return Err(*e);
                        }
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
        rtrb::Consumer<DecodeStatus<D::Output, ()>>,
    )
    where
        D: Send + 'static,
        D::Output: Send,
    {
        let (mut tx, rx) = rtrb::RingBuffer::new(n_buffers.get());

        let handle = std::thread::spawn(move || loop {
            let decode_status = decoder.decode();
            let decode_break = match &decode_status {
                DecodeStatus::Eof | DecodeStatus::Error(_) => true,
                DecodeStatus::Decoded(_) => false,
            };

            let mut item = decode_status;
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
    P: crate::Parse<D::Output>,
{
    fn drop(&mut self) {
        let handle = self.inner_decoder_thread.take().unwrap();
        handle.join().expect("Couldn't join on the Decode thread");
    }
}
