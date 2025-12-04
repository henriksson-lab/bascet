use crate::*;

type Marker = AsRecord;
impl<P, D> crate::Next<Marker> for Stream<P, D, Marker>
where
    P: crate::Context<Marker> + crate::Parse<crate::ArenaSlice<u8>, Marker>,
    D: crate::Decode + Send + 'static,
{
    type Intermediate = <P as crate::Parse<crate::ArenaSlice<u8>, Marker>>::Item;

    fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite<Marker = Marker> + Default,
        C: crate::FromParsed<A, Self::Intermediate>
            + crate::FromBacking<Self::Intermediate, C::Backing>,
    {
        let mut spinpark_counter = 0;
        if likely_unlikely::unlikely(self.inner_context.is_none()) {
            self.inner_context = Some(P::Context::default())
        }
        let mut context = unsafe { self.inner_context.take().unwrap_unchecked() };

        loop {
            let buffer_status = match self.inner_buffer_rx.peek() {
                Err(rtrb::PeekError::Empty) => {
                    spinpark_loop::spinpark_loop::<100>(&mut spinpark_counter);
                    continue;
                }
                Ok(status) => {
                    spinpark_counter = 0;
                    status
                }
            };

            match buffer_status {
                StreamBufferState::Available(decoded) => {
                    let result =
                        match std::mem::replace(&mut self.inner_status, StreamState::Aligned) {
                            StreamState::Spanning(spanning_tail) => {
                                let arena_pool = &self.inner_arena_pool;
                                self.inner_parser.parse_spanning::<C, A>(
                                    &spanning_tail,
                                    &decoded,
                                    &mut context,
                                    |sizeof_span| arena_pool.alloc(sizeof_span),
                                )
                            }
                            StreamState::Aligned => {
                                self.inner_parser.parse_aligned::<C, A>(
                                    &decoded, //
                                    &mut context,
                                )
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
                            self.inner_status = StreamState::Spanning(ArenaSlice::clone(decoded));
                            self.inner_buffer_rx.pop().unwrap();
                            continue;
                        }
                        ParseStatus::Error(_) => {
                            // SAFETY: unwrap is safe because if an error is returned a decoded block MUST exist
                            //         because a block must have been peeked at before.
                            self.inner_status = StreamState::Spanning(ArenaSlice::clone(decoded));
                            self.inner_buffer_rx.pop().unwrap();
                            continue;
                        }

                        // SAFETY: returned only by parse_finish
                        ParseStatus::Finished => unreachable!(),
                    }
                }
                StreamBufferState::Eof => {
                    // SAFETY: unwrap is safe because if EOF is returned a block MUST exist
                    //         because a block must have been peeked at before.
                    self.inner_buffer_rx.pop().unwrap();
                    match self.inner_parser.parse_finish::<C, A>(&mut context) {
                        ParseStatus::Full(cell) => return Ok(Some(cell)),
                        ParseStatus::Error(e) => return Err(e),

                        // NOTE: No cell can be built with the rest but theres no error. Only used when decoded.len() == 0
                        ParseStatus::Finished => return Ok(None),
                        // SAFETY: parse_finish must always return the final cell as complete or error out.
                        ParseStatus::Partial => unreachable!(),
                    }
                }
                StreamBufferState::Error(e) => {
                    // self.inner_buffer_rx.pop();
                    return Err(*e);
                }
            }
        }
    }
}
