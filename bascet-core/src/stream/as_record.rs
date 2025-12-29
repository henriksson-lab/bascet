use crate::{spinpark_loop::SPINPARK_PARKS_BEFORE_WARN, *};

impl<P, D, C> crate::Next<C> for Stream<P, D, C, AsRecord>
where
    D: Decode + Send + 'static,
    P: Parse<ArenaSlice<u8>, Item = C>,
    C: Composite<Marker = AsRecord, Intermediate = C> + Default,
{
    fn next_with<Q>(&mut self, query: &Q) -> Result<Option<C>, ()>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let mut spinpark_counter = 0;

        loop {
            let buffer_status = match self.inner_buffer_rx.peek() {
                Err(rtrb::PeekError::Empty) => {
                    spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                        &mut spinpark_counter,
                        "Consumer (AsRecord): waiting for data (buffer empty, decoder slow or finished)"
                    );
                    continue;
                }
                Ok(status) => {
                    spinpark_counter = 0;
                    status
                }
            };

            let decoded = match buffer_status {
                StreamBufferState::Available(decoded) => decoded,
                StreamBufferState::Error(e) => return Err(*e),
                StreamBufferState::Eof => {
                    self.inner_state = StreamState::Aligned;
                    return Ok(self.inner_context.take());
                }
            };

            let state = std::mem::replace(&mut self.inner_state, StreamState::Aligned);
            let result = match &state {
                StreamState::Spanning(spanning_tail) => {
                    let arena_pool = &self.inner_arena_pool;
                    self.inner_parser
                        .parse_spanning(&spanning_tail, &decoded, |sizeof_span| {
                            arena_pool.alloc(sizeof_span)
                        })
                }
                StreamState::Aligned => {
                    self.inner_parser.parse_aligned(
                        &decoded, //
                    )
                }
            };

            let parsed = match result {
                ParseStatus::Full(parsed) => parsed,
                ParseStatus::Partial => {
                    // Parser exhausted data
                    // SAFETY: unwrap is safe because if a partial is returned a decoded block MUST exist
                    //         because a block must have been peeked at before.
                    self.inner_state = StreamState::Spanning(ArenaSlice::clone(decoded));
                    unsafe {
                        self.inner_buffer_rx.pop().unwrap_unchecked();
                    }
                    continue;
                }
                ParseStatus::Error(e) => {
                    // SAFETY: unwrap is safe because if an error is returned a decoded block MUST exist
                    //         because a block must have been peeked at before.
                    return Err(e);
                }

                ParseStatus::Finished => {
                    // SAFETY: returned only by parse_finish
                    unreachable!();
                }
            };

            if likely_unlikely::likely(self.inner_context.is_some()) {
                let context = unsafe { self.inner_context.as_mut().unwrap_unchecked() };
                match query.apply(&parsed, &parsed) {
                    QueryResult::Emit => return Ok(Some(parsed)),
                    QueryResult::Discard => continue,
                    QueryResult::Keep => unreachable!(),
                }
            } else {
                match query.apply(&parsed, &parsed) {
                    QueryResult::Emit => return Ok(Some(parsed)),
                    QueryResult::Discard => continue,
                    QueryResult::Keep => unreachable!(),
                }
            }
        }
    }
}
