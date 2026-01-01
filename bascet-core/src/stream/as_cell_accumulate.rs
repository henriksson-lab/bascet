use crate::{spinpark_loop::SPINPARK_PARKS_BEFORE_WARN, *};

impl<P, D, C> crate::Next<C> for Stream<P, D, C, AsCell<Accumulate>>
where
    D: Decode + Send + 'static,
    P: Parse<ArenaSlice<u8>, Item = C::Intermediate>,
    C: Composite<Marker = AsCell<Accumulate>> + Default,
    C: Push<C::Collection, C::Intermediate> + FromDirect<C::Single, C::Intermediate>,
    C: PushBacking<C::Intermediate, <C::Intermediate as Composite>::Backing>,
    C::Intermediate: Composite<Marker = AsRecord> + Default + Clone,
    C::Intermediate: TakeBacking<<C::Intermediate as Composite>::Backing>,
{
    fn next_with<Q>(&mut self, query: &Q) -> Result<Option<C>, ()>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let mut spinpark_counter = 0;

        loop {
            let buffer_status = match self.inner_decoder_buffer_rx.peek() {
                Err(rtrb::PeekError::Empty) => {
                    spinpark_loop::spinpark_loop_warn::<100, SPINPARK_PARKS_BEFORE_WARN>(
                        &mut spinpark_counter,
                        "Consumer (AsCell<Accumulate>): waiting for data (buffer empty, decoder slow or finished)"
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
                StreamState::Aligned => {
                    self.inner_parser.parse_aligned(
                        &decoded, //
                    )
                }
                StreamState::Spanning(spanning_tail) => {
                    let arena_pool = &self.inner_decoder_arena_pool;
                    self.inner_parser
                        .parse_spanning(&spanning_tail, &decoded, |sizeof_span| {
                            arena_pool.alloc(sizeof_span)
                        })
                }
            };

            let parsed = match result {
                ParseResult::Full(parsed) => parsed,
                ParseResult::Partial => {
                    // Parser exhausted data
                    // SAFETY: unwrap is safe because if a partial is returned a decoded block MUST exist
                    //         because a block must have been peeked at before.
                    self.inner_state = StreamState::Spanning(ArenaSlice::clone(decoded));
                    unsafe {
                        self.inner_decoder_buffer_rx.pop().unwrap_unchecked();
                    }
                    continue;
                }
                ParseResult::Error(e) => {
                    // SAFETY: unwrap is safe because if an error is returned a decoded block MUST exist
                    //         because a block must have been peeked at before.
                    return Err(e);
                }

                ParseResult::Finished => {
                    // SAFETY: returned only by parse_finish
                    unreachable!();
                }
            };

            if likely_unlikely::likely(self.inner_context.is_some()) {
                let context = unsafe { self.inner_context.as_mut().unwrap_unchecked() };
                match query.apply(&parsed, &context) {
                    QueryResult::Discard => {
                        continue;
                    }
                    QueryResult::Keep => {
                        <C as Push<C::Collection, C::Intermediate>>::push(
                            context, //
                            &parsed,
                        );
                        match &state {
                            StreamState::Spanning(_) => {
                                context.push_backing(parsed.take_backing());
                            }
                            _ => {}
                        }

                        continue;
                    }
                    QueryResult::Emit => {
                        let result = self.inner_context.take().unwrap();
                        let mut new_ctx = C::default();
                        <C as FromDirect<C::Single, C::Intermediate>>::from_direct(
                            &mut new_ctx, //
                            &parsed,
                        );
                        <C as Push<C::Collection, C::Intermediate>>::push(
                            &mut new_ctx, //
                            &parsed,
                        );
                        new_ctx.push_backing(parsed.take_backing());

                        self.inner_context = Some(new_ctx);
                        return Ok(Some(result));
                    }
                }
            } else {
                let mut context_temp = C::default();
                <C as FromDirect<C::Single, C::Intermediate>>::from_direct(
                    &mut context_temp, //
                    &parsed,
                );

                match query.apply(&parsed, &context_temp) {
                    QueryResult::Discard => {
                        continue;
                    }
                    QueryResult::Keep => {
                        <C as Push<C::Collection, C::Intermediate>>::push(
                            &mut context_temp, //
                            &parsed,
                        );
                        context_temp.push_backing(parsed.take_backing());

                        self.inner_context = Some(context_temp);
                        continue;
                    }
                    QueryResult::Emit => {
                        <C as Push<C::Collection, C::Intermediate>>::push(
                            &mut context_temp, //
                            &parsed,
                        );
                        context_temp.push_backing(parsed.take_backing());

                        self.inner_context = None;
                        return Ok(Some(context_temp));
                    }
                }
            }
        }
    }
}

/*
Can you check:
  1. ls -la temp/29432994_* - Are there files in the middle of being written?
  2. lsof -p <pid> - What files does the process have open?
  3. strace -p <pid> - What system call is it stuck on?
 */
