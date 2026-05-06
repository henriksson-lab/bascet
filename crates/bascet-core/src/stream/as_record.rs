use std::sync::atomic::Ordering;
use tracing::warn;

use crate::threading::spinpark_loop::{self, SPINPARK_COUNTOF_PARKS_BEFORE_WARN, SpinPark};
use crate::*;

impl<P, D, C> crate::Next<C> for Stream<P, D, C, AsRecord>
where
    D: Decode + Send + 'static,
    P: Parse<ArenaSlice<u8>, Item = C>,
    C: Composite<Marker = AsRecord, Intermediate = C> + Default,
{
    fn next_with<Q>(&mut self, query: &Q) -> anyhow::Result<Option<C>>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let mut spinpark_counter = 0;

        loop {
            let decoded = match self.inner_decoder_buffer_rx.peek() {
                Err(rtrb::PeekError::Empty) => {
                    if likely_unlikely::unlikely(
                        self.inner_decoder_flag_stop.load(Ordering::Relaxed) == true,
                    ) {
                        self.inner_state = StreamState::Aligned;
                        return Ok(self.inner_context.take());
                    }

                    match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(
                        &mut spinpark_counter,
                    ) {
                        SpinPark::Warn => warn!(
                            source = "Stream::next (AsRecord)",
                            "waiting for data (buffer empty, decoder slow or finished)"
                        ),
                        _ => {}
                    }
                    continue;
                }
                Ok(status) => {
                    spinpark_counter = 0;
                    status
                }
            };

            let state = std::mem::replace(&mut self.inner_state, StreamState::Aligned);
            let result = match &state {
                StreamState::Spanning(spanning_tail) => {
                    let arena_pool = &self.inner_decoder_allocator;
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
                ParseResult::Full(parsed) => parsed,
                ParseResult::Partial => {
                    // Parser exhausted data
                    // SAFETY: unwrap is safe because if a partial is returned a decoded block MUST exist
                    //         because a block must have been peeked at before.
                    self.inner_state = StreamState::Spanning(ArenaSlice::clone(&decoded));
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

            match query.apply(&parsed, &parsed) {
                QueryResult::Emit | QueryResult::Keep => return Ok(Some(parsed)),
                QueryResult::Discard => continue,
            }
        }
    }

    fn next_batch_with_retained_bytes<Q>(
        &mut self,
        query: &Q,
        capacity: usize,
    ) -> anyhow::Result<Vec<(C, usize)>>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let mut batch = Vec::with_capacity(capacity);
        if capacity == 0 {
            return Ok(batch);
        }

        let mut spinpark_counter = 0;
        let mut charged_decoded_ptr: Option<*const u8> = None;

        loop {
            if batch.len() >= capacity {
                return Ok(batch);
            }

            let decoded = match self.inner_decoder_buffer_rx.peek() {
                Err(rtrb::PeekError::Empty) => {
                    if likely_unlikely::unlikely(
                        self.inner_decoder_flag_stop.load(Ordering::Relaxed) == true,
                    ) {
                        self.inner_state = StreamState::Aligned;
                        if let Some(record) = self.inner_context.take() {
                            batch.push((record, 0));
                        }
                        return Ok(batch);
                    }

                    match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(
                        &mut spinpark_counter,
                    ) {
                        SpinPark::Warn => warn!(
                            source = "Stream::next_batch_with_retained_bytes (AsRecord)",
                            "waiting for data (buffer empty, decoder slow or finished)"
                        ),
                        _ => {}
                    }
                    continue;
                }
                Ok(status) => {
                    spinpark_counter = 0;
                    status
                }
            };

            let decoded_ptr = decoded.as_slice().as_ptr();
            let decoded_len = decoded.as_slice().len();
            let state = std::mem::replace(&mut self.inner_state, StreamState::Aligned);
            let retained_bytes_for_record;
            let result = match &state {
                StreamState::Spanning(spanning_tail) => {
                    let arena_pool = &self.inner_decoder_allocator;
                    retained_bytes_for_record =
                        spanning_tail.as_slice().len().saturating_add(decoded_len);
                    self.inner_parser
                        .parse_spanning(&spanning_tail, &decoded, |sizeof_span| {
                            arena_pool.alloc(sizeof_span)
                        })
                }
                StreamState::Aligned => {
                    retained_bytes_for_record = if charged_decoded_ptr == Some(decoded_ptr) {
                        0
                    } else {
                        decoded_len
                    };
                    self.inner_parser.parse_aligned(&decoded)
                }
            };

            let parsed = match result {
                ParseResult::Full(parsed) => parsed,
                ParseResult::Partial => {
                    self.inner_state = StreamState::Spanning(ArenaSlice::clone(&decoded));
                    unsafe {
                        self.inner_decoder_buffer_rx.pop().unwrap_unchecked();
                    }
                    continue;
                }
                ParseResult::Error(e) => return Err(e),
                ParseResult::Finished => unreachable!(),
            };

            match query.apply(&parsed, &parsed) {
                QueryResult::Emit | QueryResult::Keep => {
                    if retained_bytes_for_record != 0 {
                        charged_decoded_ptr = Some(decoded_ptr);
                    }
                    batch.push((parsed, retained_bytes_for_record));
                }
                QueryResult::Discard => continue,
            }
        }
    }
}
