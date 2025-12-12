// use crate::*;

// impl<P, D, C> crate::Next<C> for Stream<P, D, C, AsCell<Direct>>
// where
//     D: crate::Decode + Send + 'static,
//     C: Composite<Marker = AsCell<Direct>, Intermediate = C> + Default,
//     P: crate::Parse<crate::ArenaSlice<u8>, Item = C>,
// {
//     fn next_with<Q>(&mut self, query: &Q) -> Result<Option<C>, ()>
//     where
//         Q: QueryApply<C::Intermediate, C>,
//     {
//         let mut context = C::default();
//         let mut spinpark_counter = 0;

//         loop {
//             let buffer_status = match self.inner_buffer_rx.peek() {
//                 Err(rtrb::PeekError::Empty) => {
//                     spinpark_loop::spinpark_loop::<100>(&mut spinpark_counter);
//                     continue;
//                 }
//                 Ok(status) => {
//                     spinpark_counter = 0;
//                     status
//                 }
//             };

//             match buffer_status {
//                 StreamBufferState::Available(decoded) => {
//                     let result =
//                         match std::mem::replace(&mut self.inner_state, StreamState::Aligned) {
//                             StreamState::Spanning(spanning_tail) => {
//                                 let arena_pool = &self.inner_arena_pool;
//                                 self.inner_parser.parse_spanning(
//                                     &spanning_tail,
//                                     &decoded,
//                                     |sizeof_span| arena_pool.alloc(sizeof_span),
//                                 )
//                             }
//                             StreamState::Aligned => {
//                                 self.inner_parser.parse_aligned(
//                                     &decoded, //
//                                 )
//                             }
//                         };

//                     match result {
//                         ParseStatus::Full(parsed) => match query.apply(&parsed, &parsed) {
//                             QueryResult::Discard => {
//                                 continue;
//                             }
//                             QueryResult::Keep | QueryResult::Emit => {
//                                 return Ok(Some(parsed));
//                             }
//                         },
//                         ParseStatus::Partial => {
//                             // Parser exhausted data
//                             // SAFETY: unwrap is safe because if a partial is returned a decoded block MUST exist
//                             //         because a block must have been peeked at before.
//                             self.inner_state = StreamState::Spanning(ArenaSlice::clone(decoded));
//                             self.inner_buffer_rx.pop().unwrap();
//                             continue;
//                         }
//                         ParseStatus::Error(_) => {
//                             // SAFETY: unwrap is safe because if an error is returned a decoded block MUST exist
//                             //         because a block must have been peeked at before.
//                             self.inner_state = StreamState::Spanning(ArenaSlice::clone(decoded));
//                             self.inner_buffer_rx.pop().unwrap();
//                             continue;
//                         }

//                         // SAFETY: returned only by parse_finish
//                         ParseStatus::Finished => unreachable!(),
//                     }
//                 }
//                 StreamBufferState::Eof => {
//                     // SAFETY: unwrap is safe because if EOF is returned a block MUST exist
//                     //         because a block must have been peeked at before.
//                     self.inner_buffer_rx.pop().unwrap();
//                     match self.inner_parser.parse_finish() {
//                         ParseStatus::Full(parsed) => match query.apply(&parsed, &parsed) {
//                             QueryResult::Discard => return Ok(None),
//                             QueryResult::Keep | QueryResult::Emit => {
//                                 return Ok(Some(parsed));
//                             }
//                         },
//                         ParseStatus::Error(_) => return Err(()),

//                         // NOTE: No cell can be built with the rest but theres no error. Only used when decoded.len() == 0
//                         ParseStatus::Finished => return Ok(None),
//                         // SAFETY: parse_finish must always return the final cell as complete or error out.
//                         ParseStatus::Partial => unreachable!(),
//                     }
//                 }

//                 StreamBufferState::Error(e) => {
//                     // self.inner_buffer_rx.pop();
//                     return Err(*e);
//                 }
//             }
//         }
//     }
// }
