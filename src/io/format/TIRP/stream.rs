use std::cmp::max;
use std::sync::Arc;

use rust_htslib::htslib;

use crate::{
    common::{self},
    io::{
        self, format::tirp, BascetFile, BascetStream, BascetStreamToken, BascetStreamTokenBuilder,
    },
    log_critical,
};

pub struct Stream<T> {
    hts_file: *mut htslib::htsFile,

    inner_buf: Option<Arc<Vec<u8>>>,
    inner_partial: Vec<u8>,
    inner_cursor: usize,

    _marker_t: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &io::tirp::File) -> Self {
        let path = file.file_path();

        unsafe {
            let c_path = std::ffi::CString::new(path.to_str().unwrap().as_bytes()).unwrap();
            let mode = std::ffi::CString::new("r").unwrap();

            let hts_file = htslib::hts_open(c_path.as_ptr(), mode.as_ptr());
            if hts_file.is_null() {
                log_critical!("hts null");
            }

            Stream::<T> {
                hts_file,

                inner_buf: None,
                inner_cursor: 0,
                inner_partial: Vec::new(),

                _marker_t: std::marker::PhantomData,
            }
        }
    }

    fn load_next_buf(&mut self) -> anyhow::Result<bool> {
        unsafe {
            let fp = htslib::hts_get_bgzfp(self.hts_file);
            let mut buffer: Vec<u8> = vec![0; common::HUGE_PAGE_SIZE];

            let bytes_read = htslib::bgzf_read(
                fp,
                buffer.as_mut_ptr() as *mut std::os::raw::c_void,
                common::HUGE_PAGE_SIZE,
            );

            match bytes_read {
                n if n > 0 => {
                    buffer.truncate(n as usize);

                    // Always combine with partial data (even if empty)
                    self.inner_partial.extend_from_slice(&buffer);

                    // Find last complete line
                    if let Some(last_newline_pos) = memchr::memrchr(b'\n', &self.inner_partial) {
                        // Split: complete lines + remaining partial
                        let complete_data = self.inner_partial[..=last_newline_pos].to_vec();
                        let remaining = self.inner_partial[last_newline_pos + 1..].to_vec();

                        self.inner_partial = remaining;
                        self.inner_buf = Some(Arc::new(complete_data));
                        self.inner_cursor = 0;
                        Ok(true)
                    } else {
                        Ok(true)
                    }
                }
                0 => {
                    // EOF - return any remaining data
                    if !self.inner_partial.is_empty() {
                        self.inner_buf = Some(Arc::new(self.inner_partial.clone()));
                        self.inner_partial.clear();
                        self.inner_cursor = 0;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
                _ => Err(anyhow::anyhow!("Read error: {}", bytes_read)),
            }
        }
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        unsafe {
            if !self.hts_file.is_null() {
                htslib::hts_close(self.hts_file);
            }
        }
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetStreamToken + 'static,
    T::Builder: BascetStreamTokenBuilder<Token = T>,
{
    fn set_reader_threads(self, n_threads: usize) -> Self {
        unsafe {
            htslib::hts_set_threads(self.hts_file, n_threads as i32);
        }
        self
    }

    fn next_cell(&mut self) -> anyhow::Result<Option<T>> {
        let mut cell_id: Option<Vec<u8>> = None;
        let mut builder: Option<T::Builder> = None;

        loop {
            if self.inner_buf.is_none() {
                if !self.load_next_buf()? {
                    // EOF - return any partial token
                    if let Some(b) = builder.take() {
                        return Ok(Some(b.build()));
                    } else {
                        return Ok(None);
                    }
                }

                if let Some(current_buf) = &self.inner_buf {
                    if let Some(b) = builder.take() {
                        builder = Some(b.add_underlying(Arc::clone(current_buf)));
                    }
                }
            }

            let current_buf = self.inner_buf.as_ref().unwrap();

            if let Some(next_pos) =
                memchr::memchr(common::U8_CHAR_NEWLINE, &current_buf[self.inner_cursor..])
            {
                let line_start = self.inner_cursor;
                let line_end = self.inner_cursor + next_pos;
                let line = &current_buf[line_start..line_end];

                if let Ok((id, rp)) = tirp::parse_readpair(line) {
                    match &cell_id {
                        Some(existing_id) if existing_id == id => {
                            // Same cell, add read slices
                            if let Some(b) = builder.take() {
                                builder = Some(b.add_seq_slice(rp.r1).add_seq_slice(rp.r2));
                            }
                        }
                        Some(_) => {
                            // New cell found, return current token
                            if let Some(b) = builder.take() {
                                let token = b.build();
                                return Ok(Some(token));
                            }
                        }
                        None => {
                            // First cell
                            cell_id = Some(id.to_vec());

                            let new_builder = T::builder()
                                .add_underlying(Arc::clone(current_buf))
                                .add_cell_id_slice(id)
                                .add_seq_slice(rp.r1)
                                .add_seq_slice(rp.r2);
                            builder = Some(new_builder);
                        }
                    }
                }
                self.inner_cursor = line_end + 1;
            } else {
                let remaining_data = &current_buf[self.inner_cursor..];
                if !remaining_data.is_empty() {
                    self.inner_partial.extend_from_slice(remaining_data);
                }

                // kept alive by stream token now!
                let _ = drop(current_buf);

                self.inner_buf = None;
                self.inner_cursor = 0;
                continue;
            }
        }
    }
}

// fn par_map<F, R, G, L>(
//     &mut self,
//     global_state: G,
//     local_states: Vec<L>,
//     f: F,
// ) -> (Vec<R>, Arc<G>, Vec<L>)
// where
//     F: Fn(T, &G, &mut L) -> R + Send + Sync + 'static,
//     R: Send + 'static,
//     G: Send + Sync + 'static,
//     L: Send + 'static,
// {
//     let n_workers = self.worker_threadpool.max_count();
//     let (wtx, wrx) = crossbeam::channel::bounded::<Option<T>>(128);
//     let (rtx, rrx) = crossbeam::channel::bounded::<(Vec<R>, L)>(n_workers);

//     let global_state = Arc::new(global_state);
//     let mut local_states = local_states.into_iter();

//     let f = Arc::new(f);
//     for _ in 0..n_workers {
//         let rx = wrx.clone();
//         let rtx = rtx.clone();

//         let f = Arc::clone(&f);
//         let g = Arc::clone(&global_state);

//         let mut local_state = match local_states.next() {
//             Some(state) => state,
//             None => panic!("no local state available"),
//         };

//         self.worker_threadpool.execute(move || {
//             let mut thread_results = Vec::new();
//             while let Ok(Some(token)) = rx.recv() {
//                 let result = f(token, g.as_ref(), &mut local_state);
//                 thread_results.push(result);
//             }
//             let _ = rtx.send((thread_results, local_state));
//         });
//     }

//     // Feed tokens to workers
//     while let Ok(Some(token)) = self.next() {
//         let _ = wtx.send(Some(token));
//     }

//     // Signal workers to stop
//     for _ in 0..n_workers {
//         let _ = wtx.send(None);
//     }

//     self.worker_threadpool.join();

//     let mut results = Vec::new();
//     let mut local_states = Vec::new();
//     for _ in 0..n_workers {
//         if let Ok((mut thread_vec, local_state)) = rrx.recv() {
//             results.append(&mut thread_vec);
//             local_states.push(local_state);
//         }
//     }

//     (results, global_state, local_states)
// }

// fn set_worker_threads(&mut self, n_threads: usize) {
//     self.worker_threadpool.set_num_threads(n_threads);
// }
