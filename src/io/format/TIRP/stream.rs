use std::sync::Arc;

use rust_htslib::htslib;

use crate::{
    common::{self},
    io::{format::tirp, BascetFile, BascetStream, BascetStreamToken},
    log_critical, log_info,
};

pub struct Stream<T> {
    hts_file: *mut htslib::htsFile,

    inner_buf: Vec<u8>,
    inner_pos: usize,
    inner_valid_bytes: usize,

    worker_threadpool: threadpool::ThreadPool,

    _marker_t: std::marker::PhantomData<T>,
    // _market_i: std::marker::PhantomData<I>,
    // _marker_p: std::marker::PhantomData<P>,
}

impl<T> Stream<T> {
    pub fn new(file: &tirp::File) -> Self {
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

                inner_buf: vec![0; common::HUGE_PAGE_SIZE],
                inner_pos: 0,
                inner_valid_bytes: 0,

                worker_threadpool: threadpool::ThreadPool::new(0),

                _marker_t: std::marker::PhantomData,
            }
        }
    }

    fn read_chunk(&mut self) -> anyhow::Result<Option<()>> {
        unsafe {
            let fp = htslib::hts_get_bgzfp(self.hts_file);
            self.inner_buf.resize(common::HUGE_PAGE_SIZE, 0);

            let bytes_read = htslib::bgzf_read(
                fp,
                &mut self.inner_buf[0] as *mut u8 as *mut std::os::raw::c_void,
                common::HUGE_PAGE_SIZE,
            );

            self.inner_valid_bytes = bytes_read as usize;
            self.inner_pos = 0;

            if bytes_read > 0 {
                Ok(Some(()))
            } else if bytes_read == 0 {
                Ok(None) // EOF
            } else {
                Err(anyhow::anyhow!("Read error: {}", bytes_read))
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
    T: BascetStreamToken + Send + 'static,
{
    fn next(&mut self) -> anyhow::Result<Option<T>> {
        let mut reads = Vec::<Vec<u8>>::with_capacity(1000);
        let mut last_id: Option<Vec<u8>> = None;

        loop {
            // Find next line in buffer
            // println!("Iterate! pos => {}/{} [len: {}]", self.inner_pos, self.inner_valid_bytes, self.inner_buf.len());
            let next = &self.inner_buf[self.inner_pos..self.inner_valid_bytes]
                .iter()
                .position(|&b| b == common::U8_CHAR_NEWLINE);

            // println!("Found: {:?}", next);
            if let Some(cursor) = next {
                // Extract line from buffer
                let line = &self.inner_buf[self.inner_pos..self.inner_pos + cursor];
                if line.is_empty() {
                    self.inner_pos += cursor + 1;
                    continue;
                }
                if let Some(tab_pos) = line.iter().position(|&b| b == common::U8_CHAR_TAB) {
                    let cell_id = &line[..tab_pos];
                    match &last_id {
                        Some(last) if last == cell_id => {
                            reads.push(line.to_vec());
                        }
                        Some(_) => {
                            // New cell found, return current batch
                            let token = T::new(last_id.unwrap(), reads);
                            return Ok(Some(token));
                        }
                        None => {
                            last_id = Some(cell_id.to_vec());
                            reads.push(line.to_vec());
                        }
                    }
                }
                self.inner_pos += cursor + 1;
            } else {
                // Save the partial line before reading a new chunk
                let mut partial_line =
                    self.inner_buf[self.inner_pos..self.inner_valid_bytes].to_vec();
                match self.read_chunk() {
                    Ok(Some(_)) => {
                        // Prepend the partial line to the new buffer if not empty
                        if !partial_line.is_empty() {
                            partial_line
                                .extend_from_slice(&self.inner_buf[..self.inner_valid_bytes]);
                            self.inner_buf = partial_line;
                            self.inner_valid_bytes = self.inner_buf.len();
                        }
                    }
                    Ok(None) => {
                        // EOF
                        return if !reads.is_empty() {
                            let token = T::new(last_id.unwrap(), reads);
                            return Ok(Some(token));
                        } else {
                            Ok(None)
                        };
                    }
                    Err(e) => {
                        log_info!("Error reading chunk"; "error" => format!("{:?}", e));
                        return Err(e);
                    }
                }
            }
        }
    }

    fn par_map<F, R, G, L>(
        &mut self,
        global_state: G,
        local_states: Vec<L>,
        f: F,
    ) -> (Vec<R>, Arc<G>, Vec<L>)
    where
        F: Fn(T, &G, &mut L) -> R + Send + Sync + 'static,
        R: Send + 'static,
        G: Send + Sync + 'static,
        L: Send + 'static,
    {
        let n_workers = self.worker_threadpool.max_count();
        let (wtx, wrx) = crossbeam::channel::bounded::<Option<T>>(128);
        let (rtx, rrx) = crossbeam::channel::bounded::<(Vec<R>, L)>(n_workers);

        let global_state = Arc::new(global_state);
        let mut local_states = local_states.into_iter();

        let f = Arc::new(f);
        for _ in 0..n_workers {
            let rx = wrx.clone();
            let rtx = rtx.clone();

            let f = Arc::clone(&f);
            let g = Arc::clone(&global_state);

            let mut local_state = match local_states.next() {
                Some(state) => state,
                None => panic!("no local state available"),
            };

            self.worker_threadpool.execute(move || {
                let mut thread_results = Vec::new();
                while let Ok(Some(token)) = rx.recv() {
                    let result = f(token, g.as_ref(), &mut local_state);
                    thread_results.push(result);
                }
                let _ = rtx.send((thread_results, local_state));
            });
        }

        // Feed tokens to workers
        while let Ok(Some(token)) = self.next() {
            let _ = wtx.send(Some(token));
        }

        // Signal workers to stop
        for _ in 0..n_workers {
            let _ = wtx.send(None);
        }

        self.worker_threadpool.join();

        let mut results = Vec::new();
        let mut local_states = Vec::new();
        for _ in 0..n_workers {
            if let Ok((mut thread_vec, local_state)) = rrx.recv() {
                results.append(&mut thread_vec);
                local_states.push(local_state);
            }
        }

        (results, global_state, local_states)
    }

    fn set_reader_threads(&mut self, n_threads: usize) {
        unsafe {
            htslib::hts_set_threads(self.hts_file, n_threads as i32);
        }
    }

    fn set_worker_threads(&mut self, n_threads: usize) {
        self.worker_threadpool.set_num_threads(n_threads);
    }
}
