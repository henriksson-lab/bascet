use std::{fs::File, io::{BufReader, Read}, sync::Arc};

use rust_htslib::htslib;
use zip::{unstable::stream::{ZipStreamReader, ZipStreamVisitor}, ZipArchive};

use crate::{
    common::{self},
    io::{format::tirp, BascetFile, BascetStream, BascetStreamToken},
    log_critical, log_info,
};

pub struct Stream<T> {
    inner_archive: ZipArchive<std::fs::File>,
    inner_files: Vec<String>,
    inner_files_cursor: usize,
    inner_buffer: Vec<u8>,

    worker_threadpool: threadpool::ThreadPool,

    _marker_t: std::marker::PhantomData<T>,
}

impl<T> Stream<T> {
    pub fn new(file: &tirp::File) -> Self {
        let path = file.file_path();
        let file = File::open(path).unwrap();
        let archive = ZipArchive::new(file).unwrap();
        let files: Vec<String> = archive.file_names().map(|s| String::from(s)).collect();
        Stream::<T> {
            inner_archive: archive,
            inner_files: files,
            inner_files_cursor: 0,
            inner_buffer: vec![0u8; 0],
            
            worker_threadpool: threadpool::ThreadPool::new(0),

            _marker_t: std::marker::PhantomData
        }
    }
}

impl<T> Drop for Stream<T> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<T> BascetStream<T> for Stream<T>
where
    T: BascetStreamToken + Send + 'static,
{
    fn next(&mut self) -> anyhow::Result<Option<T>> {
        let mut reads = Vec::<Vec<u8>>::with_capacity(1000);
        let mut last_id: Option<Vec<u8>> = None;

        let mut file = self.inner_archive.by_name(&self.inner_files[self.inner_files_cursor]).unwrap();
        self.inner_files_cursor += 1;

        if let Ok(bytes_read) = file.read_to_end(&mut self.inner_buffer) {
            match bytes_read {
                0 => { return Ok(None) }
                _ => { 
                    println!("{}", String::from_utf8_lossy(&self.inner_buffer));
                    return Ok(None)
                }
            }
        }
        Err(anyhow::anyhow!("Read error"))
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
        todo!();
    }

    fn set_worker_threads(&mut self, n_threads: usize) {
        self.worker_threadpool.set_num_threads(n_threads);
    }
}
