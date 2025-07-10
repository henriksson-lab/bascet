use crate::{
    common::{self},
    io::{BascetStream, BascetStreamToken, TIRP},
};

pub struct Stream<R> {
    reader: R,
    counter: std::sync::atomic::AtomicUsize,
    reader_threadpool: threadpool::ThreadPool,
    worker_threadpool: threadpool::ThreadPool,
}

pub enum StreamToken {
    Memory { reads: Vec<common::ReadPair> },
    Disk { path: std::path::PathBuf },
}
impl BascetStreamToken for StreamToken {}

impl<R> Stream<R> {
    // pub fn new(inner: R) -> Self {
    //     Self {

    //     }
    // }
}

pub type DefaultStream = Stream<TIRP::DefaultReader>;

impl DefaultStream {
    pub fn from_tirp(file: &TIRP::File) -> Self {
        todo!()
    }
}

impl BascetStream for DefaultStream {
    fn next(&mut self) -> Option<StreamToken> {
        todo!()
    }

    fn work<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        todo!()
    }

    fn set_reader_threads(&mut self, n_threads: usize) {
        self.reader.set_threads(n_threads);
    }

    fn set_worker_threads(&mut self, n_threads: usize) {
        self.reader_threadpool.set_num_threads(n_threads);
    }
}
