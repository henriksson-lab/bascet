use std::{cell, sync::Arc};

use crossbeam::channel;
use rust_htslib::htslib;

use crate::{
    common::{self},
    io::{BascetFile, BascetStream, BascetStreamToken, TIRP},
    log_info,
    runtime::CONFIG,
};

pub struct Stream {
    hts_file: *mut htslib::htsFile,

    inner_buf: Vec<u8>,
    inner_pos: usize,
    inner_valid_bytes: usize,

    counter: std::sync::atomic::AtomicUsize,
    worker_threadpool: threadpool::ThreadPool,
}

#[derive(Debug)]
pub enum StreamToken {
    Memory {
        cell_id: Vec<u8>,
        reads: Vec<common::ReadPair>,
    },
    Disk {
        cell_id: Vec<u8>,
        path: std::path::PathBuf,
    },
}
impl BascetStreamToken for StreamToken {}

impl Stream {
    // pub fn new(inner: R) -> Self {
    //     Self {

    //     }
    // }
}

pub type DefaultStream = Stream;

impl DefaultStream {
    pub fn from_tirp(file: &TIRP::File) -> Self {
        let path = file.file_path();

        unsafe {
            let c_path = std::ffi::CString::new(path.to_str().unwrap().as_bytes()).unwrap();
            let mode = std::ffi::CString::new("r").unwrap();

            let hts_file = htslib::hts_open(c_path.as_ptr(), mode.as_ptr());
            if hts_file.is_null() {
                panic!("hts null");
            }

            Stream {
                hts_file,
                // hts_buf: htslib::kstring_t {
                //     l: 0,
                //     m: 0,
                //     s: std::ptr::null_mut(),
                // },
                inner_buf: vec![0; common::HUGE_PAGE_SIZE],
                inner_pos: 0,
                inner_valid_bytes: 0,

                counter: std::sync::atomic::AtomicUsize::new(0),
                worker_threadpool: threadpool::ThreadPool::new(1),
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

impl BascetStream for DefaultStream {
    type Token = StreamToken;

    fn next(&mut self) -> anyhow::Result<Option<Self::Token>> {
        let mut reads = Vec::new();
        let mut last_id: Option<Vec<u8>> = None;

        loop {
            // Find next line in buffer
            // println!("Iterate! pos => {}/{} [len: {}]", self.inner_pos, self.inner_valid_bytes, self.inner_buf.len());
            let next = self.inner_buf[self.inner_pos..self.inner_valid_bytes]
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
                // println!("Line: {:?}", String::from_utf8_lossy(line));
                if let Ok((rp, cell_id)) = TIRP::parse_readpair(line) {
                    // println!("cell: {}", String::from_utf8_lossy(&cell_id));
                    match &last_id {
                        None => {
                            last_id = Some(cell_id);
                            reads.push(rp);
                        }
                        Some(last) if *last == cell_id => {
                            // println!("Appending");
                            reads.push(rp);
                        }
                        Some(_) => {
                            // println!("Returning. Reads.len: {}", reads.len());
                            // New cell found, return current batch
                            return Ok(Some(StreamToken::Memory {
                                cell_id: cell_id,
                                reads,
                            }));
                        }
                    }
                }
                self.inner_pos += cursor + 1;
            } else {
                // Save the partial line before reading a new chunk
                let mut partial_line =
                    self.inner_buf[self.inner_pos..self.inner_valid_bytes].to_vec();
                // println!("Fetching new page! pos => {}/{}", self.inner_pos, self.inner_valid_bytes);
                // println!("Partial: {}", String::from_utf8_lossy(&partial_line));
                match self.read_chunk() {
                    Ok(Some(_)) => {
                        // Prepend the partial line to the new buffer if not empty
                        if !partial_line.is_empty() {
                            partial_line
                                .extend_from_slice(&self.inner_buf[..self.inner_valid_bytes]);
                            self.inner_buf = partial_line;
                            self.inner_valid_bytes = self.inner_buf.len();
                        }
                        self.inner_pos = 0;
                        // Continue loop
                    }
                    Ok(None) => {
                        // EOF
                        return if !reads.is_empty() {
                            Ok(Some(StreamToken::Memory {
                                cell_id: last_id.unwrap(),
                                reads,
                            }))
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

    fn par_map<F, R>(&mut self, f: F) -> Vec<R>
    where
        F: Fn(StreamToken) -> R + Send + Sync + 'static,
        R: Send + 'static,
    {
        let n_workers = self.worker_threadpool.max_count();
        let (wtx, wrx) = channel::bounded::<Option<StreamToken>>(128);
        let (rtx, rrx) = channel::bounded::<Vec<R>>(n_workers);

        let f = Arc::new(f);
        for _ in 0..n_workers {
            let rx = wrx.clone();
            let rtx = rtx.clone();
            let f = Arc::clone(&f);

            self.worker_threadpool.execute(move || {
                let mut thread_results = Vec::new();
                while let Ok(Some(token)) = rx.recv() {
                    let result = f(token);
                    thread_results.push(result);
                }
                // Send this thread's results back to the main thread
                let _ = rtx.send(thread_results);
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

        // Wait for workers to finish
        self.worker_threadpool.join();

        // Collect all results from each thread
        let mut results = Vec::new();
        for _ in 0..n_workers {
            if let Ok(mut thread_vec) = rrx.recv() {
                results.append(&mut thread_vec);
            }
        }
        results
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

// pub fn get_minhash_kmcdump_parallel(
//     params: &KmerCounter,
//     n_workers: usize
// ) -> anyhow::Result<BoundedMinHeap<&[u8]>> {
//     //Spinning up workers for every new file can be pricey... could put this in params or something, to hide it. future work

//     let params= Arc::new(params);

//     //Create all thread states
//     let threads_buffer_size = (HUGE_PAGE_SIZE / n_workers) - (params.kmer_size + KMC_COUNTER_MAX_DIGITS);

//     //Decide on KMER encoding
//     let codec = KMERCodec::new(params.kmer_size);

//     //Set up memory-mapped reading of file
//     let file = File::open(&params.path_kmcdump).unwrap();
//     let lock = file.lock_exclusive();
//     let mmap = Arc::new(unsafe { MmapOptions::new().map(&file) }.unwrap());

//     //Set up a channel to send regions for reading to worker threads
//     let (tx, rx) = crossbeam::channel::bounded(n_workers*3);
//     let (tx, rx) = (Arc::new(tx), Arc::new(rx));

//     //Set up a channel to gather minheaps at end
//     let (tx_minheap, rx_minheap) = crossbeam::channel::bounded(n_workers);
//     let (tx_minheap, rx_minheap) = (Arc::new(tx_minheap), Arc::new(rx_minheap));

//     //Start all workers
//     let thread_pool = ThreadPool::new(n_workers);
//     for _tidx in 0..n_workers {
//         let rx = Arc::clone(&rx);
//         let tx_minheap = Arc::clone(&tx_minheap);
//         let mmap = Arc::clone(&mmap);
//         let ovlp_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
//         let kmer_size = params.kmer_size;
//         let features_nmin = params.features_nmin;
//         thread_pool.execute(move || {
//             let mut min_heap = BoundedMinHeap::with_capacity(features_nmin);
//             while let Ok(Some((start, end))) = rx.recv() {
//                 let chunk = &mmap[start..end];
//                 process_chunk_to_minheap(
//                     &chunk,
//                     &mut min_heap,
//                     codec,
//                     kmer_size,
//                     ovlp_size,
//                 );
//             }
//             tx_minheap.send(Arc::new(min_heap)).unwrap();
//         });
//     }

//     //In main thread, instruct workers where to read
//     let overlap_window_size = params.kmer_size + KMC_COUNTER_MAX_DIGITS;
//     let n_chunks = (mmap.len() + threads_buffer_size - 1) / threads_buffer_size;
//     for i in 0..n_chunks {
//         let raw_start = i * threads_buffer_size;
//         let raw_end = min(
//             raw_start + threads_buffer_size + overlap_window_size,
//             mmap.len(),
//         );
//         let valid_start = find_chunk_start(&mmap[raw_start..], raw_start, overlap_window_size);
//         let valid_end = find_chunk_end(&mmap[..raw_end], raw_end, overlap_window_size);
//         tx.send(Some((valid_start, valid_end))).unwrap();
//     }

//     //Shut down all workers and wait for them to finish
//     for _ in 0..n_workers {
//         tx.send(None).unwrap();
//     }
//     thread_pool.join();

//     //Merge all minheaps
//     let mut final_min_heap = BoundedMinHeap::with_capacity(params.features_nmin);
//     for _s in 0..n_workers {
//         let mh = rx_minheap.recv().unwrap();
//         for d in mh.iter() {
//             _ = final_min_heap.push(d.clone());
//         }
//     }

//     //Explicitly dropping file lock because i am paranoid it will not unlock otherwise
//     drop(lock);

//     Ok(final_min_heap)
// }
