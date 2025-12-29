use rust_htslib::htslib::{self, htsFile};

pub fn hts_tpool_init(
    num_threads: bounded_integer::BoundedU64<1, { u64::MAX }>,
    hts_file_ptr: *mut htslib::htsFile,
) -> htslib::htsThreadPool {
    unsafe {
        let inner_tpool = htslib::hts_tpool_init(num_threads.get() as i32);
        let mut tpool = htslib::htsThreadPool {
            pool: inner_tpool,
            qsize: 0,
        };

        if htslib::hts_set_thread_pool(hts_file_ptr, &mut tpool as *mut htslib::htsThreadPool) < 0 {
            panic!();
        }

        tpool
    }
}

pub fn hts_open<P: AsRef<std::path::Path>>(path: P) -> *mut htsFile {
    let path = path.as_ref();
    unsafe {
        let path_str = match path.to_str() {
            Some(s) => s,
            None => {
                todo!()
            }
        };

        let c_path = match std::ffi::CString::new(path_str.as_bytes()) {
            Ok(p) => p,
            Err(_) => {
                todo!()
            }
        };

        let mode = match std::ffi::CString::new("r") {
            Ok(m) => m,
            Err(_) => {
                todo!()
            }
        };

        let inner_hts_file_ptr = htslib::hts_open(c_path.as_ptr(), mode.as_ptr());
        if inner_hts_file_ptr.is_null() {
            todo!()
        }

        inner_hts_file_ptr
    }
}
