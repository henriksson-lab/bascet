use std::{fs::File, io::Read, os::raw, path::Path};

use bascet_core::*;
use bon::bon;
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use rust_htslib::htslib;

pub struct BGZFDecoder {
    inner_hts_file_ptr: UnsafePtr<htslib::htsFile>,
    inner_hts_bgzf_ptr: UnsafePtr<htslib::BGZF>,

    inner_hts_tpool: SendCell<htslib::htsThreadPool>,
    inner_hts_tpool_n: u64,

    inner_hts_block_size: ByteSize,
    inner_hts_alloc_len: usize,

    inner_arena_pool: mem::ArenaPool<u8>,
}

#[bon]
impl BGZFDecoder {
    #[builder]
    pub fn new<P: AsRef<Path>>(
        path: P,
        #[builder(default = mem::DEFAULT_SIZEOF_BUFFER)] sizeof_buffer: ByteSize,
        #[builder(default = *mem::DEFAULT_SIZEOF_ARENA)] sizeof_arena: ByteSize,
        #[builder(default = BoundedU64::new(1).unwrap())] num_threads: BoundedU64<1, { u64::MAX }>,
    ) -> std::io::Result<Self> {
        let path = path.as_ref();

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => todo!(),
        };

        let mut bgzf_hdr = [0u8; 18];
        file.read_exact(&mut bgzf_hdr)?;

        let hts_file_ptr = unsafe { UnsafePtr::new_unchecked(htsutils::hts_open(path)) };
        let hts_bgzf_ptr =
            unsafe { UnsafePtr::new_unchecked(htslib::hts_get_bgzfp(hts_file_ptr.as_ptr())) };
        let hts_tpool = htsutils::hts_tpool_init(num_threads, hts_file_ptr.as_ptr());
        let hts_send_tpool = SendCell::new(hts_tpool);

        let sizeof_bgzf_block = ByteSize::b(
            u16::from_le_bytes([bgzf_hdr[16], bgzf_hdr[17]])
                .checked_add(1)
                .expect("//TODO") as u64,
        );

        // NOTE: alloc size in terms of alloc slots not bytes
        let hts_alloc_len =
            ((num_threads.get() * sizeof_bgzf_block.as_u64()) / (size_of::<u8>() as u64)) as usize;
        let arena_pool = ArenaPool::new(sizeof_buffer, sizeof_arena)?;

        let decoder = Self {
            inner_hts_file_ptr: hts_file_ptr,
            inner_hts_bgzf_ptr: hts_bgzf_ptr,

            inner_hts_tpool: hts_send_tpool,
            inner_hts_tpool_n: num_threads.get(),

            inner_hts_block_size: sizeof_bgzf_block,
            inner_hts_alloc_len: hts_alloc_len,

            inner_arena_pool: arena_pool,
        };

        Ok(decoder)
    }
}

impl Decode for BGZFDecoder {
    type Output = ArenaSlice<'static, u8>;

    fn decode(&mut self) -> DecodeStatus<Self::Output, ()> {
        let (buf_alloc, bgzf_read) = unsafe {
            let buf_alloc = self.inner_arena_pool.alloc(self.inner_hts_alloc_len);
            let bgzf_read = htslib::bgzf_read(
                self.inner_hts_bgzf_ptr.as_ptr(),
                // SAFETY: we create the ptr in alloc, we know it is safe to cast to *mut
                buf_alloc.inner.as_ptr() as *mut raw::c_void,
                self.inner_hts_alloc_len,
            );

            (buf_alloc, bgzf_read)
        };

        match bgzf_read {
            1.. => return DecodeStatus::Decoded(buf_alloc),
            0 => {
                return DecodeStatus::Eof;
            }
            err => {
                panic!("{:?}", err);
                return DecodeStatus::Error(());
            }
        }
    }
}

impl Drop for BGZFDecoder {
    fn drop(&mut self) {
        unsafe {
            htslib::hts_close(self.inner_hts_file_ptr.as_ptr());
            htslib::hts_tpool_destroy(self.inner_hts_tpool.pool);
        }
    }
}
