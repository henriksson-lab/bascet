use std::{fs::File, io::Read, os::raw, path::Path};

use bascet_core::*;
use bounded_integer::BoundedU64;
use bytesize::ByteSize;
use rust_htslib::htslib;

pub struct BBGZDecoder {
    inner_hts_file_ptr: SendPtr<htslib::htsFile>,
    inner_hts_bgzf_ptr: SendPtr<htslib::BGZF>,

    inner_hts_tpool: SendCell<htslib::htsThreadPool>,
    inner_hts_tpool_n: u64,

    inner_hts_block_size: ByteSize,
    inner_hts_sizeof_alloc: usize,
}

#[bon::bon]
impl BBGZDecoder {
    #[builder]
    pub fn new<P: AsRef<Path>>(
        with_path: P,
        #[builder(default = BoundedU64::const_new::<1>())] countof_threads: BoundedU64<
            1,
            { u64::MAX },
        >,
    ) -> Self {
        let path = with_path.as_ref();

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => todo!(),
        };

        let mut bgzf_hdr = [0u8; 18];
        file.read_exact(&mut bgzf_hdr).unwrap();

        let hts_file_ptr = unsafe { SendPtr::new_unchecked(htsutils::hts_open(path)) };

        let hts_bgzf_ptr =
            unsafe { SendPtr::new_unchecked(htslib::hts_get_bgzfp(hts_file_ptr.as_ptr())) };
        let hts_tpool = htsutils::hts_tpool_init(countof_threads, hts_file_ptr.as_ptr());
        let hts_send_tpool = unsafe { SendCell::new(hts_tpool) };

        let sizeof_bgzf_block = ByteSize::b(
            u16::from_le_bytes([bgzf_hdr[16], bgzf_hdr[17]])
                .checked_add(1)
                .expect("//TODO") as u64,
        );

        // NOTE: alloc size in terms of alloc slots not bytes
        let hts_sizeof_alloc = ((countof_threads.get() * sizeof_bgzf_block.as_u64())
            / (size_of::<u8>() as u64)) as usize;

        return Self {
            inner_hts_file_ptr: hts_file_ptr,
            inner_hts_bgzf_ptr: hts_bgzf_ptr,

            inner_hts_tpool: hts_send_tpool,
            inner_hts_tpool_n: countof_threads.get(),

            inner_hts_block_size: sizeof_bgzf_block,
            inner_hts_sizeof_alloc: hts_sizeof_alloc,
        };
    }
}

impl Decode for BBGZDecoder {
    fn sizeof_target_alloc(&self) -> usize {
        self.inner_hts_sizeof_alloc
    }

    fn decode_into<B: AsMut<[u8]>>(&mut self, mut buf: B) -> DecodeResult<()> {
        let buf_slice = buf.as_mut();
        let bgzf_read = unsafe {
            htslib::bgzf_read(
                self.inner_hts_bgzf_ptr.as_ptr(),
                buf_slice.as_mut_ptr() as *mut raw::c_void,
                buf_slice.len(),
            )
        };

        match bgzf_read {
            1.. => {
                assert!(
                    bgzf_read as usize <= buf_slice.len(),
                    "Decoder underflow: wrote {} bytes into {} byte buffer!",
                    bgzf_read as usize,
                    buf_slice.len()
                );
                DecodeResult::Decoded(bgzf_read as usize)
            }
            0 => DecodeResult::Eof,
            err => {
                panic!("{:?}", err);
                DecodeResult::Error(())
            }
        }
    }
}

impl Drop for BBGZDecoder {
    fn drop(&mut self) {
        unsafe {
            htslib::hts_close(self.inner_hts_file_ptr.as_ptr());
            htslib::hts_tpool_destroy(self.inner_hts_tpool.pool);
        }
    }
}
