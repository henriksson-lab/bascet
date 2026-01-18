use std::fs::File;
use std::path::Path;

use bascet_core::{Decode, DecodeResult};
use bytesize::ByteSize;
use memmap2::Mmap;

pub struct PlaintextDecoder {
    mmap: Mmap,
    offset: usize,
    chunk_size: usize,
}

#[bon::bon]
impl PlaintextDecoder {
    #[builder]
    pub fn new(
        with_path: &Path,
        #[builder(default = ByteSize::mib(4))] sizeof_chunk: ByteSize,
    ) -> std::io::Result<Self> {
        let file = File::open(with_path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        #[cfg(target_os = "linux")]
        {
            let _ = mmap.advise(memmap2::Advice::Sequential);
        }

        Ok(PlaintextDecoder {
            mmap,
            offset: 0,
            chunk_size: sizeof_chunk.as_u64() as usize,
        })
    }
}

impl Decode for PlaintextDecoder {
    fn sizeof_target_alloc(&self) -> usize {
        self.chunk_size
    }

    fn decode_into<B: AsMut<[u8]>>(&mut self, mut buf: B) -> DecodeResult<()> {
        if self.offset >= self.mmap.len() {
            return DecodeResult::Eof;
        }

        let end = (self.offset + self.chunk_size).min(self.mmap.len());
        let len = end - self.offset;

        buf.as_mut()[..len].copy_from_slice(&self.mmap[self.offset..end]);
        self.offset = end;

        DecodeResult::Decoded(len)
    }
}
