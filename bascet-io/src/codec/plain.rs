use std::io::Read;

use bascet_core::{Decode, DecodeResult};
use bytesize::ByteSize;
use tracing::error;

pub struct PlaintextDecoder<R>
where
    R: Read,
{
    inner_reader: R,
    sizeof_target_alloc: ByteSize,
}

#[bon::bon]
impl<R> PlaintextDecoder<R>
where
    R: Read,
{
    #[builder]
    pub fn new(
        with_reader: R,
        #[builder(default = ByteSize::mib(4))] sizeof_target_alloc: ByteSize,
    ) -> Self {
        PlaintextDecoder {
            inner_reader: with_reader,
            sizeof_target_alloc: sizeof_target_alloc,
        }
    }
}

impl<R> Decode for PlaintextDecoder<R>
where
    R: Read,
{
    fn sizeof_target_alloc(&self) -> usize {
        self.sizeof_target_alloc.as_u64() as usize
    }

    fn decode_into<B: AsMut<[u8]>>(&mut self, mut buf: B) -> bascet_core::DecodeResult {
        match self.inner_reader.read(buf.as_mut()) {
            Ok(n) if n > 0 => bascet_core::DecodeResult::Decoded(n),
            Ok(0) => bascet_core::DecodeResult::Eof,
            Err(e) => {
                error!(error = %e, "plain read failed");
                DecodeResult::Error(anyhow::anyhow!("plain read error {e}"))
            }
            _ => unreachable!(),
        }
    }
}
