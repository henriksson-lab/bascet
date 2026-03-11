use crate::DecodeResult;

pub trait Decode {
    fn sizeof_target_alloc(&self) -> usize;
    fn decode_into<B: AsMut<[u8]>>(&mut self, buf: B) -> DecodeResult;
}
