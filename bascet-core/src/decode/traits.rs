pub enum DecodeResult<E> {
    Decoded(usize),
    Eof,
    Error(E),
}

pub trait Decode {
    fn sizeof_target_alloc(&self) -> usize;
    fn decode_into<B: AsMut<[u8]>>(&mut self, buf: B) -> DecodeResult<()>;
}
