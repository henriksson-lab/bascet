use crate::Composite;

pub enum EncodeResult<E> {
    Encoded,
    Error(E),
}

pub trait Encode {
    fn encode_into<C: Composite>(&mut self, data: &[u8], composite: &C) -> EncodeResult<()>;
}