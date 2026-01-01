pub enum EncodeResult<E> {
    Encoded(usize),
    Eof,
    Error(E),
}


pub trait Encode {
    fn encode_into(source: &[u8], dest: &mut [u8]) -> EncodeResult<()>;
}