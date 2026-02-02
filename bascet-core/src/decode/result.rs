pub enum DecodeResult {
    Decoded(usize),
    Eof,
    Error(anyhow::Error),
}