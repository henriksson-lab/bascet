use crate::BascetError;

pub enum DecodeResult {
    Decoded(usize),
    Eof,
    Error(anyhow::Error),
}