use crate::{common::ReadPair, io::BascetStream};
use std::sync::Arc;

pub struct Stream<R> {
    inner: R,
}

impl<R> Stream<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

pub type DefaultStream = Stream<std::io::Empty>;

impl DefaultStream {
    pub fn from_file(_file: &crate::io::File) -> Self {
        todo!()
    }
}

impl BascetStream for DefaultStream {
    fn next(&mut self) -> Option<crate::io::stream::Cell> {
        todo!()
    }
}
