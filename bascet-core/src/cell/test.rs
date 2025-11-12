use crate::{
    cell::*
};
use bascet_provide::*;
use std::sync::Arc;

// #[derive(cell::Builder)]
struct TestBuilder {
    id: Vec<u8>,
    read: Vec<u8>,
}

impl TestBuilder {
    fn new() -> Self {
        TestBuilder {
            id: Vec::default(),
            read: vec![],
        }
    }
}

impl Builder for TestBuilder {
    type Builds = TestCell;

    fn build(self) -> Self::Builds {
        Self::Builds {
            id: self.id,
            read: self.read,
        }
    }
}

#[cell(Id, Read)]
struct TestCell {
    id: Vec<u8>,
    read: Vec<u8>,
}

impl Cell for TestCell {
    type Builder = TestBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}

fn _test() {
    let mut builder = TestCell::builder();
    let id = Vec::from(b"Hello World");
    builder = builder.id(id);
    let mut cell = builder.build();

    let Id(id_mut) = cell.get_mut::<Id<&mut Vec<u8>>>();
    id_mut.extend_from_slice(b"This wont work");
    let (Id(_), Read(_)) = cell.get::<(Id<&Vec<u8>>, Read<&Vec<u8>>)>();
}
