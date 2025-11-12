use std::sync::Arc;
use bascet_derive::*;
use crate::{Cell, cell::{self, Builder, cell::{ID, Read}}};

// #[derive(cell::Builder)]
struct TestBuilder {
    id: Vec<u8>,
    read: Vec<u8>,
}

impl TestBuilder {
    fn new() -> Self {
        TestBuilder { 
            id: Vec::default(),
            read: vec![]
        }
    }
}

impl Builder for TestBuilder {
    type Product = TestCell;

    fn produce(self) -> Self::Product {
        Self::Product {
            id: self.id,
            read: self.read
        }
    }

    fn id(mut self, other: <Self::Product as super::marker::ProvideID>::Type) -> Self
        where
            Self::Product: super::marker::ProvideID, {
        self.id = other;
        self
    }
}

#[derive(ProvideID, ProvideRead)]
struct TestCell {
    id: Vec<u8>,
    #[cell(ID)]
    #[cell(Read)]
    read: Vec<u8>,
}

impl Cell for TestCell {
    type Builder = TestBuilder;

    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}

fn test() {
    let id = vec![b't', b'e', b's', b't'];
    let mut builder = TestCell::builder();
    builder = builder.id(id);
    let cell = builder.produce();
    let ID(id) = cell.get::<ID<Vec<u8>>>();
    let (ID(id), Read(read)) = cell.get::<(ID<Vec<u8>>, Read<Vec<u8>>)>();
}