use crate::SerialiseView;
use bascet_core::{Attr, Composite, Get};

pub trait Serialiser<A> {
    type Writer: std::io::Write;

    fn serialize<C>(&mut self, cell: &C) -> Result<(), Box<dyn std::error::Error>>
    where
        C: Composite,
        for<'a> SerialiseView<'a, C, A>: serde::Serialize;

    fn inner(&self) -> &Self::Writer;
    fn into_inner(self) -> Self::Writer;
}

pub trait SerialiseAttr<C>: Attr + Sized
where
    C: Get<Self> + Sized,
{
    type Output<'a>: serde::Serialize
    where
        C: 'a,
        Self: 'a;

    fn serialise<'a>(cell: &'a C) -> Self::Output<'a>;
}
