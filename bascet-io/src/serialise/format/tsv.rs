use std::marker::PhantomData;

use bascet_core::Composite;

use crate::{SerialiseView, Serialiser};

pub struct TsvWriter<W, A = ()>
where
    W: std::io::Write,
{
    inner: csv::Writer<W>,
    _marker: std::marker::PhantomData<A>,
}

impl<W: std::io::Write> TsvWriter<W, ()> {
    pub fn with<A>(writer: W) -> TsvWriter<W, A> {
        TsvWriter::<W, A> {
            inner: csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(writer),
            _marker: PhantomData,
        }
    }
}

impl<W: std::io::Write, A> Serialiser<A> for TsvWriter<W, A> {
    type Writer = W;

    fn serialize<C>(&mut self, cell: &C) -> Result<(), Box<dyn std::error::Error>>
    where
        C: Composite,
        for<'a> SerialiseView<'a, C, A>: serde::Serialize,
    {
        self.inner.serialize(&SerialiseView::<C, A>::new(cell))?;
        Ok(())
    }

    fn inner(&self) -> &Self::Writer {
        self.inner.get_ref()
    }

    fn into_inner(self) -> Self::Writer {
        self.inner.into_inner().unwrap()
    }
}
