use std::marker::PhantomData;

use crate::apply::{Apply, Error};
use crate::pipeline::batch::Batch;

pub fn drain<Stores>() -> Drain<Stores> {
    Drain(PhantomData)
}

pub struct Drain<Stores>(PhantomData<fn() -> Stores>);

impl<Stores> Clone for Drain<Stores> {
    fn clone(&self) -> Self {
        Drain(PhantomData)
    }
}

impl<Stores> Apply<Stores> for Drain<Stores>
where
    Stores: 'static,
{
    type Produces = ();
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<Stores>) -> Result<Option<()>, Error> {
        Ok(Some(()))
    }
}
