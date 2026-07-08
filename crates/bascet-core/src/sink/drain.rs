use std::marker::PhantomData;

use crate::coordinate::Auto;
use crate::contract::Contract;
use crate::set::Set;
use crate::source::Pull;
use crate::apply::Apply;
use crate::execute::{Async, Executable};

pub struct Drain<T>(PhantomData<T>);

impl<T> Default for Drain<T> {
    fn default() -> Self {
        Drain(PhantomData)
    }
}

impl<T> Clone for Drain<T> {
    fn clone(&self) -> Self {
        Drain(PhantomData)
    }
}

impl<T: Send + 'static> Contract for Drain<T> {
    type Input = T;
    type Output = ();
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl<T: Send + 'static> Apply for Drain<T> {
    type Runtime = Async;
    type Coordinate = Auto;

    fn apply<'this, W: Set>(
        &'this mut self,
        _want: &Pull,
        _input: T,
    ) -> <Self::Runtime as Executable>::Outcome<'this, Self::Output> {
        Box::pin(async { Ok(Some(())) })
    }
}
