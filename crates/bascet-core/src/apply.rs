use std::marker::PhantomData;

use crate::contract::Contract;
use crate::coordinate::Auto;
use crate::execute::{Error, Executable, Sync};
use crate::owned::Owned;
use crate::pipe::Pipe;
use crate::schedule::Schedule;
use crate::set::Set;
use crate::source::Pull;

pub trait Apply: Contract + Sized {
    type Runtime: Executable;
    type Coordinate;

    fn apply<'this, W: Set>(
        &'this mut self,
        want: &Pull,
        input: Self::Input,
    ) -> <Self::Runtime as Executable>::Outcome<'this, Self::Output>;
}

pub(crate) trait Scheduled:
    Apply + Owned<Schedule, Value = Schedule> + Clone + Send + 'static
{
}

impl<T> Scheduled for T where T: Apply + Owned<Schedule, Value = Schedule> + Clone + Send + 'static {}

#[derive(Clone)]
pub struct ApplyFn<In, Out, F> {
    f: F,
    _types: PhantomData<(In, Out)>,
}

impl<In, Out, F> ApplyFn<In, Out, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _types: PhantomData,
        }
    }
}

impl<In, Out, F> Contract for ApplyFn<In, Out, F>
where
    In: Set,
    Out: Set,
{
    type Input = In;
    type Output = Out;
    type Provides = Out;
    type Requires = In;
    type Resources = ();
}

impl<In, Out, F> Apply for ApplyFn<In, Out, F>
where
    In: Set,
    Out: Set,
    F: FnMut(&Pull, In) -> Result<Option<Out>, Error>,
{
    type Runtime = Sync;
    type Coordinate = Auto;

    fn apply<W: Set>(&mut self, want: &Pull, input: Self::Input) -> Result<Option<Self::Output>, Error> {
        (self.f)(want, input)
    }
}

impl<S: Apply> Contract for Pipe<S, ()> {
    type Input = S::Input;
    type Output = S::Output;
    type Provides = S::Provides;
    type Requires = S::Requires;
    type Resources = S::Resources;
}

impl<S, Tail> Contract for Pipe<S, Tail>
where
    S: Apply,
    Tail: Apply,
    Tail::Output: Into<S::Input>,
{
    type Input = Tail::Input;
    type Output = S::Output;
    type Provides = S::Provides;
    type Requires = Tail::Requires;
    type Resources = ();
}

impl<S: Apply<Runtime = Sync>> Apply for Pipe<S, ()> {
    type Runtime = Sync;
    type Coordinate = Auto;

    fn apply<W: Set>(&mut self, want: &Pull, input: Self::Input) -> Result<Option<Self::Output>, Error> {
        self.0.apply::<W>(want, input)
    }
}

impl<S, Tail> Apply for Pipe<S, Tail>
where
    S: Apply<Runtime = Sync>,
    Tail: Apply<Runtime = Sync>,
    Tail::Output: Into<S::Input>,
{
    type Runtime = Sync;
    type Coordinate = Auto;

    fn apply<W: Set>(&mut self, want: &Pull, input: Self::Input) -> Result<Option<Self::Output>, Error> {
        match self.1.apply::<W>(want, input) {
            Ok(Some(out)) => self.0.apply::<W>(want, out.into()),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
