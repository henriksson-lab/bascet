use std::marker::PhantomData;

use crate::apply::{Apply, Emit, Error};
use crate::set::Set;

pub fn channel<T: Send + 'static>() -> (Channel<T>, kanal::Receiver<T>) {
    let (out_tx, out_rx) = kanal::unbounded();
    (Channel { out_tx }, out_rx)
}

pub struct Channel<T> {
    out_tx: kanal::Sender<T>,
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            out_tx: self.out_tx.clone(),
        }
    }
}

impl<T: Send + 'static> Apply for Channel<T> {
    type Input = T;
    type Output = ();
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: T, _: &mut Emit<(), W>) -> Result<(), Error> {
        self.out_tx.send(input).map_err(|_| ())
    }
}

pub fn drain<T: Send + 'static>() -> Drain<T> {
    Drain(PhantomData)
}

pub struct Drain<T>(PhantomData<T>);

impl<T> Clone for Drain<T> {
    fn clone(&self) -> Self {
        Drain(PhantomData)
    }
}

impl<T: Send + 'static> Apply for Drain<T> {
    type Input = T;
    type Output = ();
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _: T, _: &mut Emit<(), W>) -> Result<(), Error> {
        Ok(())
    }
}
