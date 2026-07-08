use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::apply::Apply;
use crate::contract::Contract;
use crate::coordinate::Auto;
use crate::execute::{Async, Executable};
use crate::set::Set;
use crate::source::Pull;

pub struct Channel<T> {
    tx: Sender<T>,
    pub rx: Receiver<T>,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        let (tx, rx) = unbounded();
        Self { tx, rx }
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}

impl<T: Send + 'static> Contract for Channel<T> {
    type Input = T;
    type Output = ();
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl<T: Send + 'static> Apply for Channel<T> {
    type Runtime = Async;
    type Coordinate = Auto;

    fn apply<'this, W: Set>(
        &'this mut self,
        _want: &Pull,
        item: T,
    ) -> <Self::Runtime as Executable>::Outcome<'this, Self::Output> {
        let tx = self.tx.clone();
        Box::pin(async move {
            tx.send(item).ok();
            Ok(Some(()))
        })
    }
}
