use std::future::Future;

use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::layer::Layer;
use crate::set::Set;
use crate::sink::Sink;

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

impl<T: Send + 'static> Layer for Channel<T> {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl<T: Send + 'static> Sink for Channel<T> {
    type Input<'a> = T;

    fn consume<W: Set>(&mut self, item: Self::Input<'_>) -> impl Future<Output = ()> + Send {
        let tx = self.tx.clone();
        async move {
            tx.send(item).ok();
        }
    }
}
