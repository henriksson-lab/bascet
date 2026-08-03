use std::collections::VecDeque;
use std::sync::Arc;

use kanal::{Receiver, Sender};
use parking_lot::Mutex;

use crate::pipeline::gather::Closed;

pub(crate) struct Edge;

pub(crate) struct Upstream<T> {
    input_rx: Receiver<T>,
}

pub(crate) struct Downstream<T> {
    output_tx: Sender<T>,
    exhausted: bool,
}

impl Edge {
    pub(crate) fn new<T>(depth: usize) -> (Upstream<T>, Downstream<T>) {
        let (output_tx, input_rx) = kanal::bounded(depth);
        (
            Upstream { input_rx },
            Downstream {
                output_tx,
                exhausted: false,
            },
        )
    }
}

impl<T> Clone for Upstream<T> {
    fn clone(&self) -> Self {
        Self {
            input_rx: self.input_rx.clone(),
        }
    }
}

impl<T> Clone for Downstream<T> {
    fn clone(&self) -> Self {
        Self {
            output_tx: self.output_tx.clone(),
            exhausted: self.exhausted,
        }
    }
}

impl<T> Upstream<T> {
    pub(crate) fn try_recv(&self) -> Result<Option<T>, Closed> {
        match self.input_rx.try_recv() {
            Ok(item) => Ok(item),
            Err(_) => Err(Closed),
        }
    }

    pub(crate) fn close(&self) {
        self.input_rx.close().ok();
    }
}

impl<T> Downstream<T> {
    pub(crate) fn send(&self, item: T) -> Result<(), Closed> {
        self.output_tx.send(item).map_err(|_| Closed)
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn try_send(&mut self, item: &mut Option<T>) -> Result<bool, Closed> {
        match self.output_tx.try_send_option(item) {
            Ok(sent) => Ok(sent),
            Err(_) => {
                self.exhausted = true;
                Err(Closed)
            }
        }
    }
}

pub(crate) struct Zip<T, R> {
    pub(crate) inner: Arc<Mutex<T>>,
    pub(crate) outstanding: VecDeque<R>,
}

impl<T, R> Clone for Zip<T, R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            outstanding: VecDeque::new(),
        }
    }
}

bascet_variadic::variadic!(N = 2..=16, for N in N => {
    impl<@N[A~#](sep=",")> From<(@N[Upstream<A~#>](sep=","),)> for Zip<((@N[Upstream<A~#>](sep=","),), (@N[Option<A~#>](sep=","),)), (@N[Option<A~#>](sep=","),)> {
        fn from(members: (@N[Upstream<A~#>](sep=","),)) -> Self {
            Self {
                inner: Arc::new(Mutex::new((members, (@N[None::<A~#>](sep=","),)))),
                outstanding: VecDeque::new(),
            }
        }
    }
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_drop_closes_for_producer() {
        let (up, down) = Edge::new::<u32>(1);
        drop(up);
        assert!(down.send(1).is_err());
    }

    #[test]
    fn sender_drop_closes_after_drain() {
        let (up, down) = Edge::new::<u32>(4);
        down.send(1).unwrap();
        drop(down);
        assert_eq!(up.try_recv().unwrap(), Some(1));
        assert!(up.try_recv().is_err());
    }

    #[test]
    fn clones_share_the_channel() {
        let (up, down) = Edge::new::<u32>(4);
        let view = up.clone();
        down.send(7).unwrap();
        assert_eq!(view.try_recv().unwrap(), Some(7));
        assert_eq!(up.try_recv().unwrap(), None);
    }
}
