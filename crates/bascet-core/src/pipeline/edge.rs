use std::collections::VecDeque;
use std::sync::Arc;

use kanal::{Receiver, Sender};
use parking_lot::Mutex;

pub(crate) struct Upstream<T> {
    pub(crate) input_rx: Arc<Receiver<Vec<T>>>,
    pub(crate) outstanding: VecDeque<T>,
    pub(crate) exhausted: bool,
}

pub(crate) struct Downstream<T> {
    pub(crate) output_tx: Arc<Sender<Vec<T>>>,
    pub(crate) exhausted: bool,
}

impl<T> Clone for Upstream<T> {
    fn clone(&self) -> Self {
        Self {
            input_rx: Arc::clone(&self.input_rx),
            outstanding: VecDeque::new(),
            exhausted: self.exhausted,
        }
    }
}

impl<T> Clone for Downstream<T> {
    fn clone(&self) -> Self {
        Self {
            output_tx: Arc::clone(&self.output_tx),
            exhausted: self.exhausted,
        }
    }
}

impl<T> Upstream<T> {
    pub(crate) fn new(depth: usize) -> (Upstream<T>, Downstream<T>) {
        let (output_tx, input_rx) = kanal::bounded(depth);
        (
            Upstream {
                input_rx: Arc::new(input_rx),
                outstanding: VecDeque::new(),
                exhausted: false,
            },
            Downstream {
                output_tx: Arc::new(output_tx),
                exhausted: false,
            },
        )
    }

    pub(crate) fn done(&self) -> bool {
        self.exhausted || self.input_rx.sender_count() == 0
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
        let (up, down) = Upstream::<u32>::new(1);
        drop(up);
        assert!(down.output_tx.send(vec![1]).is_err());
    }

    #[test]
    fn sender_drop_drains_before_close() {
        let (up, down) = Upstream::<u32>::new(4);
        down.output_tx.send(vec![1]).unwrap();
        assert!(!up.done());
        drop(down);
        assert!(up.done());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![1]));
        assert!(up.input_rx.try_recv().is_err() || up.input_rx.try_recv().unwrap().is_none());
    }

    #[test]
    fn clones_share_the_channel() {
        let (up, down) = Upstream::<u32>::new(4);
        let view = up.clone();
        down.output_tx.send(vec![7]).unwrap();
        assert_eq!(view.input_rx.try_recv().unwrap(), Some(vec![7]));
        assert!(view.outstanding.is_empty());
    }
}
