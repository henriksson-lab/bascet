use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ops::Range;
use std::sync::{Arc, OnceLock, Weak};

use kanal::{AsyncReceiver, AsyncSender};

use super::consts::{
    PRESSURE_DECAY, PRESSURE_GROWTH, PRESSURE_INITIAL, PRESSURE_MIN, PRESSURE_STRAIN,
};
use super::scheduler::{Petitioner, Signal};
use crate::source::Pull;
use crate::utils::AtomicPressure;

pub(crate) struct Edge<T> {
    inner: Arc<Inner<T>>,
    _type: PhantomData<fn() -> T>,
}

struct Inner<T> {
    pull_tx: Weak<AsyncSender<Pull>>,
    pull_rx: Weak<AsyncReceiver<Pull>>,
    output_tx: Weak<AsyncSender<T>>,
    output_rx: Weak<AsyncReceiver<T>>,
    pull_pressure: Arc<AtomicPressure>,
    output_pressure: Arc<AtomicPressure>,
    output_receiver: Arc<OnceLock<Petitioner>>,
    pull_receiver: Arc<OnceLock<Petitioner>>,
}

pub(crate) struct Upstream<T> {
    pull_tx: Arc<AsyncSender<Pull>>,
    output_rx: Arc<AsyncReceiver<T>>,
    edge: Edge<T>,
}

pub(crate) struct Downstream<T> {
    pull_rx: Arc<AsyncReceiver<Pull>>,
    output_tx: Arc<AsyncSender<T>>,
    edge: Edge<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Closed;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Miss<T> {
    Full(T),
    Closed(T),
}

impl<T> Clone for Edge<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _type: PhantomData,
        }
    }
}

impl<T> Clone for Upstream<T> {
    fn clone(&self) -> Self {
        Self {
            pull_tx: self.pull_tx.clone(),
            output_rx: self.output_rx.clone(),
            edge: self.edge.clone(),
        }
    }
}

impl<T> Clone for Downstream<T> {
    fn clone(&self) -> Self {
        Self {
            pull_rx: self.pull_rx.clone(),
            output_tx: self.output_tx.clone(),
            edge: self.edge.clone(),
        }
    }
}

impl<T> Edge<T> {
    pub(crate) fn new(depth: usize) -> (Upstream<T>, Downstream<T>) {
        let (pull_tx, pull_rx) = kanal::bounded_async(depth);
        let (output_tx, output_rx) = kanal::bounded_async(depth);
        let pull_tx = Arc::new(pull_tx);
        let pull_rx = Arc::new(pull_rx);
        let output_tx = Arc::new(output_tx);
        let output_rx = Arc::new(output_rx);
        let edge = Edge {
            inner: Arc::new(Inner {
                pull_tx: Arc::downgrade(&pull_tx),
                pull_rx: Arc::downgrade(&pull_rx),
                output_tx: Arc::downgrade(&output_tx),
                output_rx: Arc::downgrade(&output_rx),
                pull_pressure: Self::pressure(),
                output_pressure: Self::pressure(),
                output_receiver: Arc::new(OnceLock::new()),
                pull_receiver: Arc::new(OnceLock::new()),
            }),
            _type: PhantomData,
        };

        (
            Upstream {
                pull_tx: Arc::clone(&pull_tx),
                output_rx: Arc::clone(&output_rx),
                edge: edge.clone(),
            },
            Downstream {
                pull_rx,
                output_tx,
                edge,
            },
        )
    }

    pub(crate) fn upstream(&self) -> Option<Upstream<T>> {
        Some(Upstream {
            pull_tx: self.inner.pull_tx.upgrade()?,
            output_rx: self.inner.output_rx.upgrade()?,
            edge: self.clone(),
        })
    }

    pub(crate) fn downstream(&self) -> Option<Downstream<T>> {
        Some(Downstream {
            pull_rx: self.inner.pull_rx.upgrade()?,
            output_tx: self.inner.output_tx.upgrade()?,
            edge: self.clone(),
        })
    }

    fn pressure() -> Arc<AtomicPressure> {
        Arc::new(AtomicPressure::new(
            PRESSURE_INITIAL,
            PRESSURE_MIN,
            NonZeroU32::new(PRESSURE_STRAIN).unwrap(),
            PRESSURE_GROWTH,
            PRESSURE_DECAY,
        ))
    }

    fn promote_pull(&self) {
        if let Some(level) = self.inner.pull_pressure.miss() {
            if let Some(petitioner) = self.inner.pull_receiver.get() {
                petitioner.promote(Signal::Pressure(
                    Arc::clone(&self.inner.pull_pressure),
                    level,
                ));
            }
        }
    }

    fn promote_output(&self) {
        if let Some(level) = self.inner.output_pressure.miss() {
            if let Some(petitioner) = self.inner.output_receiver.get() {
                petitioner.promote(Signal::Pressure(
                    Arc::clone(&self.inner.output_pressure),
                    level,
                ));
            }
        }
    }
}

impl<T> Upstream<T> {
    pub(crate) fn edge(&self) -> Edge<T> {
        self.edge.clone()
    }

    pub(crate) fn set_output_receiver(&self, petitioner: Petitioner) {
        self.edge.inner.output_receiver.set(petitioner).ok();
    }

    pub(crate) fn promote_upstream(&self) {
        self.edge.promote_pull();
    }

    #[allow(dead_code)]
    pub(crate) fn next(&self) -> Result<T, Closed> {
        self.pull(Pull::Next)?;
        self.take()
    }

    #[allow(dead_code)]
    pub(crate) async fn next_async(&self) -> Result<T, Closed> {
        self.pull_async(Pull::Next).await?;
        self.take_async().await
    }

    #[allow(dead_code)]
    pub(crate) fn range(&self, range: Range<u64>) -> Result<T, Closed> {
        self.pull(Pull::Read(range))?;
        self.take()
    }

    #[allow(dead_code)]
    pub(crate) async fn range_async(&self, range: Range<u64>) -> Result<T, Closed> {
        self.pull_async(Pull::Read(range)).await?;
        self.take_async().await
    }

    #[allow(dead_code)]
    fn pull(&self, pull: Pull) -> Result<(), Closed> {
        self.pull_tx.as_sync().send(pull).map_err(|_| Closed)
    }

    pub(crate) async fn pull_async(&self, pull: Pull) -> Result<(), Closed> {
        let mut opt = Some(pull);
        match self.pull_tx.as_sync().try_send_option(&mut opt) {
            Ok(true) => {
                self.edge.inner.pull_pressure.hit();
                Ok(())
            }
            Ok(false) => {
                self.edge.promote_pull();
                self.pull_tx.send(opt.unwrap()).await.map_err(|_| Closed)
            }
            Err(_) => Err(Closed),
        }
    }

    pub(crate) fn try_pull(&self, pull: Pull) -> Result<(), Miss<Pull>> {
        let mut opt = Some(pull);
        match self.pull_tx.as_sync().try_send_option(&mut opt) {
            Ok(true) => {
                self.edge.inner.pull_pressure.hit();
                Ok(())
            }
            Ok(false) => {
                self.edge.promote_pull();
                Err(Miss::Full(opt.unwrap()))
            }
            Err(_) => Err(Miss::Closed(opt.unwrap())),
        }
    }

    pub(crate) fn take(&self) -> Result<T, Closed> {
        self.output_rx.as_sync().recv().map_err(|_| Closed)
    }

    pub(crate) fn try_take(&self) -> Result<Option<T>, Closed> {
        self.output_rx.as_sync().try_recv().map_err(|_| Closed)
    }

    pub(crate) async fn take_async(&self) -> Result<T, Closed> {
        self.output_rx.recv().await.map_err(|_| Closed)
    }
}

impl<T> Downstream<T> {
    pub(crate) fn edge(&self) -> Edge<T> {
        self.edge.clone()
    }

    pub(crate) fn set_pull_receiver(&self, petitioner: Petitioner) {
        self.edge.inner.pull_receiver.set(petitioner).ok();
    }

    pub(crate) fn try_recv_pull(&self) -> Result<Option<Pull>, Closed> {
        self.pull_rx.as_sync().try_recv().map_err(|_| Closed)
    }

    #[allow(dead_code)]
    pub(crate) fn pull(&self) -> Result<Pull, Closed> {
        self.pull_rx.as_sync().recv().map_err(|_| Closed)
    }

    pub(crate) async fn pull_async(&self) -> Result<Pull, Closed> {
        self.pull_rx.recv().await.map_err(|_| Closed)
    }

    pub(crate) fn try_send(&self, output: T) -> Result<(), Miss<T>> {
        let mut opt = Some(output);
        match self.output_tx.as_sync().try_send_option(&mut opt) {
            Ok(true) => {
                self.edge.inner.output_pressure.hit();
                Ok(())
            }
            Ok(false) => {
                self.edge.promote_output();
                Err(Miss::Full(opt.unwrap()))
            }
            Err(_) => Err(Miss::Closed(opt.unwrap())),
        }
    }
}
