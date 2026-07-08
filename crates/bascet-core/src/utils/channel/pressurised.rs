use std::num::NonZeroU32;
use std::sync::Arc;

use kanal::{AsyncReceiver, AsyncSender, ReceiveError, SendError};

use crate::pipeline::consts::{
    PRESSURE_DECAY, PRESSURE_GROWTH, PRESSURE_INITIAL, PRESSURE_MIN, PRESSURE_STRAIN,
};
use crate::utils::AtomicPressure;

pub struct AsyncPressurisedSender<T> {
    sender: AsyncSender<T>,
    pressure: Arc<AtomicPressure>,
}

impl<T> Clone for AsyncPressurisedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            pressure: self.pressure.clone(),
        }
    }
}

impl<T> AsyncPressurisedSender<T> {
    pub fn channel(cap: usize) -> (Self, AsyncPressurisedReceiver<T>) {
        let (tx, rx) = kanal::bounded_async(cap);
        let pressure = Arc::new(AtomicPressure::new(
            PRESSURE_INITIAL,
            PRESSURE_MIN,
            NonZeroU32::new(PRESSURE_STRAIN).unwrap(),
            PRESSURE_GROWTH,
            PRESSURE_DECAY,
        ));
        (
            Self {
                sender: tx,
                pressure: pressure.clone(),
            },
            AsyncPressurisedReceiver {
                receiver: rx,
                pressure,
            },
        )
    }

    #[inline(always)]
    pub fn pressure(&self) -> &Arc<AtomicPressure> {
        &self.pressure
    }

    #[inline(always)]
    pub fn try_send_option(&self, opt: &mut Option<T>) -> Result<bool, SendError> {
        self.sender.as_sync().try_send_option(opt)
    }

    #[inline(always)]
    pub fn send(&self, val: T) -> Result<(), SendError> {
        let mut opt = Some(val);
        match self.sender.as_sync().try_send_option(&mut opt) {
            Ok(true) => {
                self.pressure.hit();
                Ok(())
            }
            Ok(false) => {
                self.pressure.miss();
                self.sender.as_sync().send(opt.unwrap())
            }
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub async fn send_async(&self, val: T) -> Result<(), SendError> {
        let mut opt = Some(val);
        match self.sender.as_sync().try_send_option(&mut opt) {
            Ok(true) => {
                self.pressure.hit();
                Ok(())
            }
            Ok(false) => {
                self.pressure.miss();
                self.sender.send(opt.unwrap()).await
            }
            Err(e) => Err(e),
        }
    }
}

pub struct AsyncPressurisedReceiver<T> {
    receiver: AsyncReceiver<T>,
    pressure: Arc<AtomicPressure>,
}

unsafe impl<T: Send> Send for AsyncPressurisedReceiver<T> {}

impl<T> Clone for AsyncPressurisedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            pressure: self.pressure.clone(),
        }
    }
}

impl<T> AsyncPressurisedReceiver<T> {
    #[inline(always)]
    pub fn pressure(&self) -> &Arc<AtomicPressure> {
        &self.pressure
    }

    #[inline(always)]
    pub fn recv_blocking(&self) -> Result<T, ReceiveError> {
        self.receiver.as_sync().recv()
    }

    #[inline(always)]
    pub fn try_recv(&self) -> Result<Option<T>, ReceiveError> {
        self.receiver.as_sync().try_recv()
    }

    #[inline(always)]
    pub async fn recv_async(&self) -> Result<T, ReceiveError> {
        self.receiver.recv().await
    }
}
