use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use kanal::{AsyncReceiver, AsyncSender, ReceiveError, SendError};

use crate::pipeline::consts::{
    PRESSURE_DECAY, PRESSURE_GROWTH, PRESSURE_INITIAL, PRESSURE_MIN, PRESSURE_STRAIN,
};
use crate::utils::Pressure;

const GROUP_UNSET: usize = usize::MAX;

pub fn async_pressurised<T>(
    cap: usize,
) -> (AsyncPressurisedSender<T>, AsyncPressurisedReceiver<T>) {
    let (tx, rx) = kanal::bounded_async(cap);
    let pressure = Arc::new(Pressure::new(
        PRESSURE_INITIAL,
        PRESSURE_MIN,
        NonZeroU32::new(PRESSURE_STRAIN).unwrap(),
        PRESSURE_GROWTH,
        PRESSURE_DECAY,
    ));
    let group_idx = Arc::new(AtomicUsize::new(GROUP_UNSET));
    (
        AsyncPressurisedSender {
            sender: tx,
            pressure: pressure.clone(),
            group_idx: group_idx.clone(),
        },
        AsyncPressurisedReceiver {
            receiver: rx,
            pressure,
            group_idx,
        },
    )
}

pub struct AsyncPressurisedSender<T> {
    sender: AsyncSender<T>,
    pressure: Arc<Pressure>,
    group_idx: Arc<AtomicUsize>,
}

impl<T> Clone for AsyncPressurisedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            pressure: self.pressure.clone(),
            group_idx: self.group_idx.clone(),
        }
    }
}

impl<T> AsyncPressurisedSender<T> {
    #[inline(always)]
    pub fn pressure(&self) -> &Arc<Pressure> {
        &self.pressure
    }

    #[inline(always)]
    pub fn group_idx(&self) -> Option<usize> {
        match self.group_idx.load(Ordering::Acquire) {
            GROUP_UNSET => None,
            group_idx => Some(group_idx),
        }
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
            Ok(false) => self.sender.as_sync().send(opt.unwrap()),
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub async fn send_async(&self, val: T) -> Result<(), SendError> {
        self.sender.send(val).await
    }
}

pub struct AsyncPressurisedReceiver<T> {
    receiver: AsyncReceiver<T>,
    pressure: Arc<Pressure>,
    group_idx: Arc<AtomicUsize>,
}

unsafe impl<T: Send> Send for AsyncPressurisedReceiver<T> {}

impl<T> Clone for AsyncPressurisedReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            pressure: self.pressure.clone(),
            group_idx: self.group_idx.clone(),
        }
    }
}

impl<T> AsyncPressurisedReceiver<T> {
    #[inline(always)]
    pub fn pressure(&self) -> &Arc<Pressure> {
        &self.pressure
    }

    #[inline(always)]
    pub fn set_group_idx(&self, group_idx: usize) {
        self.group_idx.store(group_idx, Ordering::Release);
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
