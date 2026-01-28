use crossbeam::channel::{Receiver, RecvError, Sender, TryRecvError};
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use bascet_runtime::logging::warn;
use crate::threading::spinpark_loop::{self, SpinPark, SPINPARK_COUNTOF_PARKS_BEFORE_WARN};

pub fn ordered_dense<T, const N: usize>() -> (OrderedDenseSender<T, N>, OrderedDenseReceiver<T, N>)
{
    let (tx, rx) = crossbeam::channel::unbounded();

    let fastpath = Arc::new(OrderedDenseFastpathInner {
        base: AtomicUsize::new(0),
        is_init: Box::new(std::array::from_fn(|_| AtomicBool::new(false))),
        ordered: Box::new(std::array::from_fn(|_| {
            UnsafeCell::new(MaybeUninit::uninit())
        })),
    });
    let slowpath = OrderedDenseSlowpathInner {
        base: 0,
        ordered: VecDeque::with_capacity(N),
    };

    (
        OrderedDenseSender {
            inner_fastpath: Arc::clone(&fastpath),
            inner_slowpath_tx: tx,
        },
        OrderedDenseReceiver {
            inner_next: 0,
            inner_fastpath: Arc::clone(&fastpath),
            inner_slowpath: slowpath,
            inner_slowpath_rx: rx,
            inner_slowpath_disconnected: false,
        },
    )
}

pub struct OrderedDenseFastpathInner<T, const N: usize> {
    pub base: AtomicUsize,
    pub is_init: Box<[AtomicBool; N]>,
    pub ordered: Box<[UnsafeCell<MaybeUninit<T>>; N]>,
}

pub struct OrderedDenseSlowpathInner<T> {
    pub base: usize,
    pub ordered: VecDeque<Option<T>>,
}

unsafe impl<T: Send, const N: usize> Sync for OrderedDenseFastpathInner<T, N> {}

pub struct OrderedDenseSender<T, const N: usize> {
    inner_fastpath: Arc<OrderedDenseFastpathInner<T, N>>,
    inner_slowpath_tx: Sender<(usize, T)>,
}

impl<T, const N: usize> Clone for OrderedDenseSender<T, N> {
    fn clone(&self) -> Self {
        Self {
            inner_fastpath: Arc::clone(&self.inner_fastpath),
            inner_slowpath_tx: self.inner_slowpath_tx.clone(),
        }
    }
}

impl<T, const N: usize> OrderedDenseSender<T, N> {
    pub fn send(&self, index: usize, value: T) {
        let current_base = self.inner_fastpath.base.load(Ordering::Acquire);

        if index >= current_base && index < current_base + N {
            let slot_idx = index % N;
            unsafe {
                (*self.inner_fastpath.ordered[slot_idx].get()).write(value);
            }
            self.inner_fastpath.is_init[slot_idx].store(true, Ordering::Release);
        } else {
            // eprintln!("[OrderedSender::send] sending to slow path");
            let _ = self.inner_slowpath_tx.send((index, value));
        }
    }
}

pub struct OrderedDenseReceiver<T, const N: usize> {
    inner_next: usize,

    pub inner_fastpath: Arc<OrderedDenseFastpathInner<T, N>>,

    pub inner_slowpath: OrderedDenseSlowpathInner<T>,
    inner_slowpath_rx: Receiver<(usize, T)>,
    inner_slowpath_disconnected: bool,
}

impl<T, const N: usize> OrderedDenseReceiver<T, N> {
    pub fn recv(&mut self) -> Result<T, RecvError> {
        let mut spinpark_counter = 0;

        loop {
            let slot_idx = self.inner_next % N;
            if self.inner_fastpath.is_init[slot_idx].load(Ordering::Acquire) == true {
                let val =
                    unsafe { (*self.inner_fastpath.ordered[slot_idx].get()).assume_init_read() };
                self.inner_fastpath.is_init[slot_idx].store(false, Ordering::Release);
                self.inner_next += 1;
                self.inner_fastpath
                    .base
                    .store(self.inner_next, Ordering::Release);
                return Ok(val);
            }

            if self.inner_slowpath_disconnected {
                // Align slowpath base with inner_next by popping
                while self.inner_slowpath.base < self.inner_next {
                    self.inner_slowpath.ordered.pop_front();
                    self.inner_slowpath.base += 1;
                }

                if self
                    .inner_slowpath
                    .ordered
                    .front()
                    .map(|v| v.is_some())
                    .unwrap_or(false)
                {
                    let val = unsafe {
                        self.inner_slowpath
                            .ordered
                            .pop_front()
                            .unwrap_unchecked()
                            .unwrap_unchecked()
                    };
                    self.inner_next += 1;
                    self.inner_slowpath.base += 1;
                    self.inner_fastpath
                        .base
                        .store(self.inner_next, Ordering::Release);

                    return Ok(val);
                }
                return Err(RecvError);
            }

            match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(&mut spinpark_counter) {
                SpinPark::Spun => {
                    // We assume the slot is going to be set soon and not be recieved out of order
                    continue;
                }
                SpinPark::Warn => {
                    warn!(source = "OrderedReceiver::recv", "waiting for ordered value");
                }
                SpinPark::Parked => {}
            }

            // NOTE:  inner_slowpath.ordered is indexed relative to next_expected. If
            //          the value there is some this is always going to be == next_expected
            if self
                .inner_slowpath
                .ordered
                .front()
                .map(|v| v.is_some())
                .unwrap_or(false)
            {
                let val = unsafe {
                    self.inner_slowpath
                        .ordered
                        .pop_front()
                        // SAFETY:  Guaranteed by above condition
                        .unwrap_unchecked()
                        .unwrap_unchecked()
                };
                self.inner_next += 1;
                self.inner_slowpath.base += 1;
                self.inner_fastpath
                    .base
                    .store(self.inner_next, Ordering::Release);
                return Ok(val);
            }

            loop {
                match self.inner_slowpath_rx.try_recv() {
                    Ok((idx, val)) => {
                        let offset = idx - self.inner_slowpath.base;
                        if offset == 0 {
                            self.inner_next += 1;
                            self.inner_slowpath.base += 1;
                            self.inner_fastpath
                                .base
                                .store(self.inner_next, Ordering::Release);
                            return Ok(val);
                        }
                        if offset >= self.inner_slowpath.ordered.len() {
                            self.inner_slowpath.ordered.resize_with(offset + 1, || None);
                        }
                        self.inner_slowpath.ordered[offset] = Some(val);
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.inner_slowpath_disconnected = true;
                        break;
                    }
                }
            }
        }
    }
}
