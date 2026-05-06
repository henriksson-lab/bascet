use crossbeam::channel::{Receiver, RecvError, Sender, TryRecvError};
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};
use tracing::warn;

use crate::threading::spinpark_loop::{self, SPINPARK_COUNTOF_PARKS_BEFORE_WARN, SpinPark};

pub fn ordered_dense<T, const N: usize>() -> (OrderedDenseSender<T, N>, OrderedDenseReceiver<T, N>)
{
    let (tx, rx) = crossbeam::channel::unbounded();

    let fastpath = Arc::new(OrderedDenseFastpathInner {
        base: AtomicUsize::new(0),
        receiver_closed: AtomicBool::new(false),
        state: (0..N).map(|_| AtomicU8::new(SLOT_EMPTY)).collect(),
        ordered: (0..N)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect(),
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
    pub receiver_closed: AtomicBool,
    pub state: Vec<AtomicU8>,
    pub ordered: Vec<UnsafeCell<MaybeUninit<T>>>,
}

const SLOT_EMPTY: u8 = 0;
const SLOT_WRITING: u8 = 1;
const SLOT_FULL: u8 = 2;
const SLOWPATH_MAX_GAP_MULTIPLIER: usize = 16;

impl<T, const N: usize> Drop for OrderedDenseFastpathInner<T, N> {
    fn drop(&mut self) {
        for (state, slot) in self.state.iter().zip(&self.ordered) {
            if state.load(Ordering::Acquire) == SLOT_FULL {
                unsafe {
                    (*slot.get()).assume_init_drop();
                }
            }
        }
    }
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
        if self.inner_fastpath.receiver_closed.load(Ordering::Acquire) {
            return;
        }

        let current_base = self.inner_fastpath.base.load(Ordering::Acquire);

        if index < current_base {
            return;
        }

        if index < current_base.saturating_add(N) {
            let slot_idx = index % N;
            if self.inner_fastpath.state[slot_idx]
                .compare_exchange(
                    SLOT_EMPTY,
                    SLOT_WRITING,
                    Ordering::Acquire,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                unsafe {
                    (*self.inner_fastpath.ordered[slot_idx].get()).write(value);
                }
                self.inner_fastpath.state[slot_idx].store(SLOT_FULL, Ordering::Release);
            } else {
                let _ = self.inner_slowpath_tx.send((index, value));
            }
        } else {
            let _ = self.inner_slowpath_tx.send((index, value));
        }
    }

    pub fn wait_until_sendable(&self, index: usize) -> bool {
        let mut spinpark_counter = 0;

        loop {
            if self.inner_fastpath.receiver_closed.load(Ordering::Acquire) {
                return false;
            }

            let current_base = self.inner_fastpath.base.load(Ordering::Acquire);

            if index < current_base {
                return false;
            }

            if index < current_base.saturating_add(N) {
                return true;
            }

            match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(
                &mut spinpark_counter,
            ) {
                SpinPark::Spun => {}
                SpinPark::Warn => {
                    warn!(
                        source = "OrderedSender::wait_until_sendable",
                        index,
                        current_base,
                        window = N,
                        "waiting for ordered window"
                    );
                }
                SpinPark::Parked => {}
            }
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

impl<T, const N: usize> Drop for OrderedDenseReceiver<T, N> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T, const N: usize> OrderedDenseReceiver<T, N> {
    pub fn close(&self) {
        self.inner_fastpath
            .receiver_closed
            .store(true, Ordering::Release);
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        let mut spinpark_counter = 0;

        loop {
            self.discard_stale_slowpath_prefix();

            let slot_idx = self.inner_next % N;
            if self.inner_fastpath.state[slot_idx].load(Ordering::Acquire) == SLOT_FULL {
                let val =
                    unsafe { (*self.inner_fastpath.ordered[slot_idx].get()).assume_init_read() };
                self.inner_fastpath.state[slot_idx].store(SLOT_EMPTY, Ordering::Release);
                self.inner_next += 1;
                self.inner_fastpath
                    .base
                    .store(self.inner_next, Ordering::Release);
                return Ok(val);
            }

            if self.inner_slowpath_disconnected {
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

            match spinpark_loop::spinpark_loop::<100, SPINPARK_COUNTOF_PARKS_BEFORE_WARN>(
                &mut spinpark_counter,
            ) {
                SpinPark::Spun => {
                    // We assume the slot is going to be set soon and not be recieved out of order
                    continue;
                }
                SpinPark::Warn => {
                    warn!(
                        source = "OrderedReceiver::recv",
                        "waiting for ordered value"
                    );
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
                        if idx < self.inner_slowpath.base {
                            continue;
                        }

                        let offset = idx - self.inner_slowpath.base;
                        let max_offset = N.saturating_mul(SLOWPATH_MAX_GAP_MULTIPLIER).max(N);
                        if offset > max_offset {
                            panic!(
                                "ordered_dense received an item too far ahead of the next expected item: \
                                 index={idx}, next_expected={}, offset={offset}, max_offset={max_offset}, \
                                 fastpath_window={N}, slowpath_gap_multiplier={SLOWPATH_MAX_GAP_MULTIPLIER}. \
                                 This usually means an earlier producer task is stuck, panicked, or failed to send \
                                 its result while later tasks kept completing. For BBGZ/TIRP inputs, suspect a \
                                 corrupt or pathological compressed block around the next_expected index, or too \
                                 much out-of-order decode work. Try rerunning with fewer decode threads or a \
                                 smaller stream buffer; if it is reproducible at the same next_expected value, \
                                 inspect or regenerate the input around that block.",
                                self.inner_slowpath.base,
                            );
                        }
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

    fn discard_stale_slowpath_prefix(&mut self) {
        while self.inner_slowpath.base < self.inner_next {
            self.inner_slowpath.ordered.pop_front();
            self.inner_slowpath.base += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn drops_unreceived_fastpath_values() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = ordered_dense::<DropCounter, 4>();

        tx.send(0, DropCounter(Arc::clone(&drops)));
        tx.send(1, DropCounter(Arc::clone(&drops)));

        drop(rx);
        drop(tx);

        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn ignores_stale_slowpath_values() {
        let (tx, mut rx) = ordered_dense::<usize, 2>();

        tx.send(0, 10);
        assert_eq!(rx.recv().unwrap(), 10);

        tx.send(0, 20);
        drop(tx);

        assert!(rx.recv().is_err());
        assert!(rx.inner_slowpath.ordered.len() < 10);
    }

    #[test]
    fn blocked_sender_returns_when_receiver_is_dropped() {
        let (tx, rx) = ordered_dense::<usize, 2>();

        let handle = std::thread::spawn(move || tx.send(100, 10));
        std::thread::sleep(Duration::from_millis(10));
        drop(rx);

        handle.join().unwrap();
    }
}
