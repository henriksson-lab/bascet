use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crossbeam::channel::{Receiver, RecvError, TryRecvError};

struct KVPair<K, V> {
    key: K,
    value: V,
}

impl<K: Ord, V> Ord for KVPair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
impl<K: Ord, V> PartialOrd for KVPair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, V> Eq for KVPair<K, V> {}
impl<K: Ord, V> PartialEq for KVPair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

pub struct OrderedReceiver<K, V, KF>
where
    K: Ord,
    KF: Fn(&K) -> K,
{
    inner_receiver: Receiver<(K, V)>,
    inner_ordered: BinaryHeap<Reverse<KVPair<K, V>>>,
    key_next: K,
    fn_key_next: KF,
}

unsafe impl<K: Ord + Send, V: Send, KF: Fn(&K) -> K> Send for OrderedReceiver<K, V, KF> {}

impl<K, V, KF> OrderedReceiver<K, V, KF>
where
    K: Ord,
    KF: Fn(&K) -> K,
{
    pub fn new(rx: Receiver<(K, V)>, key_initial: K, fn_key_next: KF) -> Self {
        Self {
            inner_receiver: rx,
            inner_ordered: BinaryHeap::new(),
            key_next: key_initial,
            fn_key_next,
        }
    }

    #[inline]
    pub fn recv_ordered(&mut self) -> Result<V, RecvError> {
        loop {
            // Check if next expected item is ready
            if let Some(Reverse(kv)) = self.inner_ordered.peek() {
                if kv.key == self.key_next {
                    let Reverse(kv) = self.inner_ordered.pop().unwrap();
                    self.key_next = (self.fn_key_next)(&self.key_next);
                    return Ok(kv.value);
                }
            }

            // Next item not ready, try to receive more
            match self.inner_receiver.recv() {
                Ok((key, value)) => {
                    self.inner_ordered.push(Reverse(KVPair { key, value }));
                }
                Err(e) => {
                    // Channel disconnected. Since indices are dense, if the next item
                    // isn't in the heap, it will NEVER be. Remaining ordered items
                    // will be drained on subsequent calls
                    return Err(e);
                }
            }
        }
    }

    #[inline]
    pub fn try_recv_ordered(&mut self) -> Result<Option<V>, TryRecvError> {
        // Check if next expected item is ready
        if let Some(Reverse(kv)) = self.inner_ordered.peek() {
            if kv.key == self.key_next {
                let Reverse(kv) = self.inner_ordered.pop().unwrap();
                self.key_next = (self.fn_key_next)(&self.key_next);
                return Ok(Some(kv.value));
            }
        }

        // Next item not ready, drain all available items from channel
        loop {
            match self.inner_receiver.try_recv() {
                Ok((key, value)) => {
                    self.inner_ordered.push(Reverse(KVPair { key, value }));

                    // Check if the next expected item is now ready
                    if let Some(Reverse(kv)) = self.inner_ordered.peek() {
                        if kv.key == self.key_next {
                            let Reverse(kv) = self.inner_ordered.pop().unwrap();
                            self.key_next = (self.fn_key_next)(&self.key_next);
                            return Ok(Some(kv.value));
                        }
                    }
                }
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => {
                    // Channel disconnected. Since indices are dense, if the next item
                    // isn't in the heap, it will NEVER be. Remaining ordered items
                    // will be drained on subsequent calls
                    return Err(TryRecvError::Disconnected);
                }
            }
        }
    }

    #[inline(always)]
    pub fn receiver(&self) -> &Receiver<(K, V)> {
        &self.inner_receiver
    }
}
