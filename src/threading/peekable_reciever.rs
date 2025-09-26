use crossbeam::channel::{Receiver, RecvError};

pub struct PeekableReceiver<T> {
    receiver: Receiver<T>,
    peeked: Option<T>,
}

unsafe impl<T: Send> Send for PeekableReceiver<T> {}
unsafe impl<T: Send> Sync for PeekableReceiver<T> {}

impl<T> PeekableReceiver<T> {
    #[inline(always)]
    pub fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver,
            peeked: None,
        }
    }

    #[inline(always)]
    pub fn peek(&mut self) -> Result<&T, RecvError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.receiver.recv()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    #[inline(always)]
    pub fn recv(&mut self) -> Result<T, RecvError> {
        if let Some(peeked) = self.peeked.take() {
            Ok(peeked)
        } else {
            self.receiver.recv()
        }
    }
}
