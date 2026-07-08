use crossbeam::channel::{Receiver, Sender, TryRecvError};

pub struct PeekableReceiver<T> {
    receiver: Receiver<T>,
    peeked: Option<T>,
}

unsafe impl<T: Send> Send for PeekableReceiver<T> {}

impl<T> PeekableReceiver<T> {
    pub fn channel() -> (Sender<T>, Self) {
        let (tx, rx) = crossbeam::channel::unbounded();
        (tx, Self::new(rx))
    }

    #[inline(always)]
    pub fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver,
            peeked: None,
        }
    }

    #[inline(always)]
    pub fn reciever(&self) -> &Receiver<T> {
        &self.receiver
    }

    #[inline(always)]
    pub fn peek(&mut self) -> Result<&T, TryRecvError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.receiver.try_recv()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(peeked) = self.peeked.take() {
            Ok(peeked)
        } else {
            self.receiver.try_recv()
        }
    }
}
