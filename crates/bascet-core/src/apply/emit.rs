use std::marker::PhantomData;

use crate::attr::Attr;
use crate::pipeline::edge::Downstream;
use crate::set::Set;

pub struct Emit<Out, Wants: Set> {
    downstream: Option<Downstream<Out>>,
    buffer: Vec<Out>,
    staged: Option<Vec<Out>>,
    finished: bool,
    _wants: PhantomData<fn() -> Wants>,
}

impl<Out, Wants: Set> Emit<Out, Wants> {
    pub(crate) fn new(downstream: Option<Downstream<Out>>) -> Self {
        Self {
            downstream,
            buffer: Vec::new(),
            staged: None,
            finished: false,
            _wants: PhantomData,
        }
    }

    pub fn push(&mut self, item: Out) {
        if self.downstream.is_some() {
            self.buffer.push(item);
        }
    }

    pub(crate) fn flush(&mut self) -> bool {
        let Some(downstream) = &mut self.downstream else {
            self.buffer.clear();
            return true;
        };
        if downstream.exhausted {
            self.staged = None;
            self.buffer.clear();
            return true;
        }
        if self.staged.is_some() {
            match downstream.output_tx.try_send_option(&mut self.staged) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(_) => {
                    downstream.exhausted = true;
                    self.staged = None;
                    self.buffer.clear();
                    return true;
                }
            }
        }
        if self.buffer.is_empty() {
            return true;
        }
        let capacity = self.buffer.len();
        self.staged = Some(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(capacity),
        ));
        match downstream.output_tx.try_send_option(&mut self.staged) {
            Ok(true) => true,
            Ok(false) => false,
            Err(_) => {
                downstream.exhausted = true;
                self.staged = None;
                true
            }
        }
    }

    pub(crate) fn full(&self) -> bool {
        self.staged.is_some()
    }

    pub(crate) fn residue(&self) -> bool {
        self.staged.is_some() || !self.buffer.is_empty()
    }

    pub fn wants<A: Attr>(&self) -> bool {
        Wants::contains::<A>()
    }

    pub fn finish(&mut self) {
        self.finished = true;
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished || self.orphaned()
    }

    pub(crate) fn orphaned(&self) -> bool {
        self.downstream
            .as_ref()
            .is_some_and(|downstream| downstream.exhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::edge::Upstream;

    fn emit(depth: usize) -> (Emit<u32, ()>, Upstream<u32>) {
        let (up, down) = Upstream::<u32>::new(depth);
        (Emit::new(Some(down)), up)
    }

    #[test]
    fn push_buffers_and_flush_sends_one_batch() {
        let (mut out, up) = emit(4);
        out.push(7);
        out.push(9);
        assert!(up.input_rx.is_empty());
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![7, 9]));
    }

    #[test]
    fn refused_flush_stages_the_batch_without_loss() {
        let (mut out, up) = emit(1);
        out.push(1);
        assert!(out.flush());
        out.push(2);
        assert!(!out.flush());
        assert!(out.full());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![1]));
        assert!(out.flush());
        assert!(!out.full());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![2]));
    }

    #[test]
    fn staged_flushes_before_buffer() {
        let (mut out, up) = emit(1);
        out.push(1);
        assert!(out.flush());
        out.push(2);
        assert!(!out.flush());
        out.push(3);
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![1]));
        assert!(!out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![2]));
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![3]));
    }

    #[test]
    fn empty_flush_is_clean() {
        let (mut out, up) = emit(1);
        assert!(out.flush());
        assert!(!out.residue());
        assert!(up.input_rx.is_empty());
    }

    #[test]
    fn orphaned_when_consumer_gone() {
        let (mut out, up) = emit(4);
        drop(up);
        out.push(1);
        assert!(out.flush());
        assert!(out.orphaned());
        assert!(out.finished());
        assert!(!out.residue());
    }

    #[test]
    fn sink_drops_silently() {
        let mut out = Emit::<u32, ()>::new(None);
        out.push(1);
        assert!(out.flush());
        assert!(!out.orphaned());
        assert!(!out.residue());
    }
}
