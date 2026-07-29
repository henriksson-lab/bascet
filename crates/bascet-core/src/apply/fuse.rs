use crate::pipeline::edge::Downstream;

pub struct Fuse<Out> {
    downstream: Option<Downstream<Out>>,
    staged: Option<Out>,
}

impl<Out> Fuse<Out> {
    pub(crate) fn new(downstream: Option<Downstream<Out>>) -> Self {
        Self {
            downstream,
            staged: None,
        }
    }

    pub(crate) fn push(&mut self, item: Out) {
        if self.downstream.is_some() {
            self.staged = Some(item);
        }
    }

    pub(crate) fn flush(&mut self) -> bool {
        let Some(downstream) = &mut self.downstream else {
            self.staged = None;
            return true;
        };
        if downstream.exhausted {
            self.staged = None;
            return true;
        }
        if self.staged.is_none() {
            return true;
        }
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

    pub(crate) fn residue(&self) -> bool {
        self.staged.is_some()
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

    fn fuse(depth: usize) -> (Fuse<u32>, Upstream<u32>) {
        let (up, down) = Upstream::<u32>::new(depth);
        (Fuse::new(Some(down)), up)
    }

    #[test]
    fn push_then_flush_sends() {
        let (mut out, up) = fuse(4);
        out.push(7);
        assert!(up.input_rx.is_empty());
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(7));
    }

    #[test]
    fn backpressure_holds_then_flushes() {
        let (mut out, up) = fuse(1);
        out.push(1);
        assert!(out.flush());
        out.push(2);
        assert!(!out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(1));
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(2));
    }

    #[test]
    fn empty_flush_is_clean() {
        let (mut out, up) = fuse(1);
        assert!(out.flush());
        assert!(!out.residue());
        assert!(up.input_rx.is_empty());
    }

    #[test]
    fn orphaned_when_consumer_gone() {
        let (mut out, up) = fuse(4);
        drop(up);
        out.push(1);
        assert!(out.flush());
        assert!(out.orphaned());
        assert!(!out.residue());
    }

    #[test]
    fn sink_drops_silently() {
        let mut out = Fuse::<u32>::new(None);
        out.push(1);
        assert!(out.flush());
        assert!(!out.orphaned());
        assert!(!out.residue());
    }
}
