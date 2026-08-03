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
        if downstream.is_exhausted() {
            self.staged = None;
            return true;
        }
        if self.staged.is_none() {
            return true;
        }
        match downstream.try_send(&mut self.staged) {
            Ok(true) => true,
            Ok(false) => false,
            Err(_) => {
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
            .is_some_and(|downstream| downstream.is_exhausted())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::edge::{Edge, Upstream};

    fn fuse(depth: usize) -> (Fuse<u32>, Upstream<u32>) {
        let (up, down) = Edge::new::<u32>(depth);
        (Fuse::new(Some(down)), up)
    }

    #[test]
    fn push_then_flush_sends() {
        let (mut out, up) = fuse(4);
        out.push(7);
        assert_eq!(up.try_recv().unwrap(), None);
        assert!(out.flush());
        assert_eq!(up.try_recv().unwrap(), Some(7));
    }

    #[test]
    fn backpressure_holds_then_flushes() {
        let (mut out, up) = fuse(1);
        out.push(1);
        assert!(out.flush());
        out.push(2);
        assert!(!out.flush());
        assert_eq!(up.try_recv().unwrap(), Some(1));
        assert!(out.flush());
        assert_eq!(up.try_recv().unwrap(), Some(2));
    }

    #[test]
    fn empty_flush_is_clean() {
        let (mut out, up) = fuse(1);
        assert!(out.flush());
        assert!(!out.residue());
        assert_eq!(up.try_recv().unwrap(), None);
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
