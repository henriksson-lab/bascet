use std::collections::VecDeque;

use crate::consts::DEPTH;
use crate::pipeline::edge::{Upstream, Zip};

pub struct Closed;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Probe {
    Ready,
    Starved,
    Exhausted,
    Full,
}

pub trait Gather: Clone + Send + 'static {
    type Item;
    fn try_recv(&mut self) -> Result<Option<Self::Item>, Closed>;
    fn probe(&self) -> Probe;
    fn residue(&self) -> bool;
}

impl<T: Send + 'static> Gather for Upstream<T> {
    type Item = T;

    fn try_recv(&mut self) -> Result<Option<T>, Closed> {
        if let Some(item) = self.outstanding.pop_front() {
            return Ok(Some(item));
        }
        if self.exhausted {
            return Err(Closed);
        }
        match self.input_rx.try_recv() {
            Ok(Some(batch)) => {
                self.outstanding = VecDeque::from(batch);
                Ok(self.outstanding.pop_front())
            }
            Ok(None) => Ok(None),
            Err(_) => {
                self.exhausted = true;
                Err(Closed)
            }
        }
    }

    fn probe(&self) -> Probe {
        if !self.outstanding.is_empty() || !self.input_rx.is_empty() {
            Probe::Ready
        } else if self.done() {
            Probe::Exhausted
        } else {
            Probe::Starved
        }
    }

    fn residue(&self) -> bool {
        !self.outstanding.is_empty()
    }
}

impl Gather for () {
    type Item = ();

    fn try_recv(&mut self) -> Result<Option<()>, Closed> {
        Ok(Some(()))
    }

    fn probe(&self) -> Probe {
        Probe::Ready
    }

    fn residue(&self) -> bool {
        false
    }
}

impl<A: Send + 'static> Gather for (Upstream<A>,) {
    type Item = A;

    fn try_recv(&mut self) -> Result<Option<A>, Closed> {
        Gather::try_recv(&mut self.0)
    }

    fn probe(&self) -> Probe {
        Gather::probe(&self.0)
    }

    fn residue(&self) -> bool {
        Gather::residue(&self.0)
    }
}

bascet_variadic::variadic!(N = 2..=16, for N in N => {
    impl<@N[A~#: Send + 'static](sep=",")> Gather for Zip<((@N[Upstream<A~#>](sep=","),), (@N[Option<A~#>](sep=","),)), (@N[Option<A~#>](sep=","),)> {
        type Item = (@N[Option<A~#>](sep=","),);

        fn try_recv(&mut self) -> Result<Option<Self::Item>, Closed> {
            if let Some(row) = self.outstanding.pop_front() {
                return Ok(Some(row));
            }
            let mut guard = self.inner.lock();
            let (members, row) = &mut *guard;
            loop {
                let mut starving = false;
                @N[if row.#.is_none() {
                    match Gather::try_recv(&mut members.#) {
                        Ok(Some(item)) => row.# = Some(item),
                        Ok(None) => starving = true,
                        Err(Closed) => {}
                    }
                }]
                if starving {
                    break;
                }
                let taken = (@N[row.#.take()](sep=","),);
                if @N[taken.#.is_none()](sep=" && ") {
                    drop(guard);
                    return match self.outstanding.pop_front() {
                        Some(row) => Ok(Some(row)),
                        None => Err(Closed),
                    };
                }
                self.outstanding.push_back(taken);
                if self.outstanding.len() >= DEPTH {
                    break;
                }
            }
            drop(guard);
            Ok(self.outstanding.pop_front())
        }

        fn probe(&self) -> Probe {
            if !self.outstanding.is_empty() {
                return Probe::Ready;
            }
            let guard = self.inner.lock();
            let (members, row) = &*guard;
            let mut starving = false;
            let mut drained = true;
            @N[if row.#.is_some() {
                drained = false;
            } else {
                match Gather::probe(&members.#) {
                    Probe::Starved => {
                        starving = true;
                        drained = false;
                    }
                    Probe::Exhausted => {}
                    _ => drained = false,
                }
            }]
            if drained {
                Probe::Exhausted
            } else if starving {
                Probe::Starved
            } else {
                Probe::Ready
            }
        }

        fn residue(&self) -> bool {
            !self.outstanding.is_empty()
        }
    }
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outstanding_serves_in_order_then_starves() {
        let (mut up, down) = Upstream::<u32>::new(4);
        down.output_tx.send(vec![1, 2]).unwrap();
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(2))));
        assert!(matches!(Gather::try_recv(&mut up), Ok(None)));
        assert!(matches!(up.probe(), Probe::Starved));
        assert!(!up.residue());
    }

    #[test]
    fn closed_only_after_outstanding_drains() {
        let (mut up, down) = Upstream::<u32>::new(4);
        down.output_tx.send(vec![1]).unwrap();
        drop(down);
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
        assert!(matches!(Gather::try_recv(&mut up), Err(Closed)));
        assert!(matches!(up.probe(), Probe::Exhausted));
    }

    #[test]
    fn residue_reports_undrained_outstanding() {
        let (mut up, down) = Upstream::<u32>::new(4);
        down.output_tx.send(vec![1, 2, 3]).unwrap();
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
        assert!(up.residue());
        drop(down);
    }

    #[test]
    fn source_gather_never_starves() {
        let mut unit = ();
        assert!(matches!(Gather::try_recv(&mut unit), Ok(Some(()))));
        assert!(matches!(Gather::probe(&()), Probe::Ready));
        assert!(!Gather::residue(&()));
    }

    #[test]
    fn uneven_batches_pair_in_order() {
        let (up_a, down_a) = Upstream::<u32>::new(4);
        let (up_b, down_b) = Upstream::<u32>::new(4);
        down_a.output_tx.send(vec![1]).unwrap();
        down_a.output_tx.send(vec![2, 3]).unwrap();
        down_b.output_tx.send(vec![10, 20, 30]).unwrap();
        let mut gather = Zip::from((up_a, up_b));
        assert!(matches!(gather.try_recv(), Ok(Some((Some(1), Some(10))))));
        assert!(matches!(gather.try_recv(), Ok(Some((Some(2), Some(20))))));
        assert!(matches!(gather.try_recv(), Ok(Some((Some(3), Some(30))))));
        assert!(matches!(gather.try_recv(), Ok(None)));
        drop(down_a);
        drop(down_b);
    }

    #[test]
    fn survivor_drains_with_none_slots() {
        let (up_a, down_a) = Upstream::<u32>::new(4);
        let (up_b, down_b) = Upstream::<u32>::new(4);
        down_a.output_tx.send(vec![1]).unwrap();
        down_b.output_tx.send(vec![10, 20]).unwrap();
        drop(down_a);
        drop(down_b);
        let mut gather = Zip::from((up_a, up_b));
        assert!(matches!(gather.try_recv(), Ok(Some((Some(1), Some(10))))));
        assert!(matches!(gather.try_recv(), Ok(Some((None, Some(20))))));
        assert!(matches!(gather.try_recv(), Err(Closed)));
    }

    #[test]
    fn clones_share_staging_but_not_outstanding() {
        let (up_a, down_a) = Upstream::<u32>::new(4);
        let (up_b, down_b) = Upstream::<u32>::new(4);
        down_a.output_tx.send(vec![1]).unwrap();
        let mut first = Zip::from((up_a, up_b));
        let mut second = first.clone();
        assert!(matches!(first.try_recv(), Ok(None)));
        down_b.output_tx.send(vec![10]).unwrap();
        assert!(matches!(second.try_recv(), Ok(Some((Some(1), Some(10))))));
        drop(down_a);
        drop(down_b);
        assert!(matches!(first.try_recv(), Err(Closed)));
    }
}
