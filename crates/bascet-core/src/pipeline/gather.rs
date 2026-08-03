use crate::consts::DEPTH;
use crate::pipeline::batch::Batch;
use crate::pipeline::edge::{Upstream, Zip};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Closed;

pub trait Gather: Clone + Send + 'static {
    type Item;
    fn try_recv(&mut self) -> Result<Option<Self::Item>, Closed>;
    fn residue(&self) -> bool;
}

impl<T: Send + 'static> Gather for Upstream<T> {
    type Item = T;

    fn try_recv(&mut self) -> Result<Option<T>, Closed> {
        Upstream::try_recv(self)
    }

    fn residue(&self) -> bool {
        false
    }
}

impl Gather for () {
    type Item = Batch<()>;

    fn try_recv(&mut self) -> Result<Option<Batch<()>>, Closed> {
        Ok(Some(Batch::new(())))
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

        fn residue(&self) -> bool {
            !self.outstanding.is_empty()
        }
    }
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::edge::Edge;

    #[test]
    fn recv_serves_then_starves() {
        let (mut up, down) = Edge::new::<u32>(4);
        down.send(1).unwrap();
        down.send(2).unwrap();
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(2))));
        assert!(matches!(Gather::try_recv(&mut up), Ok(None)));
        assert!(!up.residue());
    }

    #[test]
    fn closed_after_drain() {
        let (mut up, down) = Edge::new::<u32>(4);
        down.send(1).unwrap();
        drop(down);
        assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
        assert!(matches!(Gather::try_recv(&mut up), Err(Closed)));
    }

    #[test]
    fn source_gather_never_starves() {
        let mut unit = ();
        assert!(matches!(Gather::try_recv(&mut unit), Ok(Some(_))));
        assert!(!Gather::residue(&()));
    }

    #[test]
    fn uneven_batches_pair_in_order() {
        let (up_a, down_a) = Edge::new::<u32>(4);
        let (up_b, down_b) = Edge::new::<u32>(4);
        down_a.send(1).unwrap();
        down_a.send(2).unwrap();
        down_a.send(3).unwrap();
        down_b.send(10).unwrap();
        down_b.send(20).unwrap();
        down_b.send(30).unwrap();
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
        let (up_a, down_a) = Edge::new::<u32>(4);
        let (up_b, down_b) = Edge::new::<u32>(4);
        down_a.send(1).unwrap();
        down_b.send(10).unwrap();
        down_b.send(20).unwrap();
        drop(down_a);
        drop(down_b);
        let mut gather = Zip::from((up_a, up_b));
        assert!(matches!(gather.try_recv(), Ok(Some((Some(1), Some(10))))));
        assert!(matches!(gather.try_recv(), Ok(Some((None, Some(20))))));
        assert!(matches!(gather.try_recv(), Err(Closed)));
    }

    #[test]
    fn clones_share_staging_but_not_outstanding() {
        let (up_a, down_a) = Edge::new::<u32>(4);
        let (up_b, down_b) = Edge::new::<u32>(4);
        down_a.send(1).unwrap();
        let mut first = Zip::from((up_a, up_b));
        let mut second = first.clone();
        assert!(matches!(first.try_recv(), Ok(None)));
        down_b.send(10).unwrap();
        assert!(matches!(second.try_recv(), Ok(Some((Some(1), Some(10))))));
        drop(down_a);
        drop(down_b);
        assert!(matches!(first.try_recv(), Err(Closed)));
    }
}
