use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Weak};

use crate::apply::Apply;
use crate::apply::fuse::Fuse;
use crate::pipeline::batch::{Batch, Keys, Len};
use crate::pipeline::edge::Downstream;
use crate::pipeline::gather::Gather;
use crate::runtime::RuntimeInner;
use crate::schedule::layer::Assignment;
use crate::set::Set;
use crate::set::ops::partition::Compose;
use crate::utils::AtomicPatience;
use crate::worker::synchronous::Run;

pub type Provides<Stores, A> = <<A as Apply<Stores>>::Produces as Keys>::Output;

pub type Assembled<Stores, A, W> =
    <Stores as Compose<Provides<Stores, A>, W, <A as Apply<Stores>>::Produces>>::Output;

pub(crate) trait Assign<Stores>: Apply<Stores> {
    fn assign<U, W>(
        &self,
        gather: &U,
        downstream: &Option<Downstream<Batch<Assembled<Stores, Self, W>>>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        patience: &Arc<AtomicPatience>,
        runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = Batch<Stores>>,
        W: Set,
        Self::Produces: Keys,
        Stores: Compose<Provides<Stores, Self>, W, Self::Produces>,
        Assembled<Stores, Self, W>: Len + Send + 'static;
}

impl<Stores, A> Assign<Stores> for A
where
    Stores: 'static,
    A: Apply<Stores>,
{
    fn assign<U, W>(
        &self,
        gather: &U,
        downstream: &Option<Downstream<Batch<Assembled<Stores, A, W>>>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        patience: &Arc<AtomicPatience>,
        runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = Batch<Stores>>,
        W: Set,
        A::Produces: Keys,
        Stores: Compose<Provides<Stores, A>, W, A::Produces>,
        Assembled<Stores, A, W>: Len + Send + 'static,
    {
        Box::new(Run {
            apply: self.clone(),
            gather: gather.clone(),
            fuse: Fuse::new(downstream.clone()),
            layer,
            preempt: Arc::clone(preempt),
            patience: Arc::clone(patience),
            runtime: runtime.clone(),
            finalized: false,
        })
    }
}
