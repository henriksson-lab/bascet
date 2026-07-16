use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Weak};

use crate::apply::emit::Emit;
use crate::apply::{Apply, ApplyAsync};
use crate::consts::{YIELD_CAP, YIELD_MIN, YIELD_START};
use crate::pipeline::edge::Downstream;
use crate::pipeline::gather::Gather;
use crate::runtime::RuntimeInner;
use crate::schedule::layer::Assignment;
use crate::set::Set;
use crate::utils::Patience;
use crate::worker::synchronous::Run;

pub struct Synchronous;
pub struct Asynchronous;

pub trait Work<M>: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;
}

pub(crate) trait Dispatch<M>: Work<M> {
    fn dispatch<U, W>(
        &self,
        gather: &U,
        downstream: &Option<Downstream<Self::Output>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = Self::Input>,
        W: Set;
}

impl<A: Apply> Work<Synchronous> for A {
    type Input = A::Input;
    type Output = A::Output;
    type Provides = A::Provides;
    type Requires = A::Requires;
}

impl<A: Apply> Dispatch<Synchronous> for A
where
    A::Output: Send + 'static,
{
    fn dispatch<U, W>(
        &self,
        gather: &U,
        downstream: &Option<Downstream<A::Output>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = A::Input>,
        W: Set,
    {
        Box::new(Run {
            apply: self.clone(),
            gather: gather.clone(),
            emit: Emit::<A::Output, W>::new(downstream.clone()),
            layer,
            preempt: Arc::clone(preempt),
            runtime: runtime.clone(),
            budget: Patience::new(YIELD_START, YIELD_START, YIELD_START)
                .set_min(YIELD_MIN)
                .set_max(YIELD_CAP),
            round: 0,
            finalized: false,
        })
    }
}

impl<A: ApplyAsync> Work<Asynchronous> for A {
    type Input = A::Input;
    type Output = A::Output;
    type Provides = A::Provides;
    type Requires = A::Requires;
}

impl<A: ApplyAsync> Dispatch<Asynchronous> for A {
    fn dispatch<U, W>(
        &self,
        _gather: &U,
        _downstream: &Option<Downstream<A::Output>>,
        _layer: usize,
        _preempt: &Arc<AtomicU8>,
        _runtime: &Weak<RuntimeInner>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = A::Input>,
        W: Set,
    {
        unimplemented!("async worker execution is deferred with the compio tiers")
    }
}
