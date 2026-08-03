use std::sync::Arc;

use crate::apply::Apply;
use crate::apply::execute::{Assembled, Provides};
use crate::pipeline::batch::{Batch, Keys};
use crate::pipeline::builder::{Pipe, Pipeline, Source, Wanted};
use crate::pipeline::edge::{Downstream, Edge, Upstream};
use crate::pipeline::gather::Gather;
use crate::runtime::RuntimeInner;
use crate::schedule::layer::{Dispatch, Layer};
use crate::set::partition::Compose;
use crate::set::{Lower, Set, Subset, Union};
use crate::worker::synchronous::Task;

pub(crate) struct Build {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) layers: Vec<Option<Layer>>,
    pub(crate) upstream: Vec<Vec<usize>>,
}

impl Build {
    pub(crate) fn index(&mut self) -> usize {
        self.layers.push(None);
        self.upstream.push(Vec::new());
        self.layers.len() - 1
    }

    pub(crate) fn register<A, W, U, Stores>(
        &mut self,
        apply: A,
        gather: U,
        downstream: Option<Downstream<Batch<Assembled<Stores, A, W>>>>,
        index: usize,
    ) where
        A: Apply<Stores>,
        A::Produces: Keys,
        Stores: Compose<Provides<Stores, A>, W, A::Produces>,
        Assembled<Stores, A, W>: Send + 'static,
        U: Gather<Item = Batch<Stores>>,
        W: Set,
    {
        let dispatch: Dispatch =
            Box::new(move |layer: &Arc<Layer>| Task::new(&apply, &gather, &downstream, layer));
        let layer = Layer::new(dispatch, &self.runtime);
        self.layers[index] = Some(layer);
    }
}

pub(crate) trait Connect<W: Set> {
    type Stream: Gather;
    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream;
}

impl<A, W> Connect<W> for Source<A>
where
    A: Apply<()>,
    A::Produces: Keys,
    (): Compose<Provides<(), A>, W, A::Produces>,
    Assembled<(), A, W>: Send + 'static,
    <A::Requires as Lower>::Out: Subset<()>,
    W: Set,
{
    type Stream = Upstream<Batch<Assembled<(), A, W>>>;

    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream {
        let index = build.index();
        build.upstream[consumer].push(index);
        let (up, down) = Edge::new(crate::consts::DEPTH);
        build.register::<A, W, (), ()>(self.apply, (), Some(down), index);
        up
    }
}

impl<A, Stores, Tail, W> Connect<W> for Pipe<A, Stores, Tail>
where
    A: Apply<Stores>,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Stores: Keys,
    <Stores as Keys>::Output: Set,
    Assembled<Stores, A, W>: Send + 'static,
    <A::Requires as Lower>::Out: Union<W>,
    <A::Requires as Lower>::Out: Subset<<Stores as Keys>::Output>,
    Wanted<A, Stores, W>: Set,
    Tail: Connect<Wanted<A, Stores, W>>,
    Tail::Stream: Gather<Item = Batch<Stores>>,
    W: Set,
{
    type Stream = Upstream<Batch<Assembled<Stores, A, W>>>;

    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream {
        let index = build.index();
        build.upstream[consumer].push(index);
        let upstream = self.tail.connect(build, index);
        let (up, down) = Edge::new(crate::consts::DEPTH);
        build.register::<A, W, Tail::Stream, Stores>(self.apply, upstream, Some(down), index);
        up
    }
}

pub(crate) trait Assemble {
    fn assemble(self, build: &mut Build) -> usize;
}

impl<A, Stores, Tail> Assemble for Pipeline<Pipe<A, Stores, Tail>>
where
    A: Apply<Stores>,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, (), A::Produces>,
    Stores: Keys,
    <Stores as Keys>::Output: Set,
    Assembled<Stores, A, ()>: Send + 'static,
    <A::Requires as Lower>::Out: Set,
    <A::Requires as Lower>::Out: Subset<<Stores as Keys>::Output>,
    Tail: Connect<<A::Requires as Lower>::Out>,
    Tail::Stream: Gather<Item = Batch<Stores>>,
{
    fn assemble(self, build: &mut Build) -> usize {
        let Pipe { apply, tail, .. } = self.chain;
        let sink = build.index();
        let stream = tail.connect(build, sink);
        build.register::<A, (), Tail::Stream, Stores>(apply, stream, None, sink);
        sink
    }
}
