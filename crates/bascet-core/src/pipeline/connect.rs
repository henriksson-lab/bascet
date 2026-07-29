use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::Mutex;

use crate::apply::execute::{Assembled, Assign, Provides};
use crate::consts::{
    FLUSH_PATIENCE_DECAY, FLUSH_PATIENCE_GROWTH, FLUSH_PATIENCE_INIT, FLUSH_PATIENCE_MAX,
    FLUSH_PATIENCE_MIN,
};
use crate::pipeline::batch::{Batch, Keys, Len};
use crate::pipeline::builder::{Pipe, Pipeline, Source, Wanted};
use crate::pipeline::edge::{Downstream, Upstream};
use crate::pipeline::gather::{Gather, Probe};
use crate::runtime::RuntimeInner;
use crate::schedule::layer::{Dispatch, Layer, LayerState};
use crate::schedule::preempt::Preempt;
use crate::set::ops::partition::Compose;
use crate::set::{Lower, Set, Subset, Union};
use crate::utils::AtomicPatience;

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

    pub(crate) fn edge<T: Send + 'static>(&mut self) -> (Upstream<T>, Downstream<T>) {
        let (up, down) = Upstream::new(crate::consts::DEPTH);
        let closer_rx = Arc::downgrade(&up.input_rx);
        self.runtime.shutdown.register(Box::new(move || {
            if let Some(input_rx) = closer_rx.upgrade() {
                input_rx.close().ok();
            }
        }));
        (up, down)
    }

    pub(crate) fn register<A, W, U, Stores>(
        &mut self,
        apply: A,
        gather: U,
        downstream: Option<Downstream<Batch<Assembled<Stores, A, W>>>>,
        index: usize,
    ) where
        Stores: 'static,
        A: Assign<Stores>,
        A::Produces: Keys,
        Stores: Compose<Provides<Stores, A>, W, A::Produces>,
        Assembled<Stores, A, W>: Len + Send + 'static,
        U: Gather<Item = Batch<Stores>>,
        W: Set,
    {
        let preempt = Arc::new(AtomicU8::new(Preempt::Continue as u8));
        let patience = Arc::new(
            AtomicPatience::new(
                FLUSH_PATIENCE_INIT,
                FLUSH_PATIENCE_GROWTH,
                FLUSH_PATIENCE_DECAY,
            )
            .set_min(FLUSH_PATIENCE_MIN)
            .set_max(FLUSH_PATIENCE_MAX),
        );
        let probe_gather = gather.clone();
        let probe_tx = downstream
            .as_ref()
            .map(|downstream| Arc::clone(&downstream.output_tx));
        let probe: Box<dyn Fn() -> Probe + Send> = Box::new(move || {
            if probe_tx
                .as_ref()
                .is_some_and(|output_tx| output_tx.is_full() && output_tx.receiver_count() > 0)
            {
                return Probe::Full;
            }
            probe_gather.probe()
        });
        let runtime = Arc::downgrade(&self.runtime);
        let dispatch_preempt = Arc::clone(&preempt);
        let dispatch_patience = Arc::clone(&patience);
        let dispatch: Dispatch = Arc::new(Mutex::new(move || {
            apply.assign::<_, W>(
                &gather,
                &downstream,
                index,
                &dispatch_preempt,
                &dispatch_patience,
                &runtime,
            )
        }));
        self.layers[index] = Some(Layer {
            dispatch,
            state: LayerState::Open,
            probe,
            blocked: VecDeque::new(),
            parked: VecDeque::new(),
            workers: 0,
            pass: 0,
            preempt,
            patience,
        });
    }
}

pub(crate) trait Connect<W: Set> {
    type Stream: Gather;
    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream;
}

impl<A, W> Connect<W> for Source<A>
where
    A: Assign<()>,
    A::Produces: Keys,
    (): Compose<Provides<(), A>, W, A::Produces>,
    Assembled<(), A, W>: Len + Send + 'static,
    <A::Requires as Lower>::Out: Subset<()>,
    W: Set,
{
    type Stream = Upstream<Batch<Assembled<(), A, W>>>;

    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream {
        let index = build.index();
        build.upstream[consumer].push(index);
        let (up, down) = build.edge();
        build.register::<A, W, (), ()>(self.apply, (), Some(down), index);
        up
    }
}

impl<A, Stores, Tail, W> Connect<W> for Pipe<A, Stores, Tail>
where
    Stores: 'static,
    A: Assign<Stores>,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Stores: Keys,
    <Stores as Keys>::Output: Set,
    Assembled<Stores, A, W>: Len + Send + 'static,
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
        let (up, down) = build.edge();
        build.register::<A, W, Tail::Stream, Stores>(self.apply, upstream, Some(down), index);
        up
    }
}

pub(crate) trait Assemble<W: Set> {
    fn assemble(self, build: &mut Build) -> usize;
}

impl<W, A, Stores, Tail> Assemble<W> for Pipeline<Pipe<A, Stores, Tail>>
where
    W: Set,
    Stores: 'static,
    A: Assign<Stores>,
    A::Produces: Keys,
    Stores: Compose<Provides<Stores, A>, W, A::Produces>,
    Stores: Keys,
    <Stores as Keys>::Output: Set,
    Assembled<Stores, A, W>: Len + Send + 'static,
    <A::Requires as Lower>::Out: Union<W>,
    <A::Requires as Lower>::Out: Subset<<Stores as Keys>::Output>,
    Wanted<A, Stores, W>: Set,
    Tail: Connect<Wanted<A, Stores, W>>,
    Tail::Stream: Gather<Item = Batch<Stores>>,
{
    fn assemble(self, build: &mut Build) -> usize {
        let Pipe { apply, tail, .. } = self.chain;
        let sink = build.index();
        let stream = tail.connect(build, sink);
        build.register::<A, W, _, Stores>(apply, stream, None, sink);
        sink
    }
}
