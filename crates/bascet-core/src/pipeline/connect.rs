use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::Mutex;

use crate::apply::execute::{Dispatch, Work};
use crate::pipeline::builder::{Pipe, Pipeline, Source, Wanted};
use crate::pipeline::edge::{Downstream, Upstream};
use crate::pipeline::gather::{Gather, Probe};
use crate::runtime::RuntimeInner;
use crate::schedule::layer::{Assignment, Layer};
use crate::schedule::preempt::Preempt;
use crate::set::{Join, Set};

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

    pub(crate) fn register<A, M, W, U>(
        &mut self,
        apply: A,
        gather: U,
        downstream: Option<Downstream<A::Output>>,
        index: usize,
    ) where
        A: Dispatch<M>,
        A::Output: Send + 'static,
        U: Gather<Item = A::Input>,
        W: Set,
        M: 'static,
    {
        let preempt = Arc::new(AtomicU8::new(Preempt::Continue as u8));
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
        let dispatch: Arc<Mutex<dyn FnMut() -> Box<dyn Assignment> + Send>> =
            Arc::new(Mutex::new(move || {
                apply.dispatch::<_, W>(&gather, &downstream, index, &dispatch_preempt, &runtime)
            }));
        self.layers[index] = Some(Layer {
            dispatch: Some(dispatch),
            probe,
            blocked: VecDeque::new(),
            parked: VecDeque::new(),
            workers: 0,
            pass: 0,
            preempt,
        });
    }
}

pub(crate) trait Connect<W: Set> {
    type Stream: Gather;
    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream;
}

impl<A, M, W> Connect<W> for Source<A, M>
where
    A: Dispatch<M> + Work<M, Input = ()>,
    A::Output: Send + 'static,
    M: 'static,
    W: Set,
{
    type Stream = Upstream<A::Output>;

    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream {
        let index = build.index();
        build.upstream[consumer].push(index);
        let (up, down) = build.edge();
        build.register::<A, M, W, ()>(self.apply, (), Some(down), index);
        up
    }
}

impl<A, M, Tail, W> Connect<W> for Pipe<A, M, Tail>
where
    A: Dispatch<M>,
    A::Output: Send + 'static,
    A::Requires: Join<W>,
    Wanted<A, M, W>: Set,
    Tail: Connect<Wanted<A, M, W>>,
    Tail::Stream: Gather<Item = A::Input>,
    M: 'static,
    W: Set,
{
    type Stream = Upstream<A::Output>;

    fn connect(self, build: &mut Build, consumer: usize) -> Self::Stream {
        let index = build.index();
        build.upstream[consumer].push(index);
        let upstream = self.tail.connect(build, index);
        let (up, down) = build.edge();
        build.register::<A, M, W, Tail::Stream>(self.apply, upstream, Some(down), index);
        up
    }
}

pub(crate) trait Assemble<W: Set> {
    fn assemble(self, build: &mut Build) -> usize;
}

impl<W, A, M, Tail> Assemble<W> for Pipeline<Pipe<A, M, Tail>>
where
    W: Set,
    A: Dispatch<M> + Work<M, Output = ()>,
    A::Requires: Join<W>,
    Wanted<A, M, W>: Set,
    Tail: Connect<Wanted<A, M, W>>,
    Tail::Stream: Gather<Item = A::Input>,
    M: 'static,
{
    fn assemble(self, build: &mut Build) -> usize {
        let Pipe { apply, tail, .. } = self.chain;
        let sink = build.index();
        let stream = tail.connect(build, sink);
        build.register::<A, M, W, _>(apply, stream, None, sink);
        sink
    }
}
