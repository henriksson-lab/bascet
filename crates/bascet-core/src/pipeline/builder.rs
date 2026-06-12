use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tracing::error;
use tracing::warn;

use super::consts::{STALL_HARD, STALL_WARN};
use crate::owned::Owned;
use crate::pipe::Pipe;
use crate::set::{Set, Subset, Union};
use crate::sink::Sink;
use crate::sink::drain::Drain;
use crate::source::Pull;
use crate::stage::Output;
use crate::traits;
use crate::utils::channel::{self as channel};

use super::consts::{REQ_DEPTH_MAX, RES_QUEUE_MAX};
use super::pipeline::{Metrics, Pipeline, Runner, Runtime, make_runtime};
use super::scheduler::Scheduler;
use super::shutdown::Shutdown;
use super::stage::Connect;

pub struct PipelineBuilder<Provides, Source, Stages, Resources, Sink = ()>
where
    Provides: Set,
{
    pub source: Source,
    pub stages: Stages,
    pub resources: Resources,
    pub sink: Sink,
    _provides: PhantomData<Provides>,
}

#[derive(Clone)]
pub(super) struct Build {
    pub runtime: Runtime,
    pub metrics: Metrics,
    pub shutdown: Shutdown,
    pub petition_tx: crossbeam::channel::Sender<super::scheduler::Petition>,
}

impl PipelineBuilder<(), (), (), (), ()> {
    pub fn new() -> Self {
        PipelineBuilder {
            source: (),
            stages: (),
            resources: (),
            sink: (),
            _provides: PhantomData,
        }
    }
}

impl<Provides, Stages, Resources, Sink> PipelineBuilder<Provides, (), Stages, Resources, Sink>
where
    Provides: Set,
{
    pub fn source<Source>(
        self,
        source: Source,
    ) -> PipelineBuilder<Source::Provides, Source, Stages, Resources, Sink>
    where
        Source: traits::Source,
    {
        PipelineBuilder {
            source,
            stages: self.stages,
            resources: self.resources,
            sink: self.sink,
            _provides: PhantomData,
        }
    }
}

impl<Provides, Source, Stages, Sink> PipelineBuilder<Provides, Source, Stages, (), Sink>
where
    Provides: Set,
{
    pub fn resource<Resources>(
        self,
        resources: Resources,
    ) -> PipelineBuilder<Provides, Source, Stages, Resources, Sink> {
        PipelineBuilder {
            source: self.source,
            stages: self.stages,
            resources,
            sink: self.sink,
            _provides: PhantomData,
        }
    }
}

impl<Provides, Source, Stages, Resources, Sink>
    PipelineBuilder<Provides, Source, Stages, Resources, Sink>
where
    Provides: Set,
    Source: traits::Source,
{
    pub fn stage<S>(
        self,
        stage: S,
    ) -> PipelineBuilder<
        <Provides as Union<S::Provides>>::Output,
        Source,
        Pipe<S, Stages>,
        Resources,
        Sink,
    >
    where
        S: traits::Stage,
        S::Requires: Subset<Provides>,
        Resources: Owned<S::Resources>,
        Provides: Union<S::Provides>,
        <Provides as Union<S::Provides>>::Output: Set,
    {
        PipelineBuilder {
            source: self.source,
            stages: Pipe(stage, self.stages),
            resources: self.resources,
            sink: self.sink,
            _provides: PhantomData,
        }
    }
}

impl<Provides, Source, Stages, Resources> PipelineBuilder<Provides, Source, Stages, Resources, ()>
where
    Provides: Set,
    Source: traits::Source,
{
    pub fn sink<Sink>(
        self,
        sink: Sink,
    ) -> PipelineBuilder<Provides, Source, Stages, Resources, Sink>
    where
        Sink: traits::Sink,
        Sink::Requires: Subset<Provides>,
        Resources: Owned<Sink::Resources>,
    {
        PipelineBuilder {
            source: self.source,
            stages: self.stages,
            resources: self.resources,
            sink,
            _provides: PhantomData,
        }
    }
}

fn spawn_stall_watchdog(build: Build) {
    let rt = build.runtime.inner_task_runtime.clone();
    rt.spawn(async move {
        loop {
            tokio::select! {
                _ = build.runtime.inner_trycheck_stalled.listen() => {}
                _ = build.shutdown.wait_async() => break,
            }
            let snap = build.metrics.countof_processed.load(Ordering::Relaxed);
            tokio::select! {
                _ = tokio::time::sleep(STALL_WARN) => {}
                _ = build.shutdown.wait_async() => break,
            }
            if build.metrics.countof_processed.load(Ordering::Relaxed) == snap
                && build.metrics.any_active()
            {
                warn!("pipeline stall: no throughput in {:?}", STALL_WARN);
                tokio::select! {
                    _ = tokio::time::sleep(STALL_HARD - STALL_WARN) => {}
                    _ = build.shutdown.wait_async() => break,
                }
                if build.metrics.countof_processed.load(Ordering::Relaxed) == snap
                    && build.metrics.countof_sourced.load(Ordering::Relaxed)
                        > build.metrics.countof_processed.load(Ordering::Relaxed)
                    && build.metrics.any_active()
                {
                    error!("pipeline deadlock: no throughput in {:?}", STALL_HARD);
                }
            }
        }
    });
}

fn spawn_source<W, Src>(
    build: &Build,
    source: Src,
) -> (
    channel::AsyncPressurisedReceiver<Output<Src::Output>>,
    channel::AsyncPressurisedSender<Pull>,
)
where
    W: Set + 'static,
    Src: traits::Source + Clone + Send + 'static,
    Src::Output: Send + 'static,
{
    use super::source as src_worker;

    let (res_tx, res_rx) = channel::async_pressurised::<Output<Src::Output>>(RES_QUEUE_MAX);
    let (req_tx, req_rx) = channel::async_pressurised::<Pull>(REQ_DEPTH_MAX);

    src_worker::register::<Src, W>(build.clone(), source, req_rx, res_tx);

    (res_rx, req_tx)
}

fn make_runner(pipeline: Pipeline, build: Build, scheduler: Scheduler) -> Runner {
    Runner::new(
        pipeline,
        scheduler,
        build.runtime,
        build.shutdown,
        build.metrics,
    )
}

impl<Provides, Source, Stages, Resources> PipelineBuilder<Provides, Source, Stages, Resources, ()>
where
    Provides: Set,
    Source: traits::Source + Clone + Send + 'static,
    Source::Output: Send + 'static,
{
    pub fn build<W>(self) -> Runner
    where
        W: Set + Subset<Provides> + 'static,
        Stages: Connect<W, Source::Output> + Send + 'static,
        <Stages as Connect<W, Source::Output>>::Output: Send + 'static,
    {
        let (runtime, burn_cores, job_slots, task_slots) = make_runtime();
        let metrics = Metrics::new();
        let shutdown = Shutdown::new();
        let scheduler = Scheduler::spawn(
            metrics.countof_active.clone(),
            burn_cores,
            job_slots,
            task_slots,
        );
        let build = Build {
            runtime,
            metrics,
            shutdown,
            petition_tx: scheduler.inner_petition_tx.clone(),
        };
        let pipeline = Pipeline::default();

        spawn_stall_watchdog(build.clone());

        let (source_res_rx, source_req_tx) = spawn_source::<W, Source>(&build, self.source);
        let (out_res_rx, out_req_tx) =
            self.stages
                .connect(source_res_rx, source_req_tx, build.clone());

        Drain::<<Stages as Connect<W, Source::Output>>::Output>::default().drive::<W, _>(
            out_res_rx,
            out_req_tx,
            build.shutdown.clone(),
        );

        make_runner(pipeline, build, scheduler)
    }
}

impl<Provides, Source, Stages, Resources, S> PipelineBuilder<Provides, Source, Stages, Resources, S>
where
    Provides: Set,
    Source: traits::Source + Clone + Send + 'static,
    Source::Output: Send + 'static,
    S: traits::Sink + Send + 'static,
{
    pub fn build<W>(self) -> Runner
    where
        W: Set + Subset<Provides> + 'static,
        Stages: Connect<W, Source::Output> + Send + 'static,
        <Stages as Connect<W, Source::Output>>::Output: Send + 'static + Into<S::Input<'static>>,
    {
        let (runtime, burn_cores, job_slots, task_slots) = make_runtime();
        let metrics = Metrics::new();
        let shutdown = Shutdown::new();
        let scheduler = Scheduler::spawn(
            metrics.countof_active.clone(),
            burn_cores,
            job_slots,
            task_slots,
        );
        let build = Build {
            runtime,
            metrics,
            shutdown,
            petition_tx: scheduler.inner_petition_tx.clone(),
        };
        let pipeline = Pipeline::default();

        spawn_stall_watchdog(build.clone());

        let (source_res_rx, source_req_tx) = spawn_source::<W, Source>(&build, self.source);
        let (out_res_rx, out_req_tx) =
            self.stages
                .connect(source_res_rx, source_req_tx, build.clone());

        self.sink
            .drive::<W, _>(out_res_rx, out_req_tx, build.shutdown.clone());

        make_runner(pipeline, build, scheduler)
    }
}
