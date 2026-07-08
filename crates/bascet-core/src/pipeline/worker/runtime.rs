use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::channel::Receiver;
use event_listener::Event;
use futures::{FutureExt, select_biased};

use super::{Exit, Worker};
use crate::apply::Apply;
use crate::execute::{Async, Sync};
use crate::layer::Activity;
use crate::pipeline::edge::{Edge, Upstream};
use crate::pipeline::runtime::Runtime;
use crate::pipeline::scheduler::{Decision, Id, Slot};
use crate::schedule::Strategy;
use crate::set::Set;
use crate::source::Pull;

pub(crate) struct Drain;

impl Drain {
    pub(crate) fn spawn<T>(runtime: Runtime, input: Edge<T>, slot: Slot) -> bool
    where
        T: Send + 'static,
    {
        let Some(input) = input.upstream() else {
            return false;
        };

        input.set_output_receiver(slot.petitioner.clone());

        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(Event::new());

        let pump_input = input.clone();
        let pump_runtime = runtime.clone();
        let pump_stopped = Arc::clone(&stopped);
        let pump_stop = Arc::clone(&stop);
        runtime
            .io()
            .spawn_task(move || Self::pump(pump_input, pump_runtime, pump_stopped, pump_stop));

        let run_runtime = runtime.clone();
        let id = slot.id;
        let petitioner = slot.petitioner.clone();
        runtime.io().spawn_task(move || async move {
            let result = AssertUnwindSafe(Self::run(input, run_runtime, slot))
                .catch_unwind()
                .await;
            stopped.store(true, Ordering::Release);
            stop.notify(usize::MAX);
            match result {
                Ok((slot, processed, exit)) => slot.finish(processed, exit),
                Err(_) => petitioner.fail(id, "drain"),
            }
        });
        true
    }

    async fn pump<T>(
        input: Upstream<T>,
        runtime: Runtime,
        stopped: Arc<AtomicBool>,
        stop: Arc<Event>,
    ) where
        T: Send + 'static,
    {
        loop {
            if stopped.load(Ordering::Acquire) || runtime.shutdown().is_triggered() {
                break;
            }

            let stop_listener = stop.listen();
            if stopped.load(Ordering::Acquire) || runtime.shutdown().is_triggered() {
                break;
            }

            let pull = input.pull_async(Pull::Next).fuse();
            let shutdown = runtime.shutdown().wait_async().fuse();
            let stop_wait = stop_listener.fuse();
            futures::pin_mut!(pull, shutdown, stop_wait);

            select_biased! {
                _ = shutdown => break,
                _ = stop_wait => break,
                result = pull => {
                    if result.is_err() {
                        break;
                    }
                }
            }
        }
    }

    async fn run<T>(input: Upstream<T>, runtime: Runtime, slot: Slot) -> (Slot, u64, Exit)
    where
        T: Send + 'static,
    {
        let mut metrics = runtime.metrics();
        loop {
            if let Some(exit) = Exit::poll(&slot.decision_rx) {
                let work = metrics.local_processed;
                metrics.flush_all();
                return (slot, work, exit);
            }

            slot.state.clear_activity();
            match input.take_async().await {
                Ok(_) => {}
                Err(_) => {
                    runtime.shutdown().trigger();
                    let work = metrics.local_processed;
                    metrics.flush_all();
                    return (slot, work, Exit);
                }
            }

            slot.state.set_activity(Activity::Busy);
            metrics.add_processed(1);
        }
    }
}

pub(crate) trait Dispatch<S, W>: crate::execute::Executable
where
    S: Apply,
    W: Set + 'static,
{
    fn spawn_stage(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Clone + Send + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static;

    fn spawn_source(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Apply<Input = ()> + Clone + Send + 'static,
        S::Output: Send + 'static;
}

impl<S, W> Dispatch<S, W> for Sync
where
    S: Apply<Runtime = Sync>,
    W: Set + 'static,
{
    fn spawn_stage(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Clone + Send + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static,
    {
        tracing::debug!(
            id = ?worker.id,
            strategy = ?worker.state.strategy(),
            "sync stage worker spawn"
        );
        let id = worker.id;
        let strategy = worker.state.strategy();
        Sync::spawn("stage", id, strategy, move || {
            Sync::stage::<S, W>(worker, decision_rx)
        });
    }

    fn spawn_source(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Apply<Input = ()> + Clone + Send + 'static,
        S::Output: Send + 'static,
    {
        tracing::debug!(
            id = ?worker.id,
            strategy = ?worker.state.strategy(),
            "sync source worker spawn"
        );
        let id = worker.id;
        let strategy = worker.state.strategy();
        Sync::spawn("source", id, strategy, move || {
            Sync::source::<S, W>(worker, decision_rx)
        });
    }
}

impl<S, W> Dispatch<S, W> for Async
where
    S: Apply<Runtime = Async>,
    W: Set + 'static,
{
    fn spawn_stage(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Clone + Send + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static,
    {
        let io = worker.runtime.io().clone();
        io.spawn_task(move || async move {
            let id = worker.id;
            let petitioner = worker.petitioner.clone();
            let result = AssertUnwindSafe(Async::stage::<S, W>(worker, decision_rx))
                .catch_unwind()
                .await;
            match result {
                Ok((worker, exit)) => worker.finish(exit),
                Err(_) => petitioner.fail(id, "async stage"),
            }
        });
    }

    fn spawn_source(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Apply<Input = ()> + Clone + Send + 'static,
        S::Output: Send + 'static,
    {
        let io = worker.runtime.io().clone();
        io.spawn_task(move || async move {
            let id = worker.id;
            let petitioner = worker.petitioner.clone();
            let result = AssertUnwindSafe(Async::source::<S, W>(worker, decision_rx))
                .catch_unwind()
                .await;
            match result {
                Ok((worker, exit)) => worker.finish(exit),
                Err(_) => petitioner.fail(id, "async source"),
            }
        });
    }
}

impl Sync {
    fn spawn<F>(kind: &'static str, id: Id, strategy: Strategy, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let strategy = match strategy {
            Strategy::Burn => "burn",
            Strategy::Job => "job",
            Strategy::Task => "task",
        };

        std::thread::Builder::new()
            .name(format!("bascet-{kind}-{strategy}-{}", id.0))
            .spawn(f)
            .expect("failed to spawn bascet sync worker");
    }
}
