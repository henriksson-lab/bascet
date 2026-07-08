use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use crossbeam::channel::Receiver;
use futures::future::poll_fn;
use futures::{FutureExt, select_biased};

use super::{Exit, Worker};
use crate::pipeline::edge::{Closed, Downstream, Miss, Upstream};
use crate::pipeline::pipeline::Metrics;
use crate::pipeline::scheduler::{Decision, Id, Petitioner};
use crate::set::Set;
use crate::source::Pull;
use crate::apply::Apply;
use crate::execute::{Async, Error};
use crate::layer::{Activity, WorkerState};

impl Async {
    fn can_demote(active: &Arc<AtomicU64>, state: &WorkerState) -> bool {
        !state.pinned()
            && active.load(Ordering::Acquire) > state.parallelism.value().min().get() as u64
    }

    fn idle_delay(state: &WorkerState) -> Duration {
        Duration::from_millis(state.patience().patience() as u64)
    }

    async fn wait_pull<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        metrics: &mut Metrics,
        output: &Downstream<T>,
        watchdog: &event_listener::Event,
        decision_rx: &Receiver<Decision>,
    ) -> Result<Pull, Exit> {
        loop {
            if let Some(exit) = Exit::poll(decision_rx) {
                return Err(exit);
            }

            watchdog.notify(1);
            let pull = output.pull_async().fuse();
            let delay = compio::runtime::time::sleep(Self::idle_delay(state)).fuse();
            futures::pin_mut!(pull, delay);

            select_biased! {
                result = pull => {
                    state.patience().hit();
                    return result.map_err(|_| Exit);
                }
                _ = delay => {
                    state.clear_activity();
                    metrics.add_idle(1);
                    state.patience().miss();
                    if Self::can_demote(active, state) {
                        petitioner.demote(id);
                    }
                    Self::yield_now().await;
                }
            }
        }
    }

    async fn pull<T>(
        input: &Upstream<T>,
        mut pull: Pull,
        state: &WorkerState,
        petitioner: &Petitioner,
        metrics: &mut Metrics,
        watchdog: &event_listener::Event,
    ) -> Result<(), Closed> {
        loop {
            match input.try_pull(pull) {
                Ok(()) => {
                    if let Some(signal) = state.request() {
                        petitioner.promote(signal);
                    }
                    return Ok(());
                }
                Err(Miss::Full(next)) => {
                    pull = next;
                    state.set_activity(Activity::Starved);
                    metrics.add_idle(1);
                    watchdog.notify(1);
                    Self::yield_now().await;
                }
                Err(Miss::Closed(_)) => return Err(Closed),
            }
        }
    }

    async fn send<T>(
        output: &Downstream<T>,
        mut item: T,
        state: &WorkerState,
        metrics: &mut Metrics,
        watchdog: &event_listener::Event,
    ) -> Result<(), Closed> {
        loop {
            match output.try_send(item) {
                Ok(()) => return Ok(()),
                Err(Miss::Full(next)) => {
                    item = next;
                    state.set_activity(Activity::Backpressure);
                    metrics.add_backpressure(1);
                    watchdog.notify(1);
                    Self::yield_now().await;
                }
                Err(Miss::Closed(_)) => return Err(Closed),
            }
        }
    }

    async fn wait_input<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        metrics: &mut Metrics,
        input: &Upstream<T>,
        watchdog: &event_listener::Event,
        decision_rx: &Receiver<Decision>,
    ) -> Result<T, Exit> {
        loop {
            if let Some(exit) = Exit::poll(decision_rx) {
                return Err(exit);
            }

            match input.try_take() {
                Ok(Some(item)) => {
                    state.patience().hit();
                    state.set_activity(Activity::Busy);
                    return Ok(item);
                }
                Ok(None) => {
                    input.promote_upstream();
                    state.set_activity(Activity::Starved);
                    metrics.add_idle(1);
                    state.patience().miss();
                    if Self::can_demote(active, state) {
                        petitioner.demote(id);
                    }
                    watchdog.notify(1);
                    compio::runtime::time::sleep(Self::idle_delay(state)).await;
                }
                Err(_) => return Err(Exit),
            }
        }
    }

    async fn yield_now() {
        let mut yielded = false;
        poll_fn(move |cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    async fn output<T>(
        output: Option<&Downstream<T>>,
        state: &WorkerState,
        metrics: &mut Metrics,
        from_source: bool,
        item: Result<Option<T>, Error>,
        watchdog: &event_listener::Event,
    ) -> Result<Option<Exit>, Closed> {
        match item {
            Ok(Some(value)) => {
                if from_source {
                    metrics.add_sourced(1);
                } else {
                    metrics.add_processed(1);
                }
                if let Some(output) = output {
                    Self::send(output, value, state, metrics, watchdog).await?;
                }
                Ok(None)
            }
            Ok(None) => Ok(Some(Exit)),
            Err(error) => {
                tracing::error!("{error}");
                Ok(None)
            }
        }
    }

    async fn process<S, W>(
        worker: &mut Worker<S>,
        want: &Pull,
        input: S::Input,
        output: Option<&Downstream<S::Output>>,
        from_source: bool,
    ) -> Option<Exit>
    where
        S: Apply<Runtime = Async> + Clone + Send + 'static,
        W: Set + 'static,
        S::Output: Send + 'static,
    {
        let produced = worker.stage.apply::<W>(want, input).await;
        let runtime = worker.runtime.clone();
        match Self::output(
            output,
            &worker.state,
            &mut worker.metrics,
            from_source,
            produced,
            runtime.watchdog(),
        )
        .await
        {
            Ok(Some(exit)) => Some(exit),
            Ok(None) => None,
            Err(_) => Some(Exit),
        }
    }

    pub(super) async fn stage<S, W>(
        mut worker: Worker<S>,
        decision_rx: Receiver<Decision>,
    ) -> (Worker<S>, Exit)
    where
        S: Apply<Runtime = Async> + Clone + Send + 'static,
        W: Set + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static,
    {
        let input = worker.input.clone().expect("stage worker missing input");
        let output = worker.output.clone();

        loop {
            if let Some(exit) = Exit::poll(&decision_rx) {
                return (worker, exit);
            }

            let want = match &output {
                Some(output) => {
                    let runtime = worker.runtime.clone();
                    match Self::wait_pull(
                        worker.id,
                        &worker.petitioner,
                        &worker.active,
                        &worker.state,
                        &mut worker.metrics,
                        output,
                        runtime.watchdog(),
                        &decision_rx,
                    )
                    .await
                    {
                        Ok(want) => want,
                        Err(exit) => return (worker, exit),
                    }
                }
                None => Pull::Next,
            };

            let runtime = worker.runtime.clone();
            if Self::pull(
                &input,
                want.clone(),
                &worker.state,
                &worker.petitioner,
                &mut worker.metrics,
                runtime.watchdog(),
            )
            .await
            .is_err()
            {
                return (worker, Exit);
            }

            worker.runtime.watchdog().notify(1);
            let runtime = worker.runtime.clone();
            let input = match Self::wait_input(
                worker.id,
                &worker.petitioner,
                &worker.active,
                &worker.state,
                &mut worker.metrics,
                &input,
                runtime.watchdog(),
                &decision_rx,
            )
            .await
            {
                Ok(input) => {
                    worker.state.fulfill();
                    input
                }
                Err(exit) => return (worker, exit),
            };

            if let Some(exit) =
                Self::process::<S, W>(&mut worker, &want, input, output.as_ref(), false).await
            {
                return (worker, exit);
            }
        }
    }

    pub(super) async fn source<S, W>(
        mut worker: Worker<S>,
        decision_rx: Receiver<Decision>,
    ) -> (Worker<S>, Exit)
    where
        S: Apply<Input = (), Runtime = Async> + Clone + Send + 'static,
        W: Set + 'static,
        S::Output: Send + 'static,
    {
        let output = worker.output.clone().expect("source worker missing output");

        loop {
            if let Some(exit) = Exit::poll(&decision_rx) {
                return (worker, exit);
            }

            let runtime = worker.runtime.clone();
            let want = match Self::wait_pull(
                worker.id,
                &worker.petitioner,
                &worker.active,
                &worker.state,
                &mut worker.metrics,
                &output,
                runtime.watchdog(),
                &decision_rx,
            )
            .await
            {
                Ok(want) => want,
                Err(exit) => return (worker, exit),
            };
            worker.state.set_activity(Activity::Busy);
            if let Some(exit) =
                Self::process::<S, W>(&mut worker, &want, (), Some(&output), true).await
            {
                return (worker, exit);
            }
        }
    }
}
