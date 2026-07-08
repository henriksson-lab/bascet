use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::channel::Receiver;

use super::{Exit, Worker};
use crate::Temper;
use crate::apply::Apply;
use crate::execute::{Error, Sync};
use crate::layer::{Activity, WorkerState};
use crate::pipeline::edge::{Closed, Downstream, Miss, Upstream};
use crate::pipeline::pipeline::Metrics;
use crate::pipeline::scheduler::{Decision, Id, Petitioner};
use crate::set::Set;
use crate::source::Pull;

impl Sync {
    pub(crate) fn stage<S, W>(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Apply<Runtime = Sync> + Clone + Send + 'static,
        W: Set + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static,
    {
        let id = worker.id;
        let petitioner = worker.petitioner.clone();
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            if let Some(core) = worker.state.core_id() {
                core_affinity::set_for_current(core);
            }
            Self::drive_stage::<S, W>(worker, decision_rx)
        }));
        match result {
            Ok((worker, exit)) => worker.finish(exit),
            Err(_) => petitioner.fail(id, "sync stage"),
        }
    }

    pub(crate) fn source<S, W>(worker: Worker<S>, decision_rx: Receiver<Decision>)
    where
        S: Apply<Input = (), Runtime = Sync> + Clone + Send + 'static,
        W: Set + 'static,
        S::Output: Send + 'static,
    {
        let id = worker.id;
        let petitioner = worker.petitioner.clone();
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            if let Some(core) = worker.state.core_id() {
                core_affinity::set_for_current(core);
            }
            Self::drive_source::<S, W>(worker, decision_rx)
        }));
        match result {
            Ok((worker, exit)) => worker.finish(exit),
            Err(_) => petitioner.fail(id, "sync source"),
        }
    }

    fn can_demote(active: &Arc<AtomicU64>, state: &WorkerState) -> bool {
        !state.pinned()
            && active.load(Ordering::Acquire) > state.parallelism.value().min().get() as u64
    }

    fn miss_idle(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
    ) -> Temper<u32> {
        let temper = state.patience().miss();
        if matches!(temper, Temper::Patient(_)) && Self::can_demote(active, state) {
            petitioner.demote(id);
        }
        temper
    }

    fn wait_pull<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        metrics: &mut Metrics,
        wakeup: &event_listener::Event,
        decision_rx: &Receiver<Decision>,
        output: Option<&Downstream<T>>,
    ) -> Result<Pull, Exit> {
        let Some(output) = output else {
            return Ok(Pull::Next);
        };

        loop {
            if let Some(exit) = Exit::poll(decision_rx) {
                return Err(exit);
            }

            match output.try_recv_pull() {
                Ok(Some(pull)) => {
                    state.patience().hit();
                    return Ok(pull);
                }
                Ok(None) => {
                    state.clear_activity();
                    metrics.add_idle(1);
                    let temper = Self::miss_idle(id, petitioner, active, state);
                    if matches!(temper, Temper::Eager(_)) {
                        wakeup.notify(1);
                    }
                    state.strategy().idle(temper);
                }
                Err(_) => return Err(Exit),
            }
        }
    }

    fn pull<T>(
        input: &Upstream<T>,
        mut pull: Pull,
        state: &WorkerState,
        petitioner: &Petitioner,
        id: Id,
        active: &Arc<AtomicU64>,
        metrics: &mut Metrics,
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
                    let temper = Self::miss_idle(id, petitioner, active, state);
                    state.strategy().idle(temper);
                }
                Err(Miss::Closed(_)) => return Err(Closed),
            }
        }
    }

    fn send<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        metrics: &mut Metrics,
        output: &Downstream<T>,
        mut item: T,
    ) -> Result<(), Closed> {
        loop {
            match output.try_send(item) {
                Ok(()) => return Ok(()),
                Err(Miss::Full(next)) => {
                    item = next;
                    state.set_activity(Activity::Backpressure);
                    metrics.add_backpressure(1);
                    let temper = Self::miss_idle(id, petitioner, active, state);
                    state.strategy().idle(temper);
                }
                Err(Miss::Closed(_)) => return Err(Closed),
            }
        }
    }

    fn wait_input<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        metrics: &mut Metrics,
        wakeup: &event_listener::Event,
        decision_rx: &Receiver<Decision>,
        input: &Upstream<T>,
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
                    let temper = Self::miss_idle(id, petitioner, active, state);
                    if matches!(temper, Temper::Eager(_)) {
                        wakeup.notify(1);
                    }
                    state.strategy().idle(temper);
                }
                Err(_) => return Err(Exit),
            }
        }
    }

    fn output<T>(
        id: Id,
        petitioner: &Petitioner,
        active: &Arc<AtomicU64>,
        state: &WorkerState,
        output: Option<&Downstream<T>>,
        metrics: &mut Metrics,
        from_source: bool,
        item: Result<Option<T>, Error>,
    ) -> Result<Option<Exit>, Closed> {
        match item {
            Ok(Some(value)) => {
                if from_source {
                    metrics.add_sourced(1);
                } else {
                    metrics.add_processed(1);
                }
                if let Some(output) = output {
                    Self::send(id, petitioner, active, state, metrics, output, value)?;
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

    fn process<S, W>(
        worker: &mut Worker<S>,
        want: &Pull,
        input: S::Input,
        output: Option<&Downstream<S::Output>>,
        from_source: bool,
    ) -> Option<Exit>
    where
        S: Apply<Runtime = Sync> + Clone + Send + 'static,
        W: Set + 'static,
        S::Output: Send + 'static,
    {
        let produced = worker.stage.apply::<W>(want, input);
        match Self::output(
            worker.id,
            &worker.petitioner,
            &worker.active,
            &worker.state,
            output,
            &mut worker.metrics,
            from_source,
            produced,
        ) {
            Ok(Some(exit)) => Some(exit),
            Ok(None) => None,
            Err(_) => Some(Exit),
        }
    }

    fn drive_stage<S, W>(
        mut worker: Worker<S>,
        decision_rx: Receiver<Decision>,
    ) -> (Worker<S>, Exit)
    where
        S: Apply<Runtime = Sync> + Clone + Send + 'static,
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

            let runtime = worker.runtime.clone();
            let want = match Self::wait_pull(
                worker.id,
                &worker.petitioner,
                &worker.active,
                &worker.state,
                &mut worker.metrics,
                runtime.watchdog(),
                &decision_rx,
                output.as_ref(),
            ) {
                Ok(want) => want,
                Err(exit) => return (worker, exit),
            };

            if Self::pull(
                &input,
                want.clone(),
                &worker.state,
                &worker.petitioner,
                worker.id,
                &worker.active,
                &mut worker.metrics,
            )
            .is_err()
            {
                return (worker, Exit);
            }

            let runtime = worker.runtime.clone();
            let input = match Self::wait_input(
                worker.id,
                &worker.petitioner,
                &worker.active,
                &worker.state,
                &mut worker.metrics,
                runtime.watchdog(),
                &decision_rx,
                &input,
            ) {
                Ok(input) => {
                    worker.state.fulfill();
                    input
                }
                Err(exit) => return (worker, exit),
            };

            if let Some(exit) =
                Self::process::<S, W>(&mut worker, &want, input, output.as_ref(), false)
            {
                return (worker, exit);
            }
        }
    }

    fn drive_source<S, W>(
        mut worker: Worker<S>,
        decision_rx: Receiver<Decision>,
    ) -> (Worker<S>, Exit)
    where
        S: Apply<Input = (), Runtime = Sync> + Clone + Send + 'static,
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
                runtime.watchdog(),
                &decision_rx,
                Some(&output),
            ) {
                Ok(want) => want,
                Err(exit) => return (worker, exit),
            };
            worker.state.set_activity(Activity::Busy);
            if let Some(exit) = Self::process::<S, W>(&mut worker, &want, (), Some(&output), true) {
                return (worker, exit);
            }
        }
    }
}
