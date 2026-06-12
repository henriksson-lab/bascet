use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crossbeam::channel::Receiver;
use futures::FutureExt;

use crate::owned::Owned;
use crate::pipe::Pipe;
use crate::set::Set;
use crate::source::Pull;
use crate::stage::{Emit, Mode, Output, Scheduling, Stage, Strategy};
use crate::utils::channel::{self as channel, AsyncPressurisedReceiver, AsyncPressurisedSender};

use super::builder::Build;
use super::consts::{REQ_DEPTH_MAX, RES_QUEUE_MAX, TASK_IDLE_TIMEOUT};
use super::pipeline::{Metrics, Runtime};
use super::scheduler::{Decision, Id, Motivation, Petition, Spawn};

pub(super) struct Worker<S: Stage, Input: Send + 'static> {
    pub id: Id,
    pub group_idx: usize,
    pub countof_active: Arc<AtomicU32>,
    pub runtime: Runtime,
    pub metrics: Metrics,
    pub petition_tx: crossbeam::channel::Sender<Petition>,
    pub stage: S,
    pub req_rx: AsyncPressurisedReceiver<Pull>,
    pub req_tx: AsyncPressurisedSender<Pull>,
    pub res_rx: AsyncPressurisedReceiver<Output<Input>>,
    pub res_tx: AsyncPressurisedSender<Output<S::Output>>,
    pub standby: Arc<AtomicBool>,
    pub scheduling: Arc<Scheduling>,
    pub processed: u64,
}

enum Exit {
    Retire { processed: u64 },
    Demote { level: Strategy, processed: u64 },
}

impl<S: Stage, Input: Send + 'static> Worker<S, Input> {
    fn retire_exit(&self) -> Exit {
        Exit::Retire {
            processed: self.processed,
        }
    }

    fn demote_exit(&self) -> Exit {
        Exit::Demote {
            level: self.scheduling.strategy(),
            processed: self.processed,
        }
    }

    fn can_self_scale_down(&self) -> bool {
        !self.scheduling.pinned()
            && self.countof_active.load(Ordering::Acquire)
                > self.scheduling.mode.countof_min().get()
    }

    fn self_demote_exit(&self) -> Option<Exit> {
        self.can_self_scale_down().then(|| self.demote_exit())
    }
}

fn finish(
    petition_tx: &crossbeam::channel::Sender<Petition>,
    id: Id,
    group_idx: usize,
    exit: Exit,
) {
    match exit {
        Exit::Retire { processed } => {
            petition_tx.send(Petition::Retire { id, processed }).ok();
        }
        Exit::Demote { level, processed } => {
            petition_tx
                .send(Petition::Demote {
                    id,
                    group_idx,
                    level,
                    processed,
                })
                .ok();
        }
    }
}

fn finish_panic(petition_tx: &crossbeam::channel::Sender<Petition>, id: Id) {
    tracing::error!(?id, "stage worker panicked");
    petition_tx.send(Petition::Retire { id, processed: 0 }).ok();
}

fn try_promote(
    petition_tx: &crossbeam::channel::Sender<Petition>,
    group_idx: usize,
    motivation: Motivation,
) {
    petition_tx
        .send(Petition::Promote {
            group_idx,
            motivation,
        })
        .ok();
}

fn miss_demand<S: Stage, Input: Send + 'static>(w: &Worker<S, Input>) {
    if w.scheduling.demand().miss() {
        try_promote(&w.petition_tx, w.group_idx, Motivation::Demand);
    }
}

fn miss_pressure<S: Stage, Input: Send + 'static, T>(
    w: &Worker<S, Input>,
    tx: &AsyncPressurisedSender<T>,
) {
    if tx.pressure().miss() {
        if let Some(group_idx) = tx.group_idx() {
            try_promote(&w.petition_tx, group_idx, Motivation::Pressure);
        }
    }
}

fn try_fwd_req<S: Stage, Input: Send + 'static>(w: &Worker<S, Input>, req: &Pull) {
    let mut fwd = Some(fwd_req(req));
    match w.req_tx.try_send_option(&mut fwd) {
        Ok(true) => w.req_tx.pressure().hit(),
        Ok(false) => miss_pressure(w, &w.req_tx),
        Err(_) => {}
    }
}

struct StageSpawn<S: Stage, W, Input> {
    stage: S,
    req_rx: AsyncPressurisedReceiver<Pull>,
    req_tx: AsyncPressurisedSender<Pull>,
    res_rx: AsyncPressurisedReceiver<Output<Input>>,
    res_tx: AsyncPressurisedSender<Output<S::Output>>,
    standby: Arc<AtomicBool>,
    build: Build,
    _w: PhantomData<fn() -> W>,
}

impl<S, W, Input> Spawn for StageSpawn<S, W, Input>
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    fn spawn(
        &self,
        id: Id,
        group_idx: usize,
        sched: Arc<Scheduling>,
        decision_rx: Receiver<Decision>,
        countof_active: Arc<AtomicU32>,
    ) {
        self.req_rx.set_group_idx(group_idx);
        self.res_rx.set_group_idx(group_idx);
        run_worker::<S, W, Input>(
            Worker {
                id,
                group_idx,
                countof_active,
                runtime: self.build.runtime.clone(),
                metrics: self.build.metrics.clone(),
                petition_tx: self.build.petition_tx.clone(),
                stage: self.stage.clone(),
                req_rx: self.req_rx.clone(),
                req_tx: self.req_tx.clone(),
                res_rx: self.res_rx.clone(),
                res_tx: self.res_tx.clone(),
                standby: Arc::clone(&self.standby),
                scheduling: sched,
                processed: 0,
            },
            decision_rx,
        );
    }
}

fn run_worker<S, W, Input>(worker: Worker<S, Input>, decision_rx: Receiver<Decision>)
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    tracing::info!(
        id = ?worker.id,
        strategy = ?worker.scheduling.strategy(),
        "stage worker spawn"
    );
    match worker.scheduling.strategy() {
        Strategy::Burn => {
            std::thread::spawn(move || {
                let id = worker.id;
                let group_idx = worker.group_idx;
                let petition_tx = worker.petition_tx.clone();
                if let Some(core) = worker.scheduling.core_id() {
                    core_affinity::set_for_current(core);
                }
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    run_burn::<S, W, Input>(worker, decision_rx)
                }));
                match result {
                    Ok(exit) => finish(&petition_tx, id, group_idx, exit),
                    Err(_) => finish_panic(&petition_tx, id),
                }
            });
        }
        Strategy::Job => {
            std::thread::spawn(move || {
                let id = worker.id;
                let group_idx = worker.group_idx;
                let petition_tx = worker.petition_tx.clone();
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    run_job::<S, W, Input>(worker, decision_rx)
                }));
                match result {
                    Ok(exit) => finish(&petition_tx, id, group_idx, exit),
                    Err(_) => finish_panic(&petition_tx, id),
                }
            });
        }
        Strategy::Task => {
            let runtime = Arc::clone(&worker.runtime.inner_task_runtime);
            runtime.spawn(async move {
                let id = worker.id;
                let group_idx = worker.group_idx;
                let petition_tx = worker.petition_tx.clone();
                let result = AssertUnwindSafe(run_task::<S, W, Input>(worker, decision_rx))
                    .catch_unwind()
                    .await;
                match result {
                    Ok(exit) => finish(&petition_tx, id, group_idx, exit),
                    Err(_) => finish_panic(&petition_tx, id),
                }
            });
        }
    }
}

fn run_burn<S, W, Input>(mut w: Worker<S, Input>, decision_rx: Receiver<Decision>) -> Exit
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    loop {
        if let Ok(decision) = decision_rx.try_recv() {
            match decision {
                Decision::Exit => break,
                Decision::Demote => return w.demote_exit(),
            }
        }

        let req = match w.req_rx.try_recv() {
            Ok(Some(req)) => {
                w.scheduling.patience().hit();
                w.scheduling.countof_idle.store(0, Ordering::Relaxed);
                req
            }
            Ok(None) => {
                let p = w.scheduling.patience().miss();
                w.scheduling.countof_idle.fetch_add(1, Ordering::Relaxed);
                if p <= w.scheduling.patience().min() {
                    if let Some(exit) = w.self_demote_exit() {
                        return exit;
                    }
                    std::hint::spin_loop();
                    continue;
                }
                std::hint::spin_loop();
                continue;
            }
            Err(_) => break,
        };

        if let Pull::Shutdown = req {
            w.res_tx.send(Output::Shutdown).ok();
            break;
        }

        match pull_spin(&w, &req) {
            Ok(Some(item)) => {
                if apply_and_send::<S, W, Input>(&mut w, item).is_none() {
                    break;
                }
            }
            Ok(None) => break,
            Err(()) => break,
        }
    }
    w.retire_exit()
}

fn run_job<S, W, Input>(mut w: Worker<S, Input>, decision_rx: Receiver<Decision>) -> Exit
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    loop {
        if let Ok(decision) = decision_rx.try_recv() {
            return match decision {
                Decision::Exit => w.retire_exit(),
                Decision::Demote => w.demote_exit(),
            };
        }

        let req = loop {
            match w.req_rx.try_recv() {
                Ok(Some(req)) => {
                    w.scheduling.patience().hit();
                    break req;
                }
                Ok(None) => {
                    if w.scheduling.patience().miss() > w.scheduling.patience().min() {
                        std::hint::spin_loop();
                        continue;
                    }
                    if w.standby
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        match w.req_rx.recv_blocking() {
                            Ok(req) => {
                                w.standby.store(false, Ordering::Release);
                                break req;
                            }
                            Err(_) => {
                                w.standby.store(false, Ordering::Release);
                                return w.retire_exit();
                            }
                        }
                    } else {
                        if w.can_self_scale_down() {
                            return w.retire_exit();
                        }
                        std::hint::spin_loop();
                        continue;
                    }
                }
                Err(_) => return w.retire_exit(),
            }
        };

        if let Pull::Shutdown = req {
            w.res_tx.send(Output::Shutdown).ok();
            break;
        }

        let item = loop {
            match w.res_rx.try_recv() {
                Ok(Some(item)) => {
                    w.scheduling.patience().hit();
                    w.scheduling.demand().hit();
                    break item;
                }
                Ok(None) => {
                    miss_demand(&w);
                    try_fwd_req(&w, &req);
                    if w.scheduling.patience().miss() <= w.scheduling.patience().min() {
                        w.runtime.inner_trycheck_stalled.notify(1);
                        match w.res_rx.recv_blocking() {
                            Ok(item) => {
                                w.scheduling.demand().hit();
                                break item;
                            }
                            Err(_) => return w.retire_exit(),
                        }
                    }
                    std::hint::spin_loop();
                }
                Err(_) => return w.retire_exit(),
            }
        };

        if apply_and_send::<S, W, Input>(&mut w, item).is_none() {
            break;
        }
    }
    w.retire_exit()
}

async fn run_task<S, W, Input>(mut w: Worker<S, Input>, decision_rx: Receiver<Decision>) -> Exit
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    let mut holding_standby = false;

    loop {
        if let Ok(decision) = decision_rx.try_recv() {
            match decision {
                Decision::Exit => break,
                Decision::Demote => return w.demote_exit(),
            }
        }

        let req = match tokio::time::timeout(TASK_IDLE_TIMEOUT, w.req_rx.recv_async()).await {
            Err(_timeout) => {
                if !holding_standby {
                    if w.standby
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        holding_standby = true;
                    } else {
                        if w.can_self_scale_down() {
                            break;
                        }
                        continue;
                    }
                }
                continue;
            }
            Ok(Err(_disconnected)) => break,
            Ok(Ok(req)) => {
                if holding_standby {
                    w.standby.store(false, Ordering::Release);
                    holding_standby = false;
                }
                req
            }
        };

        if let Pull::Shutdown = req {
            w.res_tx.send_async(Output::Shutdown).await.ok();
            break;
        }

        let item = loop {
            match w.res_rx.try_recv() {
                Ok(Some(item)) => {
                    w.scheduling.patience().hit();
                    w.scheduling.demand().hit();
                    break item;
                }
                Ok(None) => {
                    w.scheduling.patience().miss();
                    miss_demand(&w);
                    try_fwd_req(&w, &req);
                    match w.res_rx.recv_async().await {
                        Ok(item) => {
                            w.scheduling.demand().hit();
                            break item;
                        }
                        Err(_) => {
                            if holding_standby {
                                w.standby.store(false, Ordering::Release);
                            }
                            return w.retire_exit();
                        }
                    }
                }
                Err(_) => {
                    if holding_standby {
                        w.standby.store(false, Ordering::Release);
                    }
                    return w.retire_exit();
                }
            }
        };

        let out = match item {
            Output::Shutdown => {
                w.res_tx.send_async(Output::Shutdown).await.ok();
                break;
            }
            Output::Error(e) => {
                tracing::error!("{e}");
                continue;
            }
            Output::Value(v) => {
                w.metrics.countof_processed.fetch_add(1, Ordering::Relaxed);
                w.processed += 1;
                w.stage.apply::<W>((&v).into())
            }
        };

        let out = match out {
            Emit::None => continue,
            Emit::Error(e) => Output::Error(e),
            Emit::Value(v) => Output::Value(v),
        };

        let mut opt = Some(out);
        match w.res_tx.try_send_option(&mut opt) {
            Ok(true) => {
                w.res_tx.pressure().hit();
            }
            Ok(false) => {
                miss_pressure(&w, &w.res_tx);
                w.res_tx.send_async(opt.unwrap()).await.ok();
            }
            Err(_) => break,
        }
    }

    if holding_standby {
        w.standby.store(false, Ordering::Release);
    }
    w.retire_exit()
}

fn apply_and_send<S, W, Input>(w: &mut Worker<S, Input>, item: Output<Input>) -> Option<()>
where
    S: Stage + Clone + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    S::Output: Send + 'static,
    for<'a> &'a Input: Into<S::Input<'a>>,
{
    let out = match item {
        Output::Shutdown => {
            w.res_tx.send(Output::Shutdown).ok();
            return None;
        }
        Output::Error(e) => {
            tracing::error!("{e}");
            return Some(());
        }
        Output::Value(v) => {
            w.metrics.countof_processed.fetch_add(1, Ordering::Relaxed);
            w.processed += 1;
            w.stage.apply::<W>((&v).into())
        }
    };

    let out = match out {
        Emit::None => return Some(()),
        Emit::Error(e) => Output::Error(e),
        Emit::Value(v) => Output::Value(v),
    };

    let mut opt = Some(out);
    match w.res_tx.try_send_option(&mut opt) {
        Ok(true) => {
            w.res_tx.pressure().hit();
        }
        Ok(false) => {
            miss_pressure(w, &w.res_tx);
            w.res_tx.send(opt.unwrap()).ok();
        }
        Err(_) => return None,
    }
    Some(())
}

fn pull_spin<S: Stage, Input: Send + 'static>(
    w: &Worker<S, Input>,
    req: &Pull,
) -> Result<Option<Output<Input>>, ()> {
    loop {
        match w.res_rx.try_recv() {
            Ok(Some(item)) => {
                w.scheduling.patience().hit();
                w.scheduling.demand().hit();
                return Ok(Some(item));
            }
            Ok(None) => {
                miss_demand(w);
                if w.scheduling.patience().miss() <= w.scheduling.patience().min() {
                    return Ok(None);
                }
                try_fwd_req(w, req);
                std::hint::spin_loop();
            }
            Err(_) => return Err(()),
        }
    }
}

fn fwd_req(req: &Pull) -> Pull {
    match req {
        Pull::Read(r) => Pull::Read(r.clone()),
        _ => Pull::Next,
    }
}

// --- Connect trait: stages wire themselves into the pipeline ---

pub(super) trait Connect<W: Set, Input: Send + 'static> {
    type Output: Send + 'static;

    fn connect(
        self,
        upstream_res_rx: AsyncPressurisedReceiver<Output<Input>>,
        upstream_req_tx: AsyncPressurisedSender<Pull>,
        build: Build,
    ) -> (
        AsyncPressurisedReceiver<Output<Self::Output>>,
        AsyncPressurisedSender<Pull>,
    );
}

impl<W: Set, Input: Send + 'static> Connect<W, Input> for () {
    type Output = Input;

    fn connect(
        self,
        res_rx: AsyncPressurisedReceiver<Output<Input>>,
        req_tx: AsyncPressurisedSender<Pull>,
        _build: Build,
    ) -> (
        AsyncPressurisedReceiver<Output<Input>>,
        AsyncPressurisedSender<Pull>,
    ) {
        (res_rx, req_tx)
    }
}

impl<S, Tail, W, Input> Connect<W, Input> for Pipe<S, Tail>
where
    S: Stage + Clone + Send + 'static,
    S::Output: Send + 'static,
    Tail: Connect<W, Input> + Send + 'static,
    Tail::Output: Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    for<'a> &'a Tail::Output: Into<S::Input<'a>>,
{
    type Output = S::Output;

    fn connect(
        self,
        upstream_res_rx: AsyncPressurisedReceiver<Output<Input>>,
        upstream_req_tx: AsyncPressurisedSender<Pull>,
        build: Build,
    ) -> (
        AsyncPressurisedReceiver<Output<S::Output>>,
        AsyncPressurisedSender<Pull>,
    ) {
        let (tail_res_rx, tail_req_tx) =
            self.1
                .connect(upstream_res_rx, upstream_req_tx, build.clone());

        let (res_tx, res_rx) = channel::async_pressurised(RES_QUEUE_MAX);
        let (req_tx, req_rx) = channel::async_pressurised(REQ_DEPTH_MAX);

        let mode: Mode = <S as Owned<Mode>>::owned(&self.0);
        let strategy: Strategy = <S as Owned<Strategy>>::owned(&self.0);
        let standby = Arc::new(AtomicBool::new(false));
        let pressure = tail_res_rx.pressure().clone();

        let spawn = Box::new(StageSpawn::<S, W, Tail::Output> {
            stage: self.0,
            req_rx: req_rx.clone(),
            req_tx: tail_req_tx.clone(),
            res_rx: tail_res_rx.clone(),
            res_tx: res_tx.clone(),
            standby,
            build: build.clone(),
            _w: PhantomData,
        });
        build
            .petition_tx
            .send(Petition::Register {
                mode,
                strategy,
                spawn,
                pressure,
            })
            .ok();

        (res_rx, req_tx)
    }
}
