use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crossbeam::channel::Receiver;
use futures::FutureExt;

use crate::owned::Owned;
use crate::set::Set;
use crate::source::{Pull, Source};
use crate::stage::{Mode, Output, Scheduling, Strategy};
use crate::utils::channel::{AsyncPressurisedReceiver, AsyncPressurisedSender};

use super::builder::Build;
use super::consts::TASK_IDLE_TIMEOUT;
use super::pipeline::{Metrics, Runtime};
use super::scheduler::{Decision, Id, Motivation, Petition, Spawn};

pub(super) struct Worker<Src: Source> {
    pub id: Id,
    pub group_idx: usize,
    pub countof_active: Arc<AtomicU32>,
    pub runtime: Runtime,
    pub metrics: Metrics,
    pub petition_tx: crossbeam::channel::Sender<Petition>,
    pub source: Src,
    pub req_rx: AsyncPressurisedReceiver<Pull>,
    pub res_tx: AsyncPressurisedSender<Output<Src::Output>>,
    pub standby: Arc<AtomicBool>,
    pub scheduling: Arc<Scheduling>,
    pub processed: u64,
}

enum Exit {
    Retire { processed: u64 },
    Demote { level: Strategy, processed: u64 },
}

impl<Src: Source> Worker<Src> {
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
    tracing::error!(?id, "source worker panicked");
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

fn miss_pressure<Src: Source, T>(w: &Worker<Src>, tx: &AsyncPressurisedSender<T>) {
    if tx.pressure().miss() {
        if let Some(group_idx) = tx.group_idx() {
            try_promote(&w.petition_tx, group_idx, Motivation::Pressure);
        }
    }
}

struct SourceSpawn<Src: Source, W> {
    build: Build,
    source: Src,
    req_rx: AsyncPressurisedReceiver<Pull>,
    res_tx: AsyncPressurisedSender<Output<Src::Output>>,
    standby: Arc<AtomicBool>,
    _w: PhantomData<fn() -> W>,
}

impl<Src, W> Spawn for SourceSpawn<Src, W>
where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
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
        run_worker::<Src, W>(
            Worker {
                id,
                group_idx,
                countof_active,
                runtime: self.build.runtime.clone(),
                metrics: self.build.metrics.clone(),
                petition_tx: self.build.petition_tx.clone(),
                source: self.source.clone(),
                req_rx: self.req_rx.clone(),
                res_tx: self.res_tx.clone(),
                standby: Arc::clone(&self.standby),
                scheduling: sched,
                processed: 0,
            },
            decision_rx,
        );
    }
}

pub(super) fn register<Src, W>(
    build: Build,
    source: Src,
    req_rx: AsyncPressurisedReceiver<Pull>,
    res_tx: AsyncPressurisedSender<Output<Src::Output>>,
) where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
{
    let mode: Mode = <Src as Owned<Mode>>::owned(&source);
    let strategy: Strategy = <Src as Owned<Strategy>>::owned(&source);
    let pressure = req_rx.pressure().clone();
    let spawn = Box::new(SourceSpawn::<Src, W> {
        build: build.clone(),
        source,
        req_rx,
        res_tx,
        standby: Arc::new(AtomicBool::new(false)),
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
}

fn run_worker<Src, W>(worker: Worker<Src>, decision_rx: Receiver<Decision>)
where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
{
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
                    run_blocking::<Src, W>(worker, decision_rx)
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
                    run_blocking::<Src, W>(worker, decision_rx)
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
                let result = AssertUnwindSafe(run_task::<Src, W>(worker, decision_rx))
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

fn run_blocking<Src, W>(mut w: Worker<Src>, decision_rx: Receiver<Decision>) -> Exit
where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
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

        let emit = w
            .runtime
            .inner_task_runtime
            .block_on(w.source.produce::<W>(req));

        if send_emit::<Src, W>(&mut w, emit).is_none() {
            break;
        }
    }
    w.retire_exit()
}

async fn run_task<Src, W>(mut w: Worker<Src>, decision_rx: Receiver<Decision>) -> Exit
where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
{
    let mut holding_standby = false;

    loop {
        if let Ok(decision) = decision_rx.try_recv() {
            if holding_standby {
                w.standby.store(false, Ordering::Release);
            }
            return match decision {
                Decision::Exit => w.retire_exit(),
                Decision::Demote => w.demote_exit(),
            };
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

        let emit = w.source.produce::<W>(req).await;

        let out = match emit {
            Output::Shutdown => {
                w.res_tx.send_async(Output::Shutdown).await.ok();
                break;
            }
            Output::Error(e) => {
                tracing::error!("{e}");
                continue;
            }
            value @ Output::Value(_) => {
                w.metrics.countof_sourced.fetch_add(1, Ordering::Relaxed);
                w.processed += 1;
                value
            }
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

fn send_emit<Src, W>(w: &mut Worker<Src>, emit: Output<Src::Output>) -> Option<()>
where
    Src: Source + Clone + Send + 'static,
    W: Set + 'static,
    Src::Output: Send + 'static,
{
    let out = match emit {
        Output::Shutdown => {
            w.res_tx.send(Output::Shutdown).ok();
            return None;
        }
        Output::Error(e) => {
            tracing::error!("{e}");
            return Some(());
        }
        value @ Output::Value(_) => {
            w.metrics.countof_sourced.fetch_add(1, Ordering::Relaxed);
            w.processed += 1;
            value
        }
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
