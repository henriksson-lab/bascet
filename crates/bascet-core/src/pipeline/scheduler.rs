use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use core_affinity::CoreId;
use crossbeam::channel::{Receiver, Sender};

use super::consts::{
    DEMAND_DECAY, DEMAND_DECAY_MAX, DEMAND_DECAY_MIN, DEMAND_GROWTH, DEMAND_GROWTH_MAX,
    DEMAND_GROWTH_MIN, DEMAND_INITIAL, DEMAND_MIN, DEMAND_STRAIN, PRESSURE_DECAY_MAX,
    PRESSURE_DECAY_MIN, PRESSURE_GROWTH_MAX, PRESSURE_GROWTH_MIN, SENSITIVITY_DOWN, SENSITIVITY_UP,
};
use crate::stage::{Mode, Scheduling, Strategy};
use crate::utils::Pressure;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Id(pub u64);

pub(crate) trait Spawn: Send {
    fn spawn(
        &self,
        id: Id,
        group_idx: usize,
        sched: Arc<Scheduling>,
        decision_rx: Receiver<Decision>,
        countof_active: Arc<AtomicU32>,
    );
}

pub(crate) enum Decision {
    Exit,
    Demote,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Motivation {
    Demand,
    Pressure,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Feedback {
    Eager,
    Late,
    Stable,
}

pub(crate) enum Petition {
    Register {
        mode: Mode,
        strategy: Strategy,
        spawn: Box<dyn Spawn>,
        pressure: Arc<Pressure>,
    },
    Retire {
        id: Id,
        processed: u64,
    },
    Demote {
        id: Id,
        group_idx: usize,
        level: Strategy,
        processed: u64,
    },
    Promote {
        group_idx: usize,
        motivation: Motivation,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lease {
    Burn(CoreId),
    Job,
    Task,
}

struct Worker {
    id: Id,
    sched: Arc<Scheduling>,
    decision: Sender<Decision>,
    motivation: Option<Motivation>,
}

#[derive(Default)]
struct Leases {
    task: Vec<Id>,
    job: Vec<Id>,
    burn: Vec<(Id, CoreId)>,
}

impl Leases {
    fn insert(&mut self, id: Id, lease: Lease) {
        match lease {
            Lease::Burn(core) => self.burn.push((id, core)),
            Lease::Job => self.job.push(id),
            Lease::Task => self.task.push(id),
        }
    }

    fn remove(&mut self, id: Id) -> Option<Lease> {
        if let Some(idx) = self.task.iter().position(|lease_id| *lease_id == id) {
            self.task.remove(idx);
            return Some(Lease::Task);
        }

        if let Some(idx) = self.job.iter().position(|lease_id| *lease_id == id) {
            self.job.remove(idx);
            return Some(Lease::Job);
        }

        if let Some(idx) = self.burn.iter().position(|(lease_id, _)| *lease_id == id) {
            let (_, core) = self.burn.remove(idx);
            return Some(Lease::Burn(core));
        }

        None
    }

    fn count(&self) -> usize {
        self.task.len() + self.job.len() + self.burn.len()
    }

    fn weakest(&self) -> Option<Strategy> {
        if !self.task.is_empty() {
            Some(Strategy::Task)
        } else if !self.job.is_empty() {
            Some(Strategy::Job)
        } else if !self.burn.is_empty() {
            Some(Strategy::Burn)
        } else {
            None
        }
    }
}

struct Group {
    mode: Mode,
    strategy: Strategy,
    spawn: Box<dyn Spawn>,
    countof_active: Arc<AtomicU32>,
    leases: Leases,
    workers: Vec<Worker>,
    demand: Arc<Pressure>,
    pressure: Arc<Pressure>,
}

impl Group {
    fn can_spare(&self) -> bool {
        !self.manual() && self.leases.count() > self.mode.countof_min().get() as usize
    }

    fn manual(&self) -> bool {
        matches!(self.mode, Mode::Manual { .. })
    }

    fn can_grow(&self) -> bool {
        !self.manual() && self.leases.count() < self.mode.countof_max().get() as usize
    }

    fn feedback(&self, motivation: Motivation, feedback: Feedback) {
        match (motivation, feedback) {
            (Motivation::Demand, Feedback::Eager) => {
                scale(
                    &self.demand.growth,
                    SENSITIVITY_DOWN,
                    DEMAND_GROWTH_MIN,
                    DEMAND_GROWTH_MAX,
                );
                scale(
                    &self.demand.decay,
                    SENSITIVITY_UP,
                    DEMAND_DECAY_MIN,
                    DEMAND_DECAY_MAX,
                );
            }
            (Motivation::Demand, Feedback::Late) => {
                scale(
                    &self.demand.growth,
                    SENSITIVITY_UP,
                    DEMAND_GROWTH_MIN,
                    DEMAND_GROWTH_MAX,
                );
                scale(
                    &self.demand.decay,
                    SENSITIVITY_DOWN,
                    DEMAND_DECAY_MIN,
                    DEMAND_DECAY_MAX,
                );
            }
            (Motivation::Pressure, Feedback::Eager) => {
                scale(
                    &self.pressure.growth,
                    SENSITIVITY_DOWN,
                    PRESSURE_GROWTH_MIN,
                    PRESSURE_GROWTH_MAX,
                );
                scale(
                    &self.pressure.decay,
                    SENSITIVITY_UP,
                    PRESSURE_DECAY_MIN,
                    PRESSURE_DECAY_MAX,
                );
            }
            (Motivation::Pressure, Feedback::Late) => {
                scale(
                    &self.pressure.growth,
                    SENSITIVITY_UP,
                    PRESSURE_GROWTH_MIN,
                    PRESSURE_GROWTH_MAX,
                );
                scale(
                    &self.pressure.decay,
                    SENSITIVITY_DOWN,
                    PRESSURE_DECAY_MIN,
                    PRESSURE_DECAY_MAX,
                );
            }
            (_, Feedback::Stable) => {}
        }
    }

    fn feedback_worker(&self, worker: &Worker, processed: u64) {
        let Some(motivation) = worker.motivation else {
            return;
        };

        if processed == 0 {
            self.feedback(motivation, Feedback::Eager);
        } else {
            self.feedback(motivation, Feedback::Stable);
        }
    }

    fn worker(&self, id: Id) -> Option<&Worker> {
        self.workers.iter().find(|w| w.id == id)
    }

    fn laziest_from<I>(&self, ids: I) -> Option<&Worker>
    where
        I: IntoIterator<Item = Id>,
    {
        ids.into_iter()
            .filter_map(|id| self.worker(id))
            .filter(|w| !w.sched.pinned())
            .max_by_key(|w| w.sched.countof_idle.load(Ordering::Relaxed))
    }

    fn laziest(&self, strategy: Strategy) -> Option<&Worker> {
        match strategy {
            Strategy::Burn => self.laziest_from(self.leases.burn.iter().map(|(id, _)| *id)),
            Strategy::Job => self.laziest_from(self.leases.job.iter().copied()),
            Strategy::Task => self.laziest_from(self.leases.task.iter().copied()),
        }
    }

    fn remove(&mut self, id: Id) -> Option<(Worker, Lease)> {
        let worker_idx = self.workers.iter().position(|w| w.id == id).map(|i| i)?;
        let lease = self.leases.remove(id)?;
        Some((self.workers.remove(worker_idx), lease))
    }
}

fn scale(value: &AtomicU32, factor: (u32, u32), min: u32, max: u32) {
    let (num, den) = factor;
    value
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
            let scaled = (old as u64).saturating_mul(num as u64) / den as u64;
            let mut next = scaled.clamp(min as u64, max as u64) as u32;

            if num > den && next == old && old < max {
                next = old + 1;
            } else if num < den && next == old && old > min {
                next = old - 1;
            }

            Some(next)
        })
        .ok();
}

fn demand_pressure() -> Arc<Pressure> {
    Arc::new(Pressure::new(
        DEMAND_INITIAL,
        DEMAND_MIN,
        NonZeroU32::new(DEMAND_STRAIN).unwrap(),
        DEMAND_GROWTH,
        DEMAND_DECAY,
    ))
}

#[derive(Clone)]
pub(crate) struct Scheduler {
    pub(crate) inner_petition_tx: Sender<Petition>,
}

impl Scheduler {
    pub(crate) fn spawn(
        countof_active: Arc<AtomicUsize>,
        burn_cores: Vec<CoreId>,
        job_slots: usize,
        task_slots: usize,
    ) -> Self {
        let (inner_petition_tx, inner_petition_rx) = crossbeam::channel::unbounded::<Petition>();
        let worker = Loop::new(
            inner_petition_rx,
            countof_active,
            burn_cores,
            job_slots,
            task_slots,
        );
        std::thread::spawn(move || worker.run());
        Self { inner_petition_tx }
    }
}

struct Loop {
    inner_petition_rx: Receiver<Petition>,
    inner_groups: Vec<Group>,
    countof_active: Arc<AtomicUsize>,
    inner_next_id: u64,
    inner_burn: VecDeque<CoreId>,
    inner_job: VecDeque<()>,
    inner_task: VecDeque<()>,
}

impl Loop {
    fn new(
        inner_petition_rx: Receiver<Petition>,
        countof_active: Arc<AtomicUsize>,
        burn_cores: Vec<CoreId>,
        job_slots: usize,
        task_slots: usize,
    ) -> Self {
        Self {
            inner_petition_rx,
            inner_groups: Vec::new(),
            countof_active,
            inner_next_id: 0,
            inner_burn: burn_cores.into_iter().collect(),
            inner_job: std::iter::repeat(()).take(job_slots).collect(),
            inner_task: std::iter::repeat(()).take(task_slots).collect(),
        }
    }

    fn run(mut self) {
        while let Ok(petition) = self.inner_petition_rx.recv() {
            self.handle(petition);
        }
    }

    fn handle(&mut self, petition: Petition) {
        match petition {
            Petition::Register {
                mode,
                strategy,
                spawn,
                pressure,
            } => self.on_register(mode, strategy, spawn, pressure),
            Petition::Retire { id, processed } => self.on_retire(id, processed),
            Petition::Demote {
                id,
                group_idx,
                level,
                processed,
            } => self.on_demote(id, group_idx, level, processed),
            Petition::Promote {
                group_idx,
                motivation,
            } => self.try_promote(group_idx, motivation),
        }
    }

    fn alloc_id(&mut self) -> Id {
        let id = Id(self.inner_next_id);
        self.inner_next_id += 1;
        id
    }

    fn laziest(&self, strategy: Strategy, except_group: Option<usize>) -> Option<Id> {
        self.inner_groups
            .iter()
            .enumerate()
            .filter(|(idx, group)| Some(*idx) != except_group && group.can_spare())
            .filter_map(|(_, group)| group.laziest(strategy))
            .max_by_key(|w| w.sched.countof_idle.load(Ordering::Relaxed))
            .map(|w| w.id)
    }

    fn find_and_remove(&mut self, id: Id) -> Option<(Worker, Lease, usize)> {
        for (idx, group) in self.inner_groups.iter_mut().enumerate() {
            if let Some((worker, lease)) = group.remove(id) {
                return Some((worker, lease, idx));
            }
        }
        None
    }

    fn force_evict(&mut self, id: Id) -> Option<(Lease, usize)> {
        let (worker, lease, group_idx) = self.find_and_remove(id)?;
        tracing::info!(
            ?id,
            group_idx,
            ?lease,
            strategy = ?worker.sched.strategy(),
            "scheduler demote request"
        );
        self.inner_groups[group_idx]
            .countof_active
            .fetch_sub(1, Ordering::Relaxed);
        worker.decision.send(Decision::Demote).ok();
        Some((lease, group_idx))
    }

    fn spawn_with_lease(&mut self, group_idx: usize, lease: Lease, motivation: Option<Motivation>) {
        let id = self.alloc_id();
        let (decision_tx, decision_rx) = crossbeam::channel::bounded::<Decision>(1);
        let mode = self.inner_groups[group_idx].mode;
        let strategy = match lease {
            Lease::Burn(_) => Strategy::Burn,
            Lease::Job => Strategy::Job,
            Lease::Task => Strategy::Task,
        };
        let countof_active = Arc::clone(&self.inner_groups[group_idx].countof_active);
        let demand = Arc::clone(&self.inner_groups[group_idx].demand);
        let sched = Arc::new(Scheduling::new(mode, strategy, demand));
        if let Lease::Burn(core) = lease {
            sched.set_core_id(Some(core));
        }
        self.inner_groups[group_idx].spawn.spawn(
            id,
            group_idx,
            Arc::clone(&sched),
            decision_rx,
            Arc::clone(&countof_active),
        );
        self.countof_active.fetch_add(1, Ordering::Relaxed);
        countof_active.fetch_add(1, Ordering::Relaxed);
        self.inner_groups[group_idx].leases.insert(id, lease);
        self.inner_groups[group_idx].workers.push(Worker {
            id,
            sched,
            decision: decision_tx,
            motivation,
        });

        if let Some(motivation) = motivation {
            tracing::info!(
                ?id,
                group_idx,
                ?motivation,
                ?lease,
                ?strategy,
                workers = self.inner_groups[group_idx].leases.count(),
                "promotion worker spawned"
            );
        }
    }

    fn try_spawn_best(
        &mut self,
        group_idx: usize,
        target: Strategy,
        motivation: Option<Motivation>,
        except_group: Option<usize>,
    ) -> bool {
        match target {
            Strategy::Burn => self.try_spawn_burn(group_idx, motivation, except_group),
            Strategy::Job => self.try_spawn_job(group_idx, motivation, except_group),
            Strategy::Task => self.try_spawn_task(group_idx, motivation, except_group),
        }
    }

    fn try_spawn_burn(
        &mut self,
        group_idx: usize,
        motivation: Option<Motivation>,
        except_group: Option<usize>,
    ) -> bool {
        if let Some(core) = self.inner_burn.pop_front() {
            self.spawn_with_lease(group_idx, Lease::Burn(core), motivation);
            return true;
        }

        if let Some(victim_id) = self.laziest(Strategy::Burn, except_group) {
            if let Some((Lease::Burn(core), victim_group)) = self.force_evict(victim_id) {
                self.spawn_with_lease(group_idx, Lease::Burn(core), motivation);
                self.try_spawn_job(victim_group, None, None);
                return true;
            }
        }

        self.try_spawn_job(group_idx, motivation, except_group)
    }

    fn try_spawn_job(
        &mut self,
        group_idx: usize,
        motivation: Option<Motivation>,
        except_group: Option<usize>,
    ) -> bool {
        if let Some(()) = self.inner_job.pop_front() {
            self.spawn_with_lease(group_idx, Lease::Job, motivation);
            true
        } else if let Some(victim_id) = self.laziest(Strategy::Job, except_group) {
            let Some((Lease::Job, victim_group)) = self.force_evict(victim_id) else {
                return false;
            };
            self.spawn_with_lease(group_idx, Lease::Job, motivation);
            self.try_spawn_task(victim_group, None, None);
            true
        } else {
            self.try_spawn_task(group_idx, motivation, except_group)
        }
    }

    fn try_spawn_task(
        &mut self,
        group_idx: usize,
        motivation: Option<Motivation>,
        except_group: Option<usize>,
    ) -> bool {
        if let Some(()) = self.inner_task.pop_front() {
            self.spawn_with_lease(group_idx, Lease::Task, motivation);
            true
        } else if let Some(victim_id) = self.laziest(Strategy::Task, except_group) {
            if let Some((Lease::Task, _)) = self.force_evict(victim_id) {
                self.spawn_with_lease(group_idx, Lease::Task, motivation);
                true
            } else {
                false
            }
        } else {
            tracing::error!("task pool exhausted, no workers to evict");
            false
        }
    }

    fn on_register(
        &mut self,
        mode: Mode,
        strategy: Strategy,
        spawn: Box<dyn Spawn>,
        pressure: Arc<Pressure>,
    ) {
        let group_idx = self.inner_groups.len();
        self.inner_groups.push(Group {
            mode,
            strategy,
            spawn,
            countof_active: Arc::new(AtomicU32::new(0)),
            leases: Leases::default(),
            workers: Vec::new(),
            demand: demand_pressure(),
            pressure,
        });
        for _ in 0..mode.countof_workers().get() {
            self.try_spawn_best(group_idx, strategy, None, None);
        }
    }

    fn on_retire(&mut self, id: Id, processed: u64) {
        self.countof_active.fetch_sub(1, Ordering::Relaxed);
        let Some((worker, lease, group_idx)) = self.find_and_remove(id) else {
            tracing::warn!(?id, processed, "unknown worker retire");
            return;
        };
        tracing::info!(
            ?id,
            group_idx,
            processed,
            ?lease,
            strategy = ?worker.sched.strategy(),
            motivation = ?worker.motivation,
            workers = self.inner_groups[group_idx].leases.count(),
            "worker retire"
        );
        self.inner_groups[group_idx].feedback_worker(&worker, processed);
        self.inner_groups[group_idx]
            .countof_active
            .fetch_sub(1, Ordering::Relaxed);
        self.release(lease);
    }

    fn on_demote(&mut self, id: Id, group_idx: usize, level: Strategy, processed: u64) {
        self.countof_active.fetch_sub(1, Ordering::Relaxed);
        let Some(group) = self.inner_groups.get_mut(group_idx) else {
            tracing::warn!(?id, group_idx, ?level, processed, "unknown group demote");
            return;
        };

        let Some((worker, lease)) = group.remove(id) else {
            tracing::warn!(?id, group_idx, ?level, processed, "unknown worker demote");
            return;
        };

        tracing::info!(
            ?id,
            group_idx,
            ?level,
            processed,
            ?lease,
            strategy = ?worker.sched.strategy(),
            motivation = ?worker.motivation,
            workers = group.leases.count(),
            "worker demote"
        );
        group.countof_active.fetch_sub(1, Ordering::Relaxed);
        group.feedback_worker(&worker, processed);
        self.release(lease);
    }

    fn try_promote(&mut self, group_idx: usize, motivation: Motivation) {
        let Some(group) = self.inner_groups.get(group_idx) else {
            tracing::warn!(group_idx, ?motivation, "unknown group promote");
            return;
        };

        let strategy = group.strategy;
        let workers = group.leases.count();
        let min = group.mode.countof_min().get();
        let max = group.mode.countof_max().get();
        let can_grow = group.can_grow();
        let target = match group.leases.weakest().unwrap_or(group.strategy) {
            Strategy::Task => Strategy::Job,
            Strategy::Job => Strategy::Burn,
            Strategy::Burn => Strategy::Burn,
        };

        tracing::info!(
            group_idx,
            ?motivation,
            ?strategy,
            workers,
            min,
            max,
            ?target,
            "promotion petition"
        );

        if !can_grow {
            tracing::info!(
                group_idx,
                ?motivation,
                workers,
                max,
                ?target,
                "promotion blocked"
            );
            return;
        }

        let late = workers > min as usize;
        let spawned = self.try_spawn_best(group_idx, target, Some(motivation), Some(group_idx));

        if spawned {
            tracing::info!(
                group_idx,
                ?motivation,
                ?target,
                workers = self.inner_groups[group_idx].leases.count(),
                late,
                "promotion accepted"
            );

            if late {
                self.inner_groups[group_idx].feedback(motivation, Feedback::Late);
            }
        } else {
            tracing::info!(
                group_idx,
                ?motivation,
                ?target,
                workers = self.inner_groups[group_idx].leases.count(),
                "promotion blocked"
            );
        }
    }

    fn release(&mut self, lease: Lease) {
        match lease {
            Lease::Burn(core) => self.inner_burn.push_back(core),
            Lease::Job => self.inner_job.push_back(()),
            Lease::Task => self.inner_task.push_back(()),
        }
    }
}
