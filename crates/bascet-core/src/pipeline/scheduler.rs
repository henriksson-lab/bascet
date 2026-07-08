use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::channel::{Receiver, Sender};

use crate::layer::{Control, Feedback, Handle, WorkerState};
use crate::schedule::Strategy;
use crate::utils::AtomicPressure;

use super::runtime::{Lease, Runtime};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Id(pub u64);

pub(crate) struct Slot {
    pub(crate) id: Id,
    pub(crate) petitioner: Petitioner,
    pub(crate) state: WorkerState,
    pub(crate) decision_rx: Receiver<Decision>,
    pub(crate) active: Arc<AtomicU64>,
}

pub(crate) enum Decision {
    Stop,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Motivation {
    Demand,
    Pressure,
}

#[derive(Clone)]
pub(crate) enum Signal {
    Demand(Arc<AtomicPressure>, NonZeroU32),
    Pressure(Arc<AtomicPressure>, NonZeroU32),
}

impl Signal {
    pub(crate) fn motivation(&self) -> Motivation {
        match self {
            Signal::Demand(_, _) => Motivation::Demand,
            Signal::Pressure(_, _) => Motivation::Pressure,
        }
    }

    pub(crate) fn pressure(&self) -> &Arc<AtomicPressure> {
        match self {
            Signal::Demand(pressure, _) | Signal::Pressure(pressure, _) => pressure,
        }
    }

    pub(crate) fn level(&self) -> NonZeroU32 {
        match self {
            Signal::Demand(_, level) | Signal::Pressure(_, level) => *level,
        }
    }

    pub(crate) fn with_level(&self, level: NonZeroU32) -> Self {
        match self {
            Signal::Demand(pressure, _) => Signal::Demand(Arc::clone(pressure), level),
            Signal::Pressure(pressure, _) => Signal::Pressure(Arc::clone(pressure), level),
        }
    }

    pub(crate) fn same_source(&self, other: &Self) -> bool {
        self.motivation() == other.motivation() && Arc::ptr_eq(self.pressure(), other.pressure())
    }

    pub(crate) fn recovered(&self) -> bool {
        self.pressure().level() < self.level().get()
    }
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Signal::Demand(_, level) => f.debug_tuple("Demand").field(level).finish(),
            Signal::Pressure(_, level) => f.debug_tuple("Pressure").field(level).finish(),
        }
    }
}

enum Petition {
    Register {
        control: Control,
        reply: Sender<Petitioner>,
    },
    Promote {
        group: usize,
        signal: Signal,
    },
    Demote {
        group: usize,
        id: Id,
    },
    Finish {
        group: usize,
        id: Id,
        processed: u64,
    },
}

#[derive(Clone)]
pub(crate) struct Petitioner {
    tx: Sender<Petition>,
    group: Option<usize>,
}

impl Petitioner {
    pub(crate) fn spawn(active: Arc<AtomicU64>, runtime: Runtime) -> Self {
        let (tx, petitions) = crossbeam::channel::unbounded::<Petition>();
        let root = Self { tx, group: None };
        let scheduler = Scheduler::new(petitions, active, runtime, root.clone());
        std::thread::spawn(move || scheduler.run());
        root
    }

    fn bind(&self, group: usize) -> Self {
        Self {
            tx: self.tx.clone(),
            group: Some(group),
        }
    }

    pub(crate) fn register(&self, control: Control) -> Petitioner {
        let (reply_tx, reply_rx) = crossbeam::channel::bounded(1);
        self.tx
            .send(Petition::Register {
                control,
                reply: reply_tx,
            })
            .ok();
        reply_rx.recv().expect("scheduler stopped during register")
    }

    pub(crate) fn promote(&self, signal: Signal) {
        let Some(group) = self.group else {
            tracing::warn!(?signal, "root petitioner cannot promote");
            return;
        };
        self.tx.send(Petition::Promote { group, signal }).ok();
    }

    pub(crate) fn demote(&self, id: Id) {
        let Some(group) = self.group else {
            tracing::warn!(?id, "root petitioner cannot demote");
            return;
        };
        self.tx.send(Petition::Demote { group, id }).ok();
    }

    pub(crate) fn finish(&self, id: Id, processed: u64) {
        let Some(group) = self.group else {
            tracing::warn!(?id, processed, "root petitioner cannot finish worker");
            return;
        };
        self.tx
            .send(Petition::Finish {
                group,
                id,
                processed,
            })
            .ok();
    }
}

struct Scheduler {
    petitions: Receiver<Petition>,
    groups: Vec<Control>,
    active: Arc<AtomicU64>,
    next: u64,
    runtime: Runtime,
    cpu_budget: usize,
    root: Petitioner,
}

#[derive(Clone)]
struct SpawnRequest {
    group_idx: usize,
    target: Strategy,
    signal: Option<Signal>,
    except_group: Option<usize>,
}

impl SpawnRequest {
    fn new(
        group_idx: usize,
        target: Strategy,
        signal: Option<Signal>,
        except_group: Option<usize>,
    ) -> Self {
        Self {
            group_idx,
            target,
            signal,
            except_group,
        }
    }

    fn retarget(&self, target: Strategy) -> Self {
        Self {
            group_idx: self.group_idx,
            target,
            signal: self.signal.clone(),
            except_group: self.except_group,
        }
    }
}

struct Promotion {
    group_idx: usize,
    signal: Signal,
    level: NonZeroU32,
    desired: usize,
}

impl Promotion {
    fn spawn_request(&self, target: Strategy) -> SpawnRequest {
        SpawnRequest::new(
            self.group_idx,
            target,
            Some(self.signal.with_level(self.level)),
            Some(self.group_idx),
        )
    }
}

impl Scheduler {
    fn new(
        petitions: Receiver<Petition>,
        active: Arc<AtomicU64>,
        runtime: Runtime,
        root: Petitioner,
    ) -> Self {
        let cpu_budget = (runtime.burn() + runtime.jobs())
            .saturating_sub(runtime.io_threads())
            .max(1);
        Self {
            petitions,
            groups: Vec::new(),
            active,
            next: 0,
            runtime,
            cpu_budget,
            root,
        }
    }

    fn run(mut self) {
        while let Ok(petition) = self.petitions.recv() {
            self.handle(petition);
        }
    }

    fn handle(&mut self, petition: Petition) {
        match petition {
            Petition::Register { control, reply } => self.on_register(control, reply),
            Petition::Promote { group, signal } => self.promote(group, signal),
            Petition::Demote { group, id } => self.demote(group, id),
            Petition::Finish {
                group,
                id,
                processed,
            } => self.on_finish(group, id, processed),
        }
    }

    fn alloc_id(&mut self) -> Id {
        let id = Id(self.next);
        self.next += 1;
        id
    }

    fn victim(
        &self,
        strategy: Strategy,
        except_group: Option<usize>,
        recovered: Option<bool>,
    ) -> Option<Id> {
        self.groups
            .iter()
            .enumerate()
            .filter(|(idx, group)| {
                Some(*idx) != except_group && group.surplus() > 0 && self.can_reclaim(*idx)
            })
            .find_map(|(_, group)| group.victim(strategy, recovered))
    }

    fn runnable_groups(&self) -> usize {
        self.groups.iter().filter(|group| group.runnable()).count()
    }

    fn fair_allocation(&self, group_idx: usize) -> usize {
        let group = &self.groups[group_idx];
        let min = group.parallelism.value().min().get() as usize;
        let max = group.parallelism.value().max().get() as usize;
        let runnable = self.runnable_groups().max(1);
        let fair = self.cpu_budget.div_ceil(runnable);

        fair.clamp(min, max)
    }

    fn can_reclaim(&self, group_idx: usize) -> bool {
        let group = &self.groups[group_idx];
        let leased = group.leased();

        leased > group.useful_width() || leased > self.fair_allocation(group_idx)
    }

    fn evict(&mut self, id: Id) -> Option<(Lease, usize)> {
        for group_idx in 0..self.groups.len() {
            if self.groups[group_idx].get(id).is_none() {
                continue;
            }

            let lease = self.groups[group_idx].leases.take(id)?;
            let Some(handle) = self.groups[group_idx].get(id) else {
                self.free(lease);
                return None;
            };

            tracing::debug!(
                ?id,
                group_idx,
                ?lease,
                strategy = ?lease.strategy(),
                "scheduler eviction release request"
            );
            handle.decision.send(Decision::Stop).ok();
            return Some((lease, group_idx));
        }

        None
    }

    fn spawn(
        &mut self,
        group_idx: usize,
        lease: Lease,
        signal: Option<Signal>,
    ) -> Option<Strategy> {
        let id = self.alloc_id();
        let (decision_tx, decision_rx) = crossbeam::channel::bounded::<Decision>(1);
        let strategy = lease.strategy();
        let parallelism = self.groups[group_idx].parallelism;
        let demand = Arc::clone(&self.groups[group_idx].demand);
        let requested = Arc::clone(&self.groups[group_idx].requested);
        let fulfilled = Arc::clone(&self.groups[group_idx].fulfilled);
        let activity = Arc::clone(&self.groups[group_idx].activity);
        let petitioner = self.groups[group_idx].petitioner.clone();
        let active = Arc::clone(&self.groups[group_idx].active);
        let state = WorkerState::new(
            parallelism,
            strategy,
            demand,
            requested,
            fulfilled,
            activity,
        );
        if let Lease::Burn(core) = lease {
            state.set_core_id(Some(core));
        }

        let spawned = self.groups[group_idx].spawn(Slot {
            id,
            petitioner,
            state,
            decision_rx,
            active: Arc::clone(&active),
        });
        if !spawned {
            tracing::debug!(
                ?id,
                group_idx,
                ?lease,
                ?strategy,
                "stage worker spawn skipped"
            );
            self.free(lease);
            return None;
        }

        self.active.fetch_add(1, Ordering::Relaxed);
        active.fetch_add(1, Ordering::Relaxed);
        self.groups[group_idx].leases.add(id, lease.clone());
        let log_signal = signal.clone();
        self.groups[group_idx].handles.push(Handle {
            id,
            decision: decision_tx,
            signal,
        });

        tracing::debug!(
            ?id,
            group_idx,
            signal = ?log_signal,
            ?lease,
            ?strategy,
            workers = self.groups[group_idx].leases.len(),
            "stage worker spawn"
        );

        Some(strategy)
    }

    fn try_spawn(&mut self, request: SpawnRequest) -> Option<Strategy> {
        match request.target {
            Strategy::Burn => self.try_spawn_burn(request),
            Strategy::Job => self.try_spawn_job(request),
            Strategy::Task => self.try_spawn_task(request),
        }
    }

    fn try_spawn_strict(&mut self, request: SpawnRequest) -> Option<Strategy> {
        match request.target {
            Strategy::Burn => self.try_spawn_burn_strict(request),
            Strategy::Job => self.try_spawn_job_strict(request),
            Strategy::Task => self.try_spawn_task_strict(request),
        }
    }

    fn try_spawn_burn(&mut self, request: SpawnRequest) -> Option<Strategy> {
        self.try_spawn_burn_strict(request.clone())
            .or_else(|| self.try_spawn(request.retarget(Strategy::Job)))
    }

    fn try_spawn_job(&mut self, request: SpawnRequest) -> Option<Strategy> {
        self.try_spawn_job_strict(request.clone())
            .or_else(|| self.try_spawn(request.retarget(Strategy::Task)))
    }

    fn try_spawn_task(&mut self, request: SpawnRequest) -> Option<Strategy> {
        self.try_spawn_task_strict(request)
    }

    fn try_spawn_burn_strict(&mut self, request: SpawnRequest) -> Option<Strategy> {
        if let Some(lease) = self.runtime.acquire(Strategy::Burn) {
            return self.spawn(request.group_idx, lease, request.signal);
        }

        if let Some(victim_id) = self
            .victim(Strategy::Burn, request.except_group, Some(true))
            .or_else(|| self.victim(Strategy::Burn, request.except_group, Some(false)))
            .or_else(|| self.victim(Strategy::Burn, request.except_group, None))
        {
            if let Some((lease @ Lease::Burn(_), victim_group)) = self.evict(victim_id) {
                let strategy = self.spawn(request.group_idx, lease, request.signal)?;
                let _ = self.try_spawn(SpawnRequest::new(victim_group, Strategy::Job, None, None));
                return Some(strategy);
            }
        }

        None
    }

    fn try_spawn_job_strict(&mut self, request: SpawnRequest) -> Option<Strategy> {
        if let Some(lease) = self.runtime.acquire(Strategy::Job) {
            self.spawn(request.group_idx, lease, request.signal)
        } else if let Some(victim_id) = self
            .victim(Strategy::Job, request.except_group, Some(true))
            .or_else(|| self.victim(Strategy::Job, request.except_group, Some(false)))
            .or_else(|| self.victim(Strategy::Job, request.except_group, None))
        {
            let Some((lease @ Lease::Job(_), victim_group)) = self.evict(victim_id) else {
                return None;
            };
            let strategy = self.spawn(request.group_idx, lease, request.signal)?;
            let _ = self.try_spawn(SpawnRequest::new(victim_group, Strategy::Task, None, None));
            Some(strategy)
        } else {
            None
        }
    }

    fn try_spawn_task_strict(&mut self, request: SpawnRequest) -> Option<Strategy> {
        if let Some(lease) = self.runtime.acquire(Strategy::Task) {
            self.spawn(request.group_idx, lease, request.signal)
        } else if let Some(victim_id) = self
            .victim(Strategy::Task, request.except_group, Some(true))
            .or_else(|| self.victim(Strategy::Task, request.except_group, Some(false)))
            .or_else(|| self.victim(Strategy::Task, request.except_group, None))
        {
            if let Some((lease @ Lease::Task(_), _)) = self.evict(victim_id) {
                self.spawn(request.group_idx, lease, request.signal)
            } else {
                None
            }
        } else {
            tracing::error!("task pool exhausted, no workers to evict");
            None
        }
    }

    fn try_upgrade_job_to_burn(
        &mut self,
        group_idx: usize,
        signal: &Signal,
        level: NonZeroU32,
    ) -> Option<Strategy> {
        let id = self.groups[group_idx].candidate_for_signal(Strategy::Job, signal)?;
        let lease = self.runtime.acquire(Strategy::Burn)?;
        let strategy = self.spawn(group_idx, lease, Some(signal.with_level(level)))?;
        self.release_replaced_worker(group_idx, id, signal, strategy);
        Some(strategy)
    }

    fn try_upgrade_task_to_job(
        &mut self,
        group_idx: usize,
        signal: &Signal,
        level: NonZeroU32,
    ) -> Option<Strategy> {
        let id = self.groups[group_idx].candidate_for_signal(Strategy::Task, signal)?;
        let lease = self.runtime.acquire(Strategy::Job)?;
        let strategy = self.spawn(group_idx, lease, Some(signal.with_level(level)))?;
        self.release_replaced_worker(group_idx, id, signal, strategy);
        Some(strategy)
    }

    fn release_replaced_worker(
        &mut self,
        group_idx: usize,
        id: Id,
        signal: &Signal,
        replacement: Strategy,
    ) {
        let tier = self.groups[group_idx].leases.strategy(id);
        let Some(lease) = self.groups[group_idx].leases.demote(id) else {
            tracing::warn!(
                ?id,
                group_idx,
                ?tier,
                ?replacement,
                "promotion upgrade lease missing"
            );
            return;
        };

        let Some(decision) = self.groups[group_idx]
            .get(id)
            .map(|handle| handle.decision.clone())
        else {
            tracing::warn!(
                ?id,
                group_idx,
                ?tier,
                ?replacement,
                "promotion upgrade handle missing"
            );
            self.free(lease);
            return;
        };

        tracing::debug!(
            ?id,
            group_idx,
            ?tier,
            ?lease,
            ?replacement,
            ?signal,
            workers = self.groups[group_idx].leases.len(),
            "scheduler promotion upgrade release request"
        );
        decision.send(Decision::Stop).ok();
    }

    fn on_register(&mut self, mut control: Control, reply: Sender<Petitioner>) {
        let group_idx = self.groups.len();
        let petitioner = self.root.bind(group_idx);
        let initial = *control.strategy.value();
        let workers = control.parallelism.value().workers().get();
        let manual_strategy = control.strategy.is_manual();
        control.bind(petitioner.clone());
        self.groups.push(control);

        for _ in 0..workers {
            let request = SpawnRequest::new(group_idx, initial, None, None);
            let _ = if manual_strategy {
                self.try_spawn_strict(request)
            } else {
                self.try_spawn(request)
            };
        }

        reply.send(petitioner).ok();
    }

    fn on_finish(&mut self, group_idx: usize, id: Id, processed: u64) {
        let Some(group) = self.groups.get_mut(group_idx) else {
            tracing::warn!(?id, group_idx, processed, "unknown group finish");
            return;
        };

        let Some(handle) = group.take_handle(id) else {
            tracing::warn!(?id, group_idx, processed, "unknown worker finish");
            return;
        };

        let lease = group.leases.take(id).or_else(|| group.leases.release(id));

        self.active.fetch_sub(1, Ordering::Relaxed);
        group.active.fetch_sub(1, Ordering::Relaxed);
        group.learn_handle(&handle, processed);

        tracing::debug!(
            ?id,
            group_idx,
            processed,
            ?lease,
            signal = ?handle.signal,
            workers = group.leases.len(),
            "worker finish"
        );

        if let Some(lease) = lease {
            self.free(lease);
        }
    }

    fn fork_missing(
        &mut self,
        promotion: &Promotion,
        target: Strategy,
    ) -> (usize, Option<Strategy>) {
        let mut spawned = 0usize;
        let mut last_strategy = None;

        loop {
            let Some(group) = self.groups.get(promotion.group_idx) else {
                break;
            };

            if group.headroom() == 0 {
                break;
            }

            let active_for = group.active_for(&promotion.signal, target);
            if active_for >= promotion.desired {
                break;
            }

            let Some(strategy) = self.try_spawn_strict(promotion.spawn_request(target)) else {
                break;
            };

            spawned += 1;
            last_strategy = Some(strategy);
        }

        (spawned, last_strategy)
    }

    fn promote(&mut self, group_idx: usize, signal: Signal) {
        let Some(group) = self.groups.get(group_idx) else {
            tracing::warn!(group_idx, ?signal, "unknown group promote");
            return;
        };

        let workers = group.active();
        let birth = signal.level().get();
        let current = signal.pressure().level();
        let cap = if group.manual_parallelism() {
            group.parallelism.value().max().get() as usize
        } else {
            self.cpu_budget
        };
        let raw_desired = group.desired_for(&signal).min(cap);
        let useful_width = group.useful_width();
        let fair = self.fair_allocation(group_idx);
        let desired = raw_desired.min(useful_width).min(fair);
        let active_for = group.active_for(&signal, Strategy::Task);
        let headroom = group.headroom();
        let manual_parallelism = group.manual_parallelism();
        let manual_strategy = group.strategy.is_manual();
        let pinned_strategy = *group.strategy.value();
        let width_deficit = desired.saturating_sub(active_for);

        tracing::info!(
            group_idx,
            ?signal,
            motivation = ?signal.motivation(),
            workers,
            current,
            desired,
            headroom,
            "promotion petition"
        );

        if current == 0 {
            self.groups[group_idx].learn(&signal, Feedback::Eager);
            tracing::info!(
                group_idx,
                ?signal,
                motivation = ?signal.motivation(),
                current,
                "promotion recovered"
            );
            return;
        }

        let feedback = if current < birth {
            Feedback::Eager
        } else if active_for > 0 && width_deficit > 0 {
            Feedback::Late
        } else {
            Feedback::Stable
        };

        if feedback != Feedback::Stable {
            self.groups[group_idx].learn(&signal, feedback);
        }

        if manual_parallelism {
            tracing::info!(
                group_idx,
                ?signal,
                reason = "manual_parallelism",
                workers,
                current,
                desired,
                "promotion blocked"
            );
            return;
        }

        let Some(level) = NonZeroU32::new(current) else {
            return;
        };

        if manual_strategy {
            let promotion = Promotion {
                group_idx,
                signal: signal.clone(),
                level,
                desired,
            };
            let (spawned, last_strategy) = self.fork_missing(&promotion, pinned_strategy);
            tracing::info!(
                group_idx,
                ?signal,
                spawned,
                strategy = ?last_strategy,
                workers = self.groups[group_idx].leases.len(),
                "manual strategy promotion handled"
            );
            return;
        }

        if let Some(strategy) = self.try_upgrade_job_to_burn(group_idx, &signal, level) {
            tracing::info!(
                group_idx,
                ?signal,
                strategy = ?strategy,
                workers = self.groups[group_idx].leases.len(),
                "promotion upgraded"
            );
            return;
        }

        if let Some(strategy) = self.try_upgrade_task_to_job(group_idx, &signal, level) {
            tracing::info!(
                group_idx,
                ?signal,
                strategy = ?strategy,
                workers = self.groups[group_idx].leases.len(),
                "promotion upgraded"
            );
            return;
        }

        let promotion = Promotion {
            group_idx,
            signal: signal.clone(),
            level,
            desired,
        };

        let (burn_spawned, burn_strategy) = self.fork_missing(&promotion, Strategy::Burn);
        let (job_spawned, job_strategy) = self.fork_missing(&promotion, Strategy::Job);
        let (task_spawned, task_strategy) = match signal.motivation() {
            Motivation::Pressure => (0, None),
            Motivation::Demand => self.fork_missing(&promotion, Strategy::Task),
        };
        let spawned = burn_spawned + job_spawned + task_spawned;
        let last_strategy = task_strategy.or(job_strategy).or(burn_strategy);
        let final_active_for = self.groups[group_idx].active_for(&signal, Strategy::Task);
        let final_width_deficit = desired.saturating_sub(final_active_for);

        if spawned == 0 && self.groups[group_idx].headroom() == 0 {
            tracing::info!(
                group_idx,
                ?signal,
                reason = "no_headroom",
                workers,
                current,
                desired,
                "promotion blocked"
            );
            return;
        }

        if spawned == 0 && final_width_deficit == 0 {
            tracing::info!(
                group_idx,
                ?signal,
                current,
                desired,
                workers,
                "promotion covered"
            );
            return;
        }

        if spawned == 0 {
            self.groups[group_idx].learn(&signal, Feedback::Late);
            tracing::info!(
                group_idx,
                ?signal,
                reason = "no_capacity",
                current,
                desired,
                workers = self.groups[group_idx].leases.len(),
                "promotion blocked"
            );
        } else {
            tracing::info!(
                group_idx,
                ?signal,
                spawned,
                strategy = ?last_strategy,
                workers = self.groups[group_idx].leases.len(),
                "promotion accepted"
            );
        }
    }

    fn demote(&mut self, group_idx: usize, id: Id) {
        let Some(group) = self.groups.get_mut(group_idx) else {
            tracing::warn!(group_idx, ?id, "unknown group demote");
            return;
        };

        if group.surplus() == 0 {
            tracing::trace!(
                group_idx,
                ?id,
                workers = group.active(),
                min = group.parallelism.value().min().get(),
                "demotion blocked"
            );
            return;
        }

        let signal = group
            .get(id)
            .and_then(|handle| handle.signal.as_ref())
            .cloned();

        if signal
            .as_ref()
            .is_some_and(|signal| !group.recovered(signal))
        {
            tracing::trace!(group_idx, ?id, ?signal, "demotion held");
            return;
        };

        let tier = group.leases.strategy(id);
        let Some(lease) = group.leases.demote(id) else {
            tracing::warn!(?id, group_idx, ?tier, "demotion lease missing");
            return;
        };

        let Some(handle) = group.get(id) else {
            tracing::warn!(?id, group_idx, ?tier, "demotion handle missing");
            self.free(lease);
            return;
        };

        tracing::debug!(
            ?id,
            group_idx,
            ?tier,
            ?lease,
            signal = ?signal,
            workers = group.leases.len(),
            "scheduler demotion release request"
        );
        handle.decision.send(Decision::Stop).ok();
    }

    fn free(&mut self, lease: Lease) {
        self.runtime.release(lease);
    }
}
