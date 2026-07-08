use bon::bon;
use core_affinity::CoreId;
use event_listener::Event;
use futures::StreamExt;
use futures::channel::mpsc;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schedule::Strategy;
use crate::set::Set;

use super::pipeline::{Metrics, Runner};
use super::run::Run;
use super::scheduler::Petitioner;
use super::shutdown::Shutdown;

type IoTask = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + Send + 'static>;

#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    watchdog: Event,
    metrics: Metrics,
    shutdown: Shutdown,
    petitioner: OnceLock<Petitioner>,
    burn: Burn,
    job: Job,
    task: Task,
    io: Io,
    io_operations: NonZeroU64,
}

pub(crate) struct Burn {
    cores: Mutex<VecDeque<CoreId>>,
    capacity: usize,
}

pub(crate) struct Job {
    slots: Mutex<VecDeque<NonZeroU64>>,
    capacity: usize,
}

pub(crate) struct Task {
    slots: Mutex<VecDeque<NonZeroU64>>,
}

#[derive(Clone)]
pub(crate) struct Io {
    slots: Arc<Vec<NonZeroU64>>,
    txs: Arc<Vec<mpsc::UnboundedSender<IoTask>>>,
    next: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
pub(crate) enum Lease {
    Burn(CoreId),
    Job(NonZeroU64),
    Task(NonZeroU64),
}

impl Lease {
    pub(crate) fn strategy(&self) -> Strategy {
        match self {
            Lease::Burn(_) => Strategy::Burn,
            Lease::Job(_) => Strategy::Job,
            Lease::Task(_) => Strategy::Task,
        }
    }
}

impl Burn {
    fn new(cores: Vec<CoreId>) -> Self {
        let capacity = cores.len();
        Self {
            cores: Mutex::new(cores.into()),
            capacity,
        }
    }

    fn acquire(&self) -> Option<Lease> {
        self.cores.lock().ok()?.pop_front().map(Lease::Burn)
    }

    fn release(&self, core: CoreId) {
        if let Ok(mut cores) = self.cores.lock() {
            cores.push_back(core);
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Job {
    fn new(slots: VecDeque<NonZeroU64>) -> Self {
        let capacity = slots.len();
        Self {
            slots: Mutex::new(slots),
            capacity,
        }
    }

    fn acquire(&self) -> Option<Lease> {
        self.slots.lock().ok()?.pop_front().map(Lease::Job)
    }

    fn release(&self, slot: NonZeroU64) {
        if let Ok(mut slots) = self.slots.lock() {
            slots.push_back(slot);
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Task {
    fn new(slots: VecDeque<NonZeroU64>) -> Self {
        Self {
            slots: Mutex::new(slots),
        }
    }

    fn acquire(&self) -> Option<Lease> {
        self.slots.lock().ok()?.pop_front().map(Lease::Task)
    }

    fn release(&self, slot: NonZeroU64) {
        if let Ok(mut slots) = self.slots.lock() {
            slots.push_back(slot);
        }
    }
}

impl Io {
    pub(crate) fn spawn(slots: VecDeque<NonZeroU64>) -> Self {
        let slots: Vec<_> = slots.into_iter().collect();
        let mut txs = Vec::with_capacity(slots.len());

        for (idx, _) in slots.iter().enumerate() {
            let (tx, rx) = mpsc::unbounded::<IoTask>();
            Self::spawn_thread(idx, rx);
            txs.push(tx);
        }

        Self {
            slots: Arc::new(slots),
            txs: Arc::new(txs),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn spawn_task<F, Fut>(&self, f: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.txs.len();
        self.txs[idx]
            .unbounded_send(Box::new(move || Box::pin(f())))
            .ok();
    }

    fn spawn_thread(idx: usize, mut rx: mpsc::UnboundedReceiver<IoTask>) {
        std::thread::Builder::new()
            .name(format!("bascet-io-{idx}"))
            .spawn(move || {
                let runtime = compio::runtime::Runtime::new()
                    .expect("failed to create bascet compio runtime");
                runtime.block_on(async move {
                    while let Some(task) = rx.next().await {
                        let task = task();
                        compio::runtime::Runtime::with_current(|runtime| {
                            runtime.spawn(task).detach();
                        });
                    }
                });
            })
            .expect("failed to spawn bascet io runtime thread");
    }

    fn len(&self) -> usize {
        self.slots.len()
    }
}

struct Defaults {
    burn: usize,
    jobs: usize,
    tasks: usize,
    io_threads: usize,
    io_operations: usize,
}

impl Defaults {
    fn from_machine() -> Self {
        let all_cores = core_affinity::get_core_ids().unwrap_or_default();
        let p = all_cores.len().max(1);
        let reserved = (p / 8).max(4).min(p.saturating_sub(1));

        Self {
            burn: p - reserved,
            jobs: reserved * 2,
            tasks: reserved * 512,
            io_threads: (reserved / 2).max(2),
            io_operations: 128,
        }
    }
}

fn slots(count: usize) -> VecDeque<NonZeroU64> {
    (1..=count.max(1) as u64)
        .map(|slot| NonZeroU64::new(slot).unwrap())
        .collect()
}

#[bon]
impl Runtime {
    #[builder]
    pub fn new(
        #[builder(name = with_burn, default = Defaults::from_machine().burn)] burn: usize,
        #[builder(name = with_jobs, default = Defaults::from_machine().jobs)] jobs: usize,
        #[builder(name = with_tasks, default = Defaults::from_machine().tasks)] tasks: usize,
        #[builder(name = with_io_threads, default = Defaults::from_machine().io_threads)]
        io_threads: usize,
        #[builder(name = with_io_operations, default = Defaults::from_machine().io_operations)]
        io_operations: usize,
    ) -> Self {
        let all_cores = core_affinity::get_core_ids().unwrap_or_default();
        let burn_cores = all_cores.into_iter().take(burn).collect();
        let jobs = jobs.max(1);
        let tasks = tasks.max(1);
        let io_threads = io_threads.max(1);
        let io_operations = NonZeroU64::new(io_operations.max(1) as u64).unwrap();

        let runtime = Self {
            inner: Arc::new(RuntimeInner {
                watchdog: Event::new(),
                metrics: Metrics::new(),
                shutdown: Shutdown::new(),
                petitioner: OnceLock::new(),
                burn: Burn::new(burn_cores),
                job: Job::new(slots(jobs)),
                task: Task::new(slots(tasks)),
                io: Io::spawn(slots(io_threads)),
                io_operations,
            }),
        };
        let petitioner =
            Petitioner::spawn(runtime.inner.metrics.shared_active.clone(), runtime.clone());
        runtime.inner.petitioner.set(petitioner).ok();
        runtime
    }

    pub fn pipeline<W>(self, builder: impl Run<W>) -> Runner
    where
        W: Set + 'static,
    {
        builder.run(self)
    }

    pub(crate) fn watchdog(&self) -> &Event {
        &self.inner.watchdog
    }

    pub(crate) fn io(&self) -> &Io {
        &self.inner.io
    }

    pub(crate) fn metrics(&self) -> Metrics {
        self.inner.metrics.clone()
    }

    pub(crate) fn shutdown(&self) -> &Shutdown {
        &self.inner.shutdown
    }

    pub(crate) fn petitioner(&self) -> &Petitioner {
        self.inner
            .petitioner
            .get()
            .expect("runtime petitioner was not initialized")
    }

    pub(crate) fn acquire(&self, strategy: Strategy) -> Option<Lease> {
        match strategy {
            Strategy::Burn => self.inner.burn.acquire(),
            Strategy::Job => self.inner.job.acquire(),
            Strategy::Task => self.inner.task.acquire(),
        }
    }

    pub(crate) fn release(&self, lease: Lease) {
        match lease {
            Lease::Burn(core) => self.inner.burn.release(core),
            Lease::Job(slot) => self.inner.job.release(slot),
            Lease::Task(slot) => self.inner.task.release(slot),
        }
    }

    pub(crate) fn burn(&self) -> usize {
        self.inner.burn.capacity()
    }

    pub(crate) fn io_threads(&self) -> usize {
        self.inner.io.len()
    }

    pub(crate) fn jobs(&self) -> usize {
        self.inner.job.capacity()
    }

    #[allow(dead_code)]
    pub(crate) fn io_operations(&self) -> NonZeroU64 {
        self.inner.io_operations
    }
}
