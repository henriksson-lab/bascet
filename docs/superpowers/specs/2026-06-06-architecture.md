# bascet 2.0 architecture

## Priorities

1. Performance: hot paths must avoid locks, avoid unnecessary allocation, and keep worker-local writes
   off scheduler-read cache lines where practical.
2. API friendliness: users should write `Source`, `Stage`, and `Sink` implementations without seeing the
   worker machinery.
3. Implementor friendliness: internal boundaries should be simple, but not at the cost of throughput or
   user-facing clarity.

## Naming style

- Prefer one-word type names when the word is specific enough: `Runner`, `Pipeline`, `Wire`,
  `Balancer`, `Group`, `Worker`, `Lease`, `Spawn`, `Signal`, `Message`, `Runtime`, `Shutdown`,
  `Metrics`, `Mode`, `Strategy`.
- Avoid vague names such as `Node`.
- Do not introduce `LayerId`; a group's index inside the balancer is enough internal identity.
- Fields that are internal plumbing and should not be touched directly are prefixed `inner_`.
- Counts are named with `countof_`: `countof_processed`, `countof_sourced`, `countof_active`,
  `countof_workers`.
- `Balancer` is the canonical name for the scheduling coordinator. Do not reintroduce `Scheduler`.

---

## Type inventory

### Eliminated

| Type | Reason |
|---|---|
| `Parallelism` | Count fields move into `Mode`. |
| `Scheduler` | Use `Balancer`; it is the single scheduling coordinator. |
| `LayerId` | Not needed; group position is enough internally. |
| `Node` | Too vague; use `Wire` for channel plumbing and `Group` for runtime scheduling. |

### New

| Type | Role |
|---|---|
| `Wire` | Passive request/result channel edge in the assembled graph. |
| `Balancer` | Owns scheduling state, execution slots, groups, and worker lifecycle. |
| `Group` | Runtime scheduling record for one logical `Source` or `Stage`. |
| `Worker` | One live thread/task running one source/stage instance. |
| `Lease` | Claimed execution capacity: `Burn(CoreId)`, `Job`, or `Task`. |
| `Runtime` | Shared executor/event context used by workers. |
| `Metrics` | Shared counters and stall-related measurements. |
| `Id` | Newtype around `u64` for worker identity. |
| `Message` | Worker-to-balancer event. |

### Modified

| Type | Change |
|---|---|
| `Runner` | Owns `Pipeline`, `Balancer`, `Runtime`, `Shutdown`, and `Metrics`. |
| `Pipeline` | Becomes a passive assembled channel graph; no scheduling, shutdown, metrics, or runtime ownership. |
| `Mode` | Absorbs `Parallelism` using `countof_*` fields. Strategy stays separate. |
| `Scheduling` | Per-worker runtime state created by `Balancer`; no standby field. |
| `Signal` | Adds `Demote`. |
| `Stage` / `Source` | Require `Owned<Mode, Value = Mode> + Owned<Strategy, Value = Strategy>`. |
| `Sink` | Keeps `drive()`; scheduled sinks are out of scope for 2.0. |
| `Spawn` | Factory owned by `Group`; creates workers for the group's source/stage. |

---

## Ownership model

```rust
pub struct Runner {
    inner_pipeline: Pipeline,
    inner_balancer: Balancer,
    inner_runtime: Runtime,
    inner_shutdown: Shutdown,
    inner_metrics: Metrics,
}
```

`Runner` is the lifecycle owner. Public lifecycle methods live here:

```rust
impl Runner {
    pub fn builder() -> PipelineBuilder<...>;
    pub fn shutdown(&self);
    pub fn join(&self);
    pub fn any_active(&self) -> bool;
    pub fn metrics(&self) -> &Metrics;
}
```

`Pipeline` is not the runtime owner. It is the assembled channel graph.

```rust
pub(crate) struct Pipeline {
    inner_wires: Vec<Wire>,
}
```

`Wire` is the graph vocabulary. It is passive plumbing between two logical layers.

```rust
pub(crate) struct Wire {
    // Type-erased ownership/metadata for graph lifetime and inspection.
    // Typed channel handles are cloned into Spawn implementations during build.
}
```

The typed pipe builder may keep most channels in generic code while assembling the graph. If the
runtime needs to retain heterogeneous wires, `Pipeline` stores erased wire guards or metadata. No
worker loop reads `Pipeline` on the hot path.

`Runtime` holds shared execution primitives.

```rust
pub(crate) struct Runtime {
    pub(crate) inner_task_runtime: Arc<tokio::Runtime>,
    pub(crate) inner_trycheck_stalled: Arc<Event>,
}
```

`Metrics` holds counters.

```rust
pub struct Metrics {
    pub countof_processed: Arc<AtomicU64>,
    pub countof_sourced: Arc<AtomicU64>,
    pub countof_active: Arc<AtomicUsize>,
}
```

Workers receive cheap clones of `Runtime`, `Metrics`, `Shutdown`, and the balancer message sender.
They do not receive `Arc<Pipeline>`.

---

## Graph assembly

The builder has two jobs:

1. Assemble `Wire`s between `Source`, `Stage`s, and `Sink`.
2. Register exactly one `Group` with the `Balancer` for each logical `Source` or `Stage`.

For a pipeline:

```text
Source -> StageA -> StageB -> Sink
```

the runtime shape is:

```text
Runner
  inner_pipeline
    Wire(Source -> StageA)
    Wire(StageA -> StageB)
    Wire(StageB -> Sink)
  inner_balancer
    Group(Source)
    Group(StageA)
    Group(StageB)
```

There is one group per logical source/stage, not one group per worker clone.

Wrong:

```text
StageA countof_workers = 4
builder clones StageA 4 times
builder sends 4 Register messages
balancer spawns 4 workers per register
result: 16 workers
```

Right:

```text
StageA countof_workers = 4
builder creates one Spawn for StageA
builder sends one Register message
balancer creates one Group
balancer spawns 4 workers in that Group
result: 4 workers
```

`Spawn` is the runtime-safe form of "make another worker for this logical source/stage". It is
constructed while assembling the graph, captures cloned channel handles, and moves into `Group`.

---

## Mode and strategy

`Strategy` and `Mode` are orthogonal. `Strategy` describes the preferred execution tier.
`Mode` describes whether the balancer may move the group and how many workers it may keep alive.

```rust
pub enum Strategy {
    Burn,
    Job,
    Task,
}

pub enum Mode {
    Auto {
        countof_workers: NonZeroU32,
        countof_min: NonZeroU32,
        countof_max: NonZeroU32,
    },
    Manual {
        countof_workers: NonZeroU32,
        countof_min: NonZeroU32,
        countof_max: NonZeroU32,
    },
}
```

`Mode::Auto` allows the balancer to promote, demote, fork, or replace workers within
`countof_min..=countof_max`.

`Mode::Manual` pins the group's strategy. Manual workers do not self-promote or self-demote, and
the balancer does not select them as demotion victims.

Public constructors and derive attributes can stay friendlier than the internal field names, but
the stored fields follow the `countof_` convention.

---

## Traits

### Emit types

```rust
// Returned by Stage::apply. Stages do not end the stream.
pub enum Emit<T> {
    None,
    Value(T),
    Error(Box<dyn std::error::Error + Send + Sync>),
}

// Returned by Source::produce. Sources may end the stream.
pub enum Output<T> {
    Value(T),
    Shutdown,
    Error(Box<dyn std::error::Error + Send + Sync>),
}
```

### Stage

```rust
pub trait Stage: Layer + Owned<Mode, Value = Mode> + Owned<Strategy, Value = Strategy> {
    type Input<'a>;
    type Output;

    fn apply<W: Set>(&mut self, input: Self::Input<'_>) -> Emit<Self::Output>;
}
```

### Source

```rust
pub trait Source: Layer + Owned<Mode, Value = Mode> + Owned<Strategy, Value = Strategy> {
    type Output;

    fn produce<W: Set>(
        &mut self,
        req: Request,
    ) -> impl Future<Output = Output<Self::Output>> + Send;
}
```

Sources use the same `Spawn`/`Register` path as stages. Source workers may run at any strategy
level, including `Burn`.

Source concurrency must be explicit. Cloning a stateful source can duplicate input. The default
source mode should therefore use `countof_workers = 1`; parallel sources must coordinate through
shared state or range requests such as `Request::Read`.

### Sink

```rust
pub trait Sink: Layer {
    type Input<'a>;

    fn consume<W: Set>(&mut self, input: Self::Input<'_>) -> impl Future<Output = ()> + Send;

    fn drive<W: Set + 'static, T: Send + 'static>(self, ...);
}
```

`drive()` is kept. A scheduled `Sink`/`SinkSpawn` is out of scope for 2.0.

---

## Balancer

```rust
pub(crate) struct Balancer {
    inner_msg_rx: Receiver<Message>,
    inner_groups: Vec<Group>,
    countof_active: Arc<AtomicUsize>,
    inner_next_id: u64,
    inner_burn: VecDeque<CoreId>,
    inner_job: VecDeque<()>,
    inner_task: VecDeque<()>,
}

struct Group {
    mode: Mode,
    strategy: Strategy,
    spawn: Box<dyn Spawn>,
    countof_active: Arc<AtomicU32>,
    workers: Vec<Worker>,
}

struct Worker {
    id: Id,
    group_idx: usize,
    sched: Arc<Scheduling>,
    signal: Sender<Signal>,
}

enum Lease {
    Burn(CoreId),
    Job,
    Task,
}
```

Workers for a group live in `Group::workers` regardless of their current strategy tier.
The `inner_burn`, `inner_job`, and `inner_task` deques track only available execution slots.

`Group::can_spare()` is:

```rust
workers.len() > mode.countof_min()
```

The balancer never evicts a worker from a group where `can_spare()` is false, and never force-demotes
manual workers.

### Id

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Id(pub u64);
```

### Spawn

```rust
pub(crate) trait Spawn: Send {
    fn spawn(
        &self,
        id: Id,
        group_idx: usize,
        sched: Arc<Scheduling>,
        signal_rx: Receiver<Signal>,
        countof_active: Arc<AtomicU32>,
    );
}
```

`Spawn::spawn` takes `&self`; the balancer calls it repeatedly from the same group for initial
workers, forks, replacements, and respawns.

### Signal

Balancer to worker:

```rust
pub(crate) enum Signal {
    Stop,
    Demote,
}
```

`Stop` terminates a worker without replacement. `Demote` asks a worker to exit and report a demotion
so the balancer can cascade it to a lower tier.

### Message

Worker to balancer:

```rust
pub(crate) enum Message {
    Register {
        mode: Mode,
        strategy: Strategy,
        spawn: Box<dyn Spawn>,
    },
    Retire(Id),
    Demote {
        id: Id,
        group_idx: usize,
        level: Strategy,
    },
    Promote(Id),
}
```

Workers include `group_idx` and `level` in `Message::Demote` so the balancer can handle both normal
self-demotion and force-eviction messages whose worker entries have already been removed.

---

## Balancer logic

### Register

```text
on_register(mode, strategy, spawn):
    group_idx = inner_groups.len()
    inner_groups.push(Group {
        mode,
        strategy,
        spawn,
        countof_active: Arc::new(AtomicU32::new(0)),
        workers: [],
    })
    for _ in 0..mode.countof_workers:
        try_spawn_best(group_idx, strategy)
```

`do_spawn_*(group_idx, lease)` allocates an `Id`, builds `Arc<Scheduling>`, calls
`inner_groups[group_idx].spawn.spawn(...)`, increments `Metrics::countof_active`, increments
`Group::countof_active`, and pushes a `Worker` into the group.

### Retire

Called when a worker received `Signal::Stop` or panicked and has now exited.

```text
on_retire(id):
    metrics.countof_active--
    worker = find_and_remove(id)
    if not found: return
    group.countof_active--
    release worker lease back to inner_burn / inner_job / inner_task
```

There is no respawn on `Retire`. A `Stop` is explicit.

### Demote

Called when a worker steps down because patience expired or because it received `Signal::Demote`.

```text
on_demote(id, group_idx, level):
    metrics.countof_active--
    worker = inner_groups[group_idx].remove(id)
    if not found:
        // Force-evicted: balancer already removed the Worker and reused its Lease.
        // The worker exited without touching Group::countof_active; decrement it now.
        inner_groups[group_idx].countof_active--
        return

    match level:
        Burn:
            release Burn lease
            try_spawn_job(group_idx)
        Job:
            release Job lease
            try_spawn_task(group_idx)
        Task:
            release Task lease
```

### Promote

Called when a worker reports sustained saturation.

Burn workers fork: they send `Message::Promote(id)` and continue running. The balancer tries to add
a sibling, respecting `countof_max`.

Job and Task workers move up: they send `Message::Promote(id)` and exit. The balancer removes their
old worker entry, releases the old lower-tier lease, and spawns a replacement at the best available
higher tier. If promotion cannot improve the tier, the replacement may stay at the same tier.

Promotion never violates `Mode::Manual`, `countof_min`, or `countof_max`.

### Force eviction

```text
force_evict_*(id) -> lease:
    worker = find_and_remove(id)
    worker.signal.send(Signal::Demote).ok()
    return worker lease directly to caller
```

Force eviction does not decrement `Metrics::countof_active` and does not decrement
`Group::countof_active`. The evicted worker is still alive until it handles `Signal::Demote`; its
orphaned `Message::Demote` performs the count decrement in the "not found" path.

---

## Scheduling

`Scheduling` is per-worker state created by the balancer.

```rust
pub struct Scheduling {
    pub mode: Mode,
    pub(crate) inner_strategy: AtomicU8,
    pub(crate) inner_core_id: AtomicUsize,
    pub(crate) countof_idle: CachePadded<AtomicU32>,
    pub(crate) inner_patience: AtomicPatience<AtomicU32>,
}
```

Balancer-read fields and worker-write fields should stay on separate cache lines where practical.
`countof_idle` replaces vague "laziness" language: it is the observed count of idle misses used to
choose demotion victims.

---

## Worker behaviour

Every worker entry point wraps the worker body and sends exactly one terminal message:

```rust
let result = std::panic::catch_unwind(AssertUnwindSafe(|| worker_main(...)));
if result.is_err() {
    tracing::error!("worker {id:?} panicked");
}
msg_tx.send(Message::Retire(id)).ok();
```

Workers that intentionally promote or demote send `Message::Promote` or `Message::Demote` instead
of `Message::Retire`; the wrapper must not double-send.

Before self-demoting, an auto worker checks the group's live count:

```rust
let prev = countof_active.fetch_sub(1, Ordering::AcqRel);
if prev <= mode.countof_min().get() {
    countof_active.fetch_add(1, Ordering::Release);
    // Stay alive.
} else {
    // Send Message::Demote and exit.
}
```

Manual workers skip self-demotion and self-promotion.

Workers receiving `Signal::Demote` exit without touching `Group::countof_active`; the balancer
handles the count when it receives `Message::Demote`.

Workers receiving `Signal::Stop` exit without touching `Group::countof_active`; the balancer handles
the count when it receives `Message::Retire`.

---

## Invariants

- `Metrics::countof_active` is the number of worker threads/tasks currently alive, including workers
  whose terminal messages have not yet been processed.
- `Group::countof_active` approximates the number of live workers in that group. It may briefly
  exceed `workers.len()` during transitions and converges when orphaned messages are processed.
- A lease in `inner_burn`, `inner_job`, or `inner_task` means no worker is using it.
- `inner_burn.len() + countof_burn_workers == countof_burn_slots` at all times; same for job and
  task leases.
- Force eviction removes the worker entry and transfers its lease directly to the caller; it does not
  return the lease to the available deque.
- `can_spare()` is checked before force eviction.
- Manual groups are never force-demoted, self-demoted, or self-promoted.
- `countof_workers` is the initial group size, `countof_min` is the lower bound, and `countof_max`
  is the upper bound for auto promotion/forking.
- The balancer event loop is single-threaded; all group and lease mutations are race-free inside it.
