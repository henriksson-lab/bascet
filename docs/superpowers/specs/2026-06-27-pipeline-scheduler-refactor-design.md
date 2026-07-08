# Pipeline Scheduler Refactor

Revised 2026-07-05 after review. Revised 2026-07-08: scheduler raised to runtime scope; event plane, Emit, gather, and shutdown reworked to match.

## Goal

Replace the petition scheduler with one runtime-level scheduler behind a concrete event plane. Scheduling runs as a single event loop — the *driver*, the resident task on the System thread; workers, edges, and the build talk to it by sending events. The entire event plane is concrete — `Event`, `Action`, `Port`, `Worker`, and the `Scheduler` trait carry no type parameters — so the channel is uniform, `Auto` is a plain struct, and custom schedulers need no bounds gymnastics. The data plane is fully typed and monomorphised per layer; nothing dynamic sits on a per-item path. Termination is a return value, joining is completion-based, shutdown is a symmetric closure cascade, and OS threads are a fixed set created at build — workers are closures sent to parked threads, never spawned.

Design rule applied throughout: every mechanism rides an existing system. Pressure bands gate steady-state signaling; patience carries every other cadence — hot-path checkpoints, wait behavior, liveness re-nudges. Channel closure is the only termination signal, kanal is the only wait primitive, and directives to workers are stores to atomics on handles that already exist.

Cadence rule, three tiers: **per-item** is reserved for free loads with urgent semantics — exactly one exists, the `Halt` check; **checkpoints** carry everything periodic on the hot path (`Yield`, executor yield, gauge folds); **natural pauses** carry the rest (demote hints, counter flushes). A pause counts as a checkpoint — the cold path does strictly more. Any future "how often is X checked" question answers itself by sorting into a tier.

## Object model

### `Port` — a layer's shared face

```rust
pub struct Port {
    load: Load,               // packed gauge: one AtomicU64, 4 × 16-bit activity counts
    demand: Pressure,
    events_tx: Sender<Event>, // the one runtime channel; every port holds a clone
}
```

`Port` is everything another party may know about a layer: its gauges, and the runtime channel on which to petition the scheduler about it. It is the subject and sender stamp on every `Event`, the registry entry the scheduler ranks victims by, and the address edges promote. Tier is not here — tier is a worker-level fact, named by the worker's slot.

Ports are individually `Arc`'d; the registry is `Box<[Arc<Port>]>` on `RuntimeInner`, set exactly once by `Pipeline::build` (topology is fixed at build; one pipeline per runtime — intended, and fan-in does not change it). Events, edges, and workers hold `Arc<Port>` clones. The control plane is band-thinned, so a refcount bump per event is free; there is no lifetime contract to police and nothing unsafe to audit.

### `Layer` — the worker-shared, immutable piece

```rust
pub struct Layer<U: Gather, Out> {
    upstream: U,                 // the gather: typed edge handles, arity 1 for linear
    downstream: Downstream<Out>,
    port: Arc<Port>,
}
```

Immutable after wiring, `Arc`-shared with every worker of the layer. kanal endpoints operate through `&self`, so one set of edge handles serves all workers — nothing is cloned per worker beyond the `Arc` itself. Workers reach the world exclusively through `self.layer` and `self.runtime`. The upstream is a `Gather` — arity 1 is the linear case, zip instantiates the variadic tuple. The downstream stays scalar: even merge needs no variadic downstream — every producer of a merge holds the same MPMC edge — and fan-out, if it ever earns its keep, lands in `Emit` and `Connect`, not here.

The scheduler's per-layer mutable state — the pristine template inside the mint (`Clone` mints per-worker instances; it never crosses threads, so the apply traits need only `Clone + Send + 'static`), the roster, the learned start values — is plain per-layer fields of the one scheduler and its `Driver`. No other struct exists.

Sources are kicked with one build-synthesized `Promote` each — nothing has demand history yet, and zero-width revival is the case whose answer must be a spawn, so the kick needs no special path. Everything else starts asleep: **sleeping at zero workers is a valid layer state** — layers with pending work are revived by whoever waits on them (see Actions), and layers nothing waits on are never touched. Width bounds are scheduler configuration, not layer state, and are deferred entirely (see Open questions).

### `Worker` — the one shared handle

```rust
pub struct Worker {
    slot: Slot,              // which persistent thread / permit; identity, tier, and trace key
    port: Arc<Port>,         // its layer's port: activity mirroring, the guard's Released
    preempt: AtomicU8,       // scheduler-written half
    // — CachePadded boundary —
    state: AtomicU8,         // worker-written half: lifecycle
    activity: AtomicU8,      // instantaneous, mirrored into the layer's Load
    patience: AtomicU32,     // final checkpoint patience, published once by the guard at exit
}

pub enum State { New, Running, Released, Finished, Panicked }
```

Scheduler-written and worker-written halves live on separate cache lines (`CachePadded`), so the worker's own activity stores never contend with the scheduler's preempt writes. `State` is lifecycle (`Released` = idle/evicted exit, `Finished` = exhausted input or orphaned output, `Panicked` = unwound); `Activity` is what it is doing right now. Both readable by anyone holding the handle.

The verbs that touch the handle live on it: `set_activity` swaps the own atomic and applies the packed `Load` delta on the port in one method — the two can never desync — and the guard's exit path (`finish`: store the final `State`, publish patience, send `Released`) goes through the worker's own port. Composite cold-path choreography — patience decay, watchdog wake, re-nudge, the wait itself — spans runtime and run-locals and stays on the run value.

`Worker` is `Arc`-shared — the worker's thread, the scheduler's roster, and in-flight `Released` events genuinely overlap with unordered last drop. Everything else about a running worker is owned by its run value (see Worker loop), not a struct on the handle.

### `Event` and `Receipt`

```rust
pub struct Event {
    action: Action,
    subject: Arc<Port>,          // the layer the petition concerns
    sender: Arc<Port>,           // the SENDING layer's port
    worker: Option<Arc<Worker>>, // the sending worker, where one exists
    receipt: Option<Receipt>,
}

pub type Receipt = kanal::Sender<()>;
```

One runtime channel serves every layer, so the target moved out of the addressing and into the event: `subject` says who the petition concerns; `sender` and `worker` stay who is talking, whole, so the scheduler can read either side's gauges off the event itself. Attribution per action:

| action | `subject` | `sender` | `worker` |
|---|---|---|---|
| `Promote` | the layer to grow/revive | far side of the edge (band crossing, teardown nudge, patience-expiry re-nudge), own layer (request miss), or subject itself (build kick, scheduler-synthesized) | the motivating worker; `None` when synthesized |
| `Demote` | own layer | own layer | the idle worker |
| `Acquire` | the claimant layer | pool (self-posted) | `None` |
| `Released` | own layer | own layer | always the exiting worker |
| `Yield` | the layer to shed | the petitioner | `None` |

`Receipt` is confirmation, not data: the handler `send(())`s to confirm; dropping it unconfirmed closes the channel, which the requester reads as declined — NACK from drop order, panic-safe for free. One rule, trivially satisfiable now that the scheduler is the sole handler: **workers, on their own threads, may block on receipts freely; the scheduler resolves every receipt it holds during handling and never waits on one** — a receipt it waited on could only be resolved by itself.

## Apply

```rust
pub trait Apply: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    fn apply<Wants: Set>(&mut self, input: Self::Input, out: &mut Emit<Self::Output, Wants>)
        -> Result<(), Error>;
}

pub trait ApplyAsync: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    async fn apply<Wants: Set>(&mut self, input: Self::Input, out: &mut AsyncEmit<Self::Output, Wants>)
        -> Result<(), Error>;
}
```

`Contract` is folded in. `Input = ()` is a source, `Output = ()` is a sink, the general case is a stage — told apart by `Input`/`Output` alone.

- **Authors never name an execution model.** They write `fn` or `async fn`; the two traits unify internally through one sealed trait parameterized by an inference marker — `Work<Synchronous | Asynchronous>`, the markers named after the loop files they select, private ZSTs never written by users (the axum-handler pattern: coherence forbids blanket impls over "Future vs not-Future" directly, so a marker type must exist; `.layer()` infers it because any given type implements exactly one of the two traits). `Executable`, the old public marker structs, and the `Outcome` GAT leave the API; async unboxing falls out of AFIT — zero per-item allocation.
- **The scheduler is runtime configuration.** `Runtime::builder().with_scheduler(s)` carries a configured instance whose concrete type is inferred from the argument; the bare builder constructs an `Auto` explicitly — an explicit choice of the default policy, not a `Default` bound. Layers carry no scheduler and `.layer(apply)` is the only form; there is no `Scheduler` associated type.
- **`Emit<Out, Wants>` and `AsyncEmit<Out, Wants>` carry the outputs — two faces over one core, kanal's own `Sender`/`AsyncSender` pattern.** Everything that isn't the wait — the `wants` fold, reject accounting, the credit decrement, band signaling, the try-send fast path — is written once in a shared private inner; the twins differ only in the wait tail: `fn push` blocks under `Apply`, `async fn push` awaits under `ApplyAsync`. No marker reaches a signature, no future-shaped value reaches sync code. Zero pushes filter the item, one maps it, n flat-map it (zip entry → blocks, block → reads). `push(value)` writes straight into the output edge — no intermediate collection — and the backpressure wait plus the checkpoint live inside it. The raw kanal sender is never handed out: push carries protocol duties — credits, band signaling toward a dormant consumer, the patience-expiry re-nudge — that a raw send would silently bypass. Record outputs are built as plain local values through the existing `Put`/`Mut` attr traits, gated by `out.wants::<A>()`, and shipped whole with `push(r)` — there is no staged record and no bare commit: one verb, one arity. A source ends its stream with `out.finish()`; a stage never signals EOF — its end arrives as upstream disconnect.
- **`reject(reason: &'static str)`** is the audit path for *defective* items — corrupt record, failed checksum — as opposed to policy filtering, which is just not pushing. The static string doubles as a label key: a per-reason count in the worker's locals, folded at checkpoints, surfaced per layer at `join`; warn-logged rate-limited (first occurrence per reason, not per item). Unused it costs nothing; used it costs a local increment.
- **Errors are fatal.** `Err` from apply means the layer cannot continue — see Lifecycle. The three item outcomes: not pushed (policy), rejected (defect, audited), `Err` (the layer is broken).
- **What "next" means is the consumer's to define — with layers.** There is no `Pull` type, no request vocabulary, and no request machinery: static selection is construction config on the source apply; dynamic addressing is topology (see Serving). Batching never reaches an apply — authors think single-item on both sides, always (see Worker loop).
- **Checks are per edge, like everything else.** `Next::Requires: Subset<Producer::Provides>` at each `.layer()` call, against the direct producer only (the branch heads at a zip join). A carrying layer re-declares what its output carries, so the direct producer's `Provides` is the whole truth of an edge — there is no accumulated set, and the error lands on the offending call site. `Provides` reads "my output can carry these, and I fill them when wanted": `put` is bounded by `Output: Mut<A>`, but filling is conditional by design under demand-gating.
- **Carrying is expressed, not enumerated.** Records carry `type Attrs: Set`, emitted by the record derive (which already enumerates the fields), so a generic pass-through declares `Provides = R::Attrs` — one line, any record, exactly true because forwarding pushes the same record value. Carriers compose expressions: `Union<R::Attrs, (Score,)>` carries and adds; `Intersect<R::Attrs, (Id, Header)>` carries the overlap; a plain tuple states an exact promise. Arity is tupling: binary combinators, tuple operands.
- **Set expressions are real, normalized types.** Attr identity is a *type*: `Attr::Id`, a 16-tuple of hex-digit markers (`H0`–`HF`) and the only identity declaration an attr makes — `AttrId`, implemented by identity tuples, carries `const ID: u64` folded from the digits, so nothing const lives on `Attr` and there is one source of truth (`contains`, `wants()`, and the `inventory` registration read `<A::Id as AttrId>::ID`). Radix 16 is the deliberate sweet spot: 256 trivial `TEq` impls — equality over the *closed* digit alphabet, workable only because it is closed; it is not general type equality — and 16-wide short-circuiting `And` folds, matching the variadic machinery's arity. With identity in type-land, decisions are ordinary coherence: `AttrEq` (attr-level equality, answering `Hit`/`Miss`) → `In` (membership, an `Or`-fold) → `Select` (`Hit ↦ (A,)`, `Miss ↦ ()`) → `Concat` (internal fragment glue) — so filtering, dedup, and general intersection are constructible on stable Rust with no const-generic expressions. `Union<L, R>`/`Intersect<L, R>` are projection aliases over the `Join`/`Meet` op traits and *are* the deduplicated tuple — dedup preserves operand order; sets are compared by membership, never by cross-expression type equality — unification resolves on every set expression, and no symbolic/materialized split exists. `Subset` migrates to a membership-fold bound — non-subset-ness fails the bound itself at the offending `.layer()` call, `on_unimplemented` fires, no const asserts. `contains`/`out.wants()` keep the const value path, because the fixed `apply` signature cannot carry per-attr bounds. In generic code the ops thread as one ordinary bound per operation (`R::Attrs: Meet<K>` — appearing only in generic impls; concrete layers never write it); no inference is involved, which is why frunk was rejected (inference-driven list surgery cannot run on generic operands and proves presence, never absence) and typenum too (sixteen house digits beat foreign vocabulary). Costs accepted deliberately: compile time is a non-goal; the tuple arity ceiling is a regeneration knob on the variadic macros.
- **`Wants` is what the output is wanted for — and it types the emit.** Each layer's `Wants` is computed at its wiring step from its direct consumer alone: the consumer's `Requires ∪ (Wants ∩ Provides)`, seeded by `pipeline::<Wants>` at the boundary. It reshapes at every edge — an attr is wanted upstream only until the layer that provides it (plus one inert hop, since two sets cannot distinguish "I make this" from "I carry this") — and pass-along, mutation, drop-after-use, and new-type layers are all expressed by the two declarations that already exist. The method generic exists only to introduce the type; bodies consult the output: `out.wants::<A>()` delegates to `Set::contains::<A>()` — ID comparisons over consts, generated in the variadic block — and folds to a literal per monomorphized pipeline, so `if out.wants::<Blocks>() { r.put::<Blocks>(..) }` is not a branch: unwanted work is eliminated dead code — unfetched, unparsed, unallocated. Sound because the chain of wants is the same declarations the visibility check verified, one hop at a time.

## Actions and the scheduling flow

```rust
pub enum Action {
    Promote,   // directive: demand exists at the subject — spawn at zero width, grow if warranted
    Demote,    // directive: a worker reports idleness — shrink if the motivation recovered
    Acquire,   // a slot claim can now be served; pool-posted to its own loop
    Released,  // report: a worker exited; state, slot, and final patience ride event.worker
    Yield,     // directive: shed a worker at the subject
}
```

Two directives and one report carry all external traffic; `Acquire` and `Yield` thin to mechanism↔policy seams inside the one loop — self-posted events that defer a decision to the next iteration — but stay in the vocabulary: unused variants cost nothing, a regrown vocabulary costs churn, and external petitioners (watchdog, shutdown paths, custom schedulers) may still speak them.

`Promote` is the single demand action; zero-worker revival is just the case whose answer must be a spawn. Its senders: an edge on a band crossing, a worker on a request miss, the build's source kick, teardown's downstream nudge, and a **stuck neighbor's re-nudge** — a worker blocked or starved past patience expiry re-sends `Promote` with the far port as subject, bypassing the band gate, rate-limited by its own patience growth. The re-nudge is the liveness guarantee that keeps sleeping valid: a layer with pending work is revived by its waiters — which covers a panicked layer with buffered input too, since its consumer keeps pulling, starves, and re-nudges.

Shed guard: the scheduler must not shed a layer's **last** worker while its input edge is non-empty (one channel length read, cold path only) — a demote race must not orphan a buffered edge with nobody left downstream to nudge for it.

Signaling cadence is the existing band mechanism: `Pressure::miss()` emits only on a band increase, so event traffic thins exponentially as pressure grows; `Released`/`Demote` are rate-limited by worker lifecycle; re-nudges are rate-limited by patience growth. The event channel is unbounded — a scheduling event must never block a worker's hot path.

### `Scheduler`

```rust
pub trait Scheduler: Send + 'static {
    fn schedule(&mut self, event: Event, driver: &mut Driver);
}
```

One method, sync, non-generic, one instance per runtime. The event loop — recv, handle, repeat, teardown inline — is the resident task on the System thread. `Driver` is the concrete mechanism half the loop owns: the boxed mints (one per layer, created at build where `A` is still in scope), the pool, the port registry, the `Runtime`. Spawn, shed, swap, and teardown are its methods — policy invokes mechanism through it, and slots never appear in policy code. Acquire is a synchronous call: pool and policy share one owner and one thread, so consume-or-decline is call-or-withdraw, and no grant object exists. One policy instance sees every roster, so growth at one layer and eviction at another are one decision — tier-exact, because a worker's slot names its tier — not diplomacy between peers.

The **mint** is one boxed closure per layer: `FnMut(Tier, Patience) -> Job`. It clones the template, clones the layer's `Arc<Layer>` and the `Runtime` — three refcount bumps, the entire per-spawn cloning cost — and boxes the run closure. Its two arguments are the spawn parameters, both already-named concepts: the target tier, and a ready-made starting `Patience` the scheduler constructs from the start values it has learned for this layer.

### `Preempt` and checkpoints

```rust
#[repr(u8)]
pub enum Preempt {
    Continue = 0,   // keep running
    Yield    = 1,   // exit at next checkpoint
    Halt     = 2,   // exit at next item — immediate
}
```

The hot loop's per-item control cost is `streak += 1`, a register compare against patience, and **one relaxed load answering only `Halt`** — a predicted-not-taken branch on a cache line only the scheduler writes, the original unmeasurable claim; immediacy for forced shutdown and unresponsive victims is worth one load. When `streak >= patience` the worker takes a **checkpoint**:

- act on `preempt`: `Yield` (or `Halt`) → exit;
- async workers yield to the executor — cooperative fairness on a shared compio thread; nothing else returns control while the try-paths keep succeeding, and neither reading a flag nor the OS can preempt a future;
- per-worker gauge locals fold into the shared demand/request accounting;
- streak resets.

Patience updates only at checkpoint granularity: **grow** when a full streak completes unpreempted — nobody wants the slot, be greedier — and **shrink** on cold-path entry. Its cap is the **`Yield` latency contract**: worst-case polite-eviction latency in items (order 64–256), chosen deliberately and independent of the wait-patience maxima, which are sized for spin behavior. The adaptation aligns with victim selection — frequent cold-path workers (the likely victims) run polite with tight checkpoints, saturated greedy workers (rarely victims) run long streaks — and the cap covers the residual all-busy case. `Auto` evicts with `Yield` and escalates to `Halt` when it cannot wait.

Patience is one dimensionless self-tuning tolerance per worker with three consumers — wait strategy at misses, executor-yield cadence, checkpoint interval — each applying its own scaling constant. The constants live together in `consts.rs`; tuning one now tunes fairness too.

**Patience outlives the worker.** The guard publishes the final value on the handle; `Released` hands it to the scheduler, which folds it into the start values it keeps as per-layer fields and hands new workers their starting `Patience` through the mint. New workers start polite under contention, greedy in quiet times. A `Yield`/`Halt` observation is tier-contention information — "something else wants this tier" — and feeds two future decisions: the starting patience, and the scheduler's tier appetite (workers evicted young at Burn → ask for Job instead). It complements the pool's records: those show *current* contention, `Released` history shows how it *played out* for this layer. The adjustment policy is deferred with the rest of learning; the plumbing is part of this refactor.

### `Auto` — the default scheduler

Per-layer state — the roster (entries record the demand band at spawn), patience start values, tier appetite — is plain fields indexed by layer. Handling:

- **`Promote`** — if the demand/pressure heuristics warrant (desired width from demand strain, capped by useful width from `Load`), spawn — from zero width this is revival; or swap an existing worker a tier when a slot there is cheaper than a new worker.
- **`Demote`** — shrink when the motivating band has recovered; never the last worker over a non-empty input edge.
- **`Yield`** — pick the victim within the subject layer — lowest spawn-band, ties to oldest, filtered to the wanted tier when a claim is being served — and write its `preempt`. If the event carries a receipt and `Auto` judges the layer at its floor, drop the receipt — declined, and the petitioner tries the next donor.
- **`Acquire`** — the pool can serve the subject's standing claim: complete the acquire synchronously and spawn, or withdraw the claim if the demand has recovered meanwhile.
- **`Released`** — drop the roster entry; reclaim the slot; fold the published patience into the layer's start values; a `Finished` state triggers that layer's teardown.

A missed acquire sets the claim and picks a donor directly: rank layers by `Load` (`Starved + Blocked`), shed the weakest's cheapest worker on the claimed tier — a roster scan, not a message exchange.

Feedback is outcome-based, entirely inside `Auto`, no clocks and no shared knobs: a worker that goes `New → Released` young says the spawn was too eager; bands still climbing across the window after a spawn say it came too late; a shed followed promptly by a re-`Promote` says the shrink was too eager. The seed plumbing lands now; `Auto::learn` — how seeds and appetites adjust — stays the deferred seam, gated on `benches/`. `Pressure` growth/decay are plain config — the hot path carries no tuning loads.

## Pool and tiers

OS threads are a fixed set, created at `Runtime::build`, born into their tier: **Burn** — one per granted core, pinned at creation, never repinned; **Job** — unpinned; **Task** — user async on the compio dispatcher (payload: a concurrency permit); **System** — the single-thread compio runtime whose resident task is the scheduler's event loop, isolated from Task so user applies can never starve scheduling.

A sync slot **is** its persistent thread. The thread body is the whole dispatcher:

```rust
while let Ok(job) = inbox.recv() { job() }   // kanal recv is the park; send is the unpark
```

Spawning a worker is sending the mint's closure — nothing else travels. A run returning parks the thread back into the pool's free list. Panics are caught at the body: the guard has already sent `Released` with `Panicked`, the thread survives and parks. Threads are joined exactly once, when the `Runtime` drops and the inbox senders close. Nothing spawns or joins OS threads anywhere else.

Slot bookkeeping is per-tier, owned by the scheduler behind `Driver`:

```rust
struct Pool<S> {
    free: Vec<S>,                    // parked threads (Burn/Job) or permits (Task)
    layers: Box<[Record]>,           // indexed by layer
}

struct Record {
    pass: u64,                       // grants received
    waiting: Option<u32>,            // None = not waiting; Some(band) = standing claim
}
```

There is no claim queue — a claim is per-layer state, overwritten in place when the band moves. The bit is required because band signaling thins as pressure grows; without it a freed slot could idle until the next band crossing.

- **acquire**: serve a waiting layer first if one outranks the requester; else pop `free`; else set `waiting` — the caller (policy) picks a donor and sheds (see `Auto`).
- **release**: pick the winner among waiting records — **band-first, `pass` as tie-break** — and post `Acquire` for its layer to the loop; the handler completes the acquire synchronously or withdraws. `pass` bumps on the actual grant. If a higher-ranked latecomer takes the slot before the handler runs, that is correct policy — more starved wins — and the standing claim catches the next release.

**Tier movement is a swap**, not a repin: acquire on the target tier (through the normal claim path if it is full), spawn the replacement **first**, then shed the old worker — the layer's width never dips. The apply instance is re-minted from the template; per-worker state is scratch by contract, so nothing of value moves. Affinity never changes at runtime. Task is a different execution model, fixed per layer by which trait it implements, so movement exists only inside the sync ladder.

## Worker loop

```rust
fn run(mut self) -> State {
    loop {
        let input = self.wait_input()?;                    // demand topped up; misses are checkpoints
        self.apply.apply(input, &mut self.emit)?;          // Emit::push waits + checkpoints inside
        if self.worker.halted() { break }                  // per item: one relaxed load
        self.streak += 1;
        if self.streak >= self.patience { self.checkpoint()?; }
    }
    // guard fires on every path out
}
```

The run value owns everything the loop touches: the apply instance, patience and streak, metric locals, the `Arc<Worker>` handle, the `Arc`'d `Layer`, and `runtime: Runtime` — three `Arc` bumps per spawn, the rest freshly constructed. Helpers are `&mut self` methods; the loop calls `apply` directly. The apply itself never sees any of it. Its name is `Run<A>`, declared in `worker.rs` and private to the worker module — the name is free because `Connect` absorbed the old `Run` trait.

Two loop bodies, selected by the internal marker: a plain `fn` in `worker/synchronous.rs` with blocking waits, an `async fn` in `worker/asynchronous.rs` — never one generic loop with a const-branched await, or the sync instantiation becomes a pointless state machine polled through waker plumbing.

**Demand protocol** (invisible to applies): the consumer loop keeps `outstanding` and tops it up to a watermark — default half the edge depth — with one credit message per top-up; the producer counts **items pushed** against its budget, which composes with the emit (items, not calls). Credits are the only demand — descriptors are ordinary items (see Serving). Outstanding accounting stays worker-local; the shared demand gauge is touched at checkpoint granularity.

Waits are the existing spinpark/patience machinery; crossing into the cold path calls `runtime.watchdog().wake()` and is itself a checkpoint. The `WorkerGuard` is armed first thing and fires on return and on unwind: it folds counters, publishes the final checkpoint patience, stores the final `State` (`Released`, `Finished`, or `Panicked`), and sends `Released`. A worker's completion **is** its `Released` event; nobody joins anything per-worker.

## Hot-path accounting

The previous design's per-item shared-atomic traffic (≈8–12 RMWs per item on layer-shared cache lines) was the scaling limiter. This refactor removes it:

- **Per-worker locals, folded at checkpoints**: request/fulfill accounting, metric counters, patience, streak — plain integers in the run value. Shared state is touched at checkpoint and pause granularity only; checkpoints guarantee a bounded fold interval even for a worker that never misses.
- **`Pressure`**: value and band level packed into one atomic; `hit()` is a single unconditional `fetch_sub` on a signed representation with clamp-on-read and a rare `fetch_max(min)` repair when the floor is crossed — no CAS loops on any hot path; `miss()` emits only on a band increase; growth/decay are plain config.
- **`Load`**: four 16-bit activity counts packed into one `AtomicU64`; a transition is a single wrapping `fetch_add` of `(1 << new·16) − (1 << old·16)` — one RMW on one line, replacing two RMWs across four unpadded atomics. Performed by `Worker::set_activity`, which owns both the handle swap and the port delta.
- **`Worker`**: scheduler-written and worker-written halves on separate cache lines.
- **`Metrics`**: plain atomics on `RuntimeInner` — no per-counter `Arc`, no metrics handle to clone.

## Edges

Edges wake layers in both directions through the existing pressure banding — push-wake toward a slow or dormant consumer, pull-wake toward a dormant producer — sending `Promote` with the far side as subject, through the `Arc<Port>`s stored at wire-up. Stuck workers add the patience-expiry re-nudge past the band gate.

kanal endpoints are already cheap cloneable handles: the edge holds them directly — no `Arc` wrappers, no `Weak` upgrade dance. `Edge::Inner` is the channel ends, one gauge per direction, and both ports. One set of edge handles per layer, shared by all its workers through the `Arc`'d `Layer` (kanal ops take `&self`).

Depth is per edge, chosen at wire-up, budgeted by item weight (bytes) rather than one global item count — 32 arena blocks and 32 integers are not the same amount of memory.

The keeper mechanism: kanal voids buffered items on close or disconnect (verified by test — **keep that test as a permanent canary**; termination correctness rides this behavior), so at producer-side teardown a sender for the channel is parked in the edge (`OnceLock<Sender<T>>` on `Edge::Inner`). The keeper lives exactly as long as the consumers' edge handles — the buffer stays drainable until the last consumer is done, and nobody has to be told when that is.

### Serving

Serving is topology, not protocol. "A layer asking its upstream for whatever *it* would output for this range" decomposes into a **pair-component**: an ask-producing half (a source emitting descriptors in the upstream's vocabulary) and a consuming half (a stage), sharing a cursor through an `Arc` field — packaged as one user-facing unit by its constructor:

```rust
let (asks, parse) = Bgzf::parser();    // two halves, one fresh shared cursor

Pipeline::builder()
    .source(asks)      // Apply<Input = (),    Output = Range<u64>>  "your output for this range"
    .layer(reader)     // Requires = Range — any range-servable producer, anonymous to the asker
    .layer(parse)      // Apply<Input = Bytes, Output = Record>      consumes answers, steers the cursor
    .sink(write)
```

Demand is credits everywhere, flowing sink → parse → reader → asks through the normal watermark top-up. The back edge a request channel would have needed is the shared cursor at the value level — which is why the channel graph stays acyclic and no cycle machinery exists. An ask-half polled before the cursor has advanced pushes nothing; the normal starvation path parks it. The contract is the ordinary attr flow: the reader declares `Requires = Range` — "incoming requests to me carry the range they want" — the ask-half declares `Provides = Range`, and the existing checks cover it, because this *is* the existing item flow. Multi-hop translation composes the same way: a range-servable decompressor is itself such a pair (output-ranges translated to input-ranges above, inflation below), so asks chain hop by hop, each in its own vocabulary. Selective materialization needs no mode: the parser asks its output — `out.wants::<Blocks>()` — and steers the cursor toward header-sized or whole-block ranges; bytes nobody wants are never fetched.

## Fan-in

One pipeline per runtime is unchanged; both fan-in shapes live inside one pipeline.

**Merge** (same item type): several producer layers push into one shared MPMC edge — kanal gives this for free. EOF composes automatically: the pull side closes when the last producer's keeper drops. Builder: `.merge((a, b))`.

**Zip** (different item types): builder **segments** are un-terminated `PipelineBuilder`s — already values today:

```rust
let r1 = Pipeline::builder().source(fastq_r1).layer(parse);
let r2 = Pipeline::builder().source(fastq_r2).layer(parse);

Pipeline::zip((r1, r2))        // upstream type: (Option<Read1>, Option<Read2>)
    .layer(pair)               // Apply<Input = (Option<Read1>, Option<Read2>)>
    .sink(write)
```

The gather is an **option tuple**: one take waits on every still-live member positionally; `None` marks a member that is closed *and* drained — a slow member is waited on, never `None`d, so starvation and EOF stay distinct — and the input ends when every member is `None`. Uneven members are a valid, normal run: the join drains survivors to their natural EOF and the teardown cascade is uniform with the linear case. EOF policy lives in `Gather` — a fan-in with different run-out semantics is a different `Gather` impl. Early exit — a join that stops when any member goes `None` — is parked: stages cannot initiate EOF (`finish()` is source-only), so it waits on stage-initiated finish (see Open questions).

`bascet_variadic::variadic!` generates the tuple impls up to 16: the join layer's per-edge check runs against the branch heads' `Provides` (their `Union`); the upstream tuple implements `Gather` — top-up fans out to every member, take yields the option tuple. Each member edge keeps its own bands and wake targets, so scheduling composes with no changes. Apply authors see a tuple input, single-item, nothing else.

No new assembly trait carries any of this: `Connect` — whose duty is already "wire yourself, hand back your output stream" — gains the head case for an un-terminated builder and a variadic impl for tuples of them, returning a `Gather` (arity 1 for a lone edge, so a linear pipeline is the trivial instance of the same concept). The old `Run` trait is absorbed with it, freeing the name for the run value.

## Lifecycle and shutdown

**Build order** (`runtime.pipeline::<W>(Pipeline::builder().source(..).layer(..).sink(..))`): fill the port registry, wire edges with both `Arc<Port>`s (zip segments wire per branch before the join layer), register mints and pool records, start the scheduler loop on System, send one build-synthesized `Promote` per source layer. Edges before workers (the first push needs a promote address); the loop before the kicks (the first event needs a live channel).

**Termination is a return value.** A source ends its stream with `out.finish()`; a stage finishes when its upstream reads `Disconnected` — or when its push meets a closed edge: **downstream disconnect is upstream disconnect's twin**, and `Finished` means exhausted input or orphaned output.

**Closure cascades symmetrically at the edge level.** An edge shuts down when its upstream side is gone — a terminated producer, or the `()` boundary — or its upstream edge has closed: draining first, because the keeper holds the buffer for consumers. And it shuts down when its downstream side is gone or its downstream edge has closed: voiding immediately, because no consumer can exist. Sources and sinks are the degenerate `()` boundaries of one rule, not special cases.

**Teardown** — run inline by the scheduler when a layer's roster empties after a `Finished`:

1. Close the pull side of the output edge. This is the finished signal — channel closure, no flag.
2. Park the keeper in the edge.
3. Send one `Promote` with the downstream port as subject, in case it sleeps at zero workers with items buffered.
4. Done. `Released` bookkeeping already reclaimed the slots; there is nothing to join.

**Errors.** Two classes, nothing in between:

- *Recoverable* (corrupt record, dropped entry): handled inside apply — emit nothing, `out.reject(..)` for the warn log and audit counter, continue.
- *Fatal* (`Err` from apply): the layer tears down exactly like `Finished` — the same four steps — with the error recorded on the runtime, and `Shutdown` is triggered for prompt upstream cleanup. Downstream cascades through the same empty-and-closed path; `Runner::join` returns the first recorded error. Upstream needs no signal for correctness — its pulls stop and it sleeps; shutdown only makes the end prompt.

**Consumers detect the end where they already look**: the miss path gains one condition — output empty *and* pull side closed means empty-forever. The consumer writes off its outstanding requests against that edge and returns `Finished`, continuing the cascade. Fan-in composes: merge ends when the last keeper drops; zip ends when every member has closed and drained.

**`Runner::join`** blocks on the completion the scheduler sends when the sink layer tears down, then fires the `Shutdown` handle to reap survivors — layers asleep at zero workers with no consumer left to wake them; a no-op for a linear pipeline that ended naturally — then awaits the remaining teardowns, which are now guaranteed. It returns `Result<(), Error>`: the first fatal error wins, a panic converts into the same `Error` with its payload and layer attached, and everything after the first is a consequence of shutdown — warn-logged, not aggregated. It holds its `Runtime` clone, which owns the ports, threads, and pool.

**Forced shutdown** skips the niceties: the `Shutdown` handle closes channels immediately, voiding whatever is in flight, and the same teardown runs.

## Watchdog

One stall detector, `wake()`-driven from cold-path entry, on its own thread. Progress is measured **per edge**: crossing counts folded from worker locals at checkpoints. The global `sourced > processed` pair is gone — any filtering layer made it permanently true. Stall predicate: zero `busy` across all ports at both probe endpoints, no edge crossing count moved, and some edge non-empty. A worker that is `Busy` remains progress by definition — the framework trusts user code for the duration of one item.

Instrumentation note: tracing is debugging scaffolding — the refactor carries only worker-panic and fatal-error reporting; everything else is deletable.

## Open questions

1. **Builder type-state plumbing — unproven, not undecided.** Each wiring step checks `Requires: Subset<Provides>` against the direct producer, derives `Wants = Union<Requires, Intersect<Wants, Provides>>`, infers the sync/async marker, and threads tuple upstreams for zip; no accumulated state remains. The close-out is a types-only compile test: stub applies with real associated types, one linear chain, one zip — asserting that inference resolves, that `Union`/`Intersect` normalize to the expected tuples (type-equality asserts against the deterministic dedup order), and that the derived `Wants` memberships answer correctly through `contains`. The trait-solver failure list, if any, is the remaining work.

2. **Stage-initiated finish.** A stage cannot currently end its own stream — `finish()` is source-only, and a stage's end arrives as upstream disconnect. Early-exit zip (stop at the first `None`) and any future "I have seen enough" stage need it; without it they degenerate to drain-and-discard. Parked until a consumer forces the design.

Parked: per-layer width bounds (scheduler configuration if ever needed — not layer state, not builder API); per-branch `Wants` in zip (each member edge already derives its own, nothing structural blocks it).

Resolved since the first draft: grant selection (band-first, `pass` tie-break); policy → mechanism surface (`schedule(&mut self, event, &mut Driver)`); scheduler provenance (a configured instance on the runtime builder, no `Default` bound anywhere); `Pressure`'s home (stays in `utils/`); fatal errors (`join() -> Result<(), Error>`, first error wins, panics convert, the rest warn-logged); segments (no new trait — `Connect` gains the head case and tuple impls, retiring the old `Run` trait); request machinery (deleted whole — serving is topology: descriptor-producing layers, shared state, credits as the only demand; `Pull` deleted). Resolved in this revision: scheduler scope (one runtime-level instance — per-layer drivers bought no parallelism on one System thread and mixed policies over one pool are adversarial); grant transport (none — the pool is a synchronous call behind `Driver`); the initial kick (a build-synthesized `Promote`); `Port.tier` (deleted — tier is the slot's fact); `Emit` (twins over one core, kanal's pattern; staging deleted); zip (option-tuple gather); shutdown (symmetric edge cascade; join keys on the sink, then reaps); `Pipe` (cons-list only, fusion deferred).

## Decision log

Each entry: the choice, over what, and why.

- **One runtime-level scheduler** over per-layer drivers, and over the old petition thread: per-layer instances bought no parallelism — every driver already multiplexed the one System thread — and mixed policies sharing one pool are adversarial by construction; eviction between peers was diplomacy (`request_yield`, cross-scheduler `Yield`, receipt-NACK rounds) that a single owner does as a tier-exact roster scan. Scheduling *state* stays per-layer — plain fields indexed by layer — the policy that reads it is one instance.
- **Events name their sender — and, now, their subject** over receiver-typed `Event<A>`: with one runtime channel the target moved out of the addressing and into the event; `subject` says who the petition concerns, `sender`/`worker` stay who is talking, and the whole event plane remains non-generic while every control decision dispatches statically inside the one monomorphised loop.
- **`Arc<Port>` per event** over `SendPtr` + registry lifetime contract, and over index addressing: the control plane is band-thinned, so a refcount bump per event is free; the pointer scheme was a convention-enforced UB hazard riding in queues at teardown; indices were rejected on ergonomics. Symmetric with `Arc<Worker>`.
- **`Worker` stays `Arc`**: thread, roster, and in-flight events genuinely overlap with unordered last drop.
- **`Receipt = kanal::Sender<()>`** over a custom oneshot: kanal waits both sync and async on one channel, and drop-as-decline gives panic-safe NACK for free. The rule collapses with the topology: workers may block on receipts; the scheduler resolves held receipts during handling and never waits on one.
- **Five actions, kept through the centralization** over shrinking to the three that carry external traffic: `Acquire`/`Yield` thin to self-posted mechanism↔policy seams, but unused variants cost nothing, a regrown vocabulary costs churn, and external petitioners may still speak them. (`Wake` stays folded into `Promote`: zero-width revival and a live layer's capacity request are one decision whose zero case must spawn.)
- **`State` lifecycle on `Worker`** over an `Exit` payload on `Released`: richer, and the action needs no payload. The final checkpoint patience is published beside it.
- **A worker's slot is its identity — and names its tier** over a `WorkerId` counter and over tier as layer state: a slot — literally a persistent thread or Task permit — is held by exactly one live worker, is already the trace key, and is born into its tier.
- **Completion-based joining (`Released` is the completion)** over joining OS handles: the scheduler loop never blocks on a join; panic reporting rides `State::Panicked`.
- **Pull-side closure as the finished signal, keeper parked in the edge** over a drained-notification event or finished flag: channel closure already exists, the consumer's miss path already runs, and the keeper's lifetime needs no coordination. Forced by kanal semantics (buffered items do not survive close — tested; the test stays as a canary).
- **No grant object** over grant-travels-with-`Acquire` and over a reservation array: pool and policy share one owner and one thread, so acquire is a synchronous call and consume-or-decline is call-or-withdraw; `Acquire` the event is the pool's self-posted "a claim can be served now." A higher-ranked latecomer sniping the slot before the handler runs is correct policy — more starved wins — and the standing claim catches the next release.
- **Per-layer pool records** over a `BinaryHeap` of claims: one claim per layer is per-layer state, not a queue.
- **Persistent pinned threads + swap** over promotion-by-repin and over per-worker `thread::spawn`: spawn is a closure send to a parked thread — no syscall, no stack allocation — so evict-and-respawn is repriced; the old rejection priced it against thread creation and state movement, but threads are never created at runtime and apply instances are scratch minted from the template. Affinity never changes after build; the repin directive and field are deleted; panics stop killing threads.
- **`Yield` at checkpoints, `Halt` per item** over one cadence for both: normal eviction rides the adaptive checkpoint — the patience cap is its explicit latency contract, and the adaptation (grow on an unpreempted streak, shrink on cold-path entry) aligns polite workers with likely victims — while the immediate kill keeps one relaxed per-item load, predicted-not-taken on a scheduler-written line, because forced shutdown must not wait out a streak.
- **Emit out-parameter** over `Result<Option<O>>`: filtering and flat-map become possible (they weren't), the `Ok(None)` EOF double meaning dissolves, outputs stream without intermediate collections, and arena-aware emission gets its seam.
- **Twin emits over one core (kanal's own pattern)** over a marker parameter on `Emit` and over a dual-mode push future: `Sender`/`AsyncSender` is the precedent — two faces over one channel state, each with the natural wait for its world. Every sync part is written once in the shared inner; no ZST reaches a signature, no future-shaped value reaches sync code, and the raw sender is never handed out because push carries protocol duties (credits, band signaling, the re-nudge) a raw send would bypass.
- **Records built as local values; staging deleted** over Emit-as-staged-record: `Put`/`Mut` already exist on record types, `wants` gates filling just as well on a local, and one `push(value)` of one arity survives — the bare-commit `push()` collided with it (duplicate inherent method names are rejected regardless of bounds) and bought only sugar.
- **Two apply traits with an internal marker-inferred unifier** over the `Execution` associated type, the `Sync`/`Async` markers, and the `Outcome` GAT: authors write `fn` or `async fn` and never name an execution model; coherence forbids blanket impls over "Future vs not-Future" directly, so a marker must exist (`Work<Synchronous | Asynchronous>`, the axum-handler pattern), but it is sealed and internal. Async unboxing falls out of AFIT — the last per-item dynamic dispatch on the data plane, removed.
- **The scheduler is runtime configuration** over per-layer instances and over `Default` construction: `Runtime::builder().with_scheduler(s)` passes a value whose type is inferred; the bare builder constructs an `Auto` explicitly — no `Default` bound anywhere, no `Scheduler` associated type, and `.layer` takes only the apply.
- **Per-edge visibility over accumulated `Provides`**: a carrying layer re-declares carried attrs, so the direct producer's `Provides` is the whole truth of an edge; the accumulated union was one more piece of global state and a false-positive generator (`A` provides `X`, `B` drops it, `C` requires `X` — the union check passed while `C` read an absent field). Checks land at the offending `.layer()` call, and the builder loses its `Provides` accumulator entirely.
- **`Wants` returns to `apply`, typing the emit** over the first draft's deletion of the work-set parameter: computed per edge (the direct consumer's `Requires ∪ (Wants ∩ Provides)`, seeded by `pipeline::<Wants>`) instead of user-threaded, and consumed through `out.wants::<A>()` — `Set::contains` underneath — for demand-gated materialization that monomorphizes into dead-code elimination. Deleted when it had no consumer; restored when it earned one, located on the output because the output is what's wanted of.
- **Type-level identity; eager, normalized set ops** over symbolic membership-only combinators, over frunk, and over typenum: `Attr::Id` is a derive-emitted 16-tuple of hex digits (`H0`–`HF` — radix 16 is the sweet spot: 256 trivial `TEq` impls, 16-wide folds, the variadic arity) and `AttrId` carries the derived `const ID` — one source of truth, nothing const on `Attr`. Decisions become coherence (`AttrEq` → `In` → `Select` → `Concat`), so `Join`/`Meet` produce real deduplicated tuples (order-preserving — sets are compared by membership, never by cross-expression type equality), normalization is native everywhere, and `Subset` becomes a bound that fails naturally at the offending call. frunk rejected: inference-driven surgery cannot run on generic operands and proves presence, never absence. Costs accepted: compile time (waived), arity ceiling (a regeneration knob).
- **Credits hidden in the loop** over per-item pulls and over user-visible batching: apply authors think single-item; the loop tops up to a watermark; performant by default. Absorbs the parked adaptive-granularity idea; further adaptivity stays gated on `benches/`.
- **Serving is topology, not protocol** over `Pull::Read`, request-typed edges, `Want`, `open`, and `Emit::ask` — each designed, each collapsed into something that already existed: a server is a stage whose `Input` is a descriptor type, descriptors are produced by an upstream layer under the ordinary `Provides`/`Requires` contract, coordination is shared state inside the applies, and the pull channel carries credits only. An asking layer is a **pair-component** — ask-half source plus consumer stage around a shared cursor, packaged by one constructor — chosen over literal back edges, which would have cost multi-output emits, cycle wiring, and credit-flow deadlock analysis.
- **Packed, signed, fold-at-checkpoint gauges** over per-item CAS loops on layer-shared atomics: the shared RMW traffic was the scaling limiter.
- **`Layer<U: Gather, Out>` as the worker-shared immutable piece; scheduler state as plain per-layer fields** over a driver-owned `Layer` struct and over separate linear/zip layer types: the upstream is the gather (arity 1 is the linear case), workers get `self.layer` / `self.runtime` access, all of a layer's workers share one set of edge handles, and one noun disappears. The downstream stays scalar — even merge holds one shared MPMC edge — and fan-out, if it ever earns its keep, lands in `Emit` and `Connect`, not here.
- **Patience travels through `Released` into the next spawn** over letting eviction history die with the worker: guard publishes, scheduler remembers in plain per-layer fields, mint receives `(Tier, Patience)` — both already-named concepts, no spawn-parameter struct. Plumbing now; policy with learning.
- **Waiter re-nudge + last-worker shed guard** over wake-on-released-to-zero: sleeping at zero workers is a required valid state; liveness comes from whoever is waiting, rate-limited by their patience.
- **Fatal-error teardown as EOF-with-cause** over a parallel error channel: reuses the entire termination machinery; recoverable drops are apply-internal with an audit counter.
- **Downstream disconnect is upstream disconnect's twin** over surviving-branch orphans: a push into a closed edge is `Finished` (orphaned output), so closure cascades both ways at the edge — upstream-gone drains first (the keeper holds the buffer), downstream-gone voids immediately (no consumer can exist) — and sources and sinks are the `()` boundaries of one rule.
- **Option-tuple `Gather`** over strict-tuple zip EOF: `None` marks closed-and-drained, take waits on every live member, input ends at all-`None` — uneven members become a valid run instead of an orphaned branch, EOF policy lives in `Gather`, and early exit waits on stage-initiated finish (parked).
- **`join` keys on the sink, then reaps** over awaiting all teardowns blind: sink teardown → `Shutdown` for survivors — a no-op after a natural linear end — → the remaining teardowns are guaranteed to arrive.
- **`Pipe` is the cons-list; nothing runs in it** over porting its fusion: the `Apply for Pipe` impls die with `Result<Option>`; running several applies inside one worker returns, if it ever earns its keep, as an explicit combinator, not a hidden duty of the cons-list cell.
- **`join() -> Result<(), Error>`, first error wins** over aggregation: after the first fatal error, shutdown makes everything else a consequence — warn-logged, not collected; panics convert into the same `Error` with the layer attached; no error-collection noun is invented.
- **Merge on a shared MPMC edge; zip via variadic segments** over multi-pipeline composition: fan-in lives inside one pipeline; one-pipeline-per-runtime stands.
- **`Connect` absorbs segments and the old `Run` trait** over a new `Segment` trait: wiring is one duty, one trait — builders, layer cons-lists, and tuples of builders all implement it, returning a `Gather`; arity 1 is the linear case.
- **Per-edge stall counters** over global `sourced > processed`: filters made the global pair permanently true.
- **Outcome-based feedback in `Auto`** over runtime-tuned growth/decay atomics: the learner owns its state, the hot path sheds the loads. Learning policy gated on `benches/`; the seed plumbing is exempt and lands now.
- **An unbounded event channel** over bounded: fire-and-forget must never block a worker; band-gating already bounds the rate.
- **`Verdict` and `Act` stay dead**: the verbs hang on `Driver`, a concept the loop already owns.
- **Separate System and Task runtimes** over sharing with priority: compio has no priority and cooperation can't preempt; isolation is the only guard. The scheduler loop is System's resident task — it never contends with user applies.
- **Sources floored at one worker** over `min = 0`: the initial kick must come from somewhere — it is a build-synthesized `Promote`.
- **No preemption inside an item** — chunking is user-contract; `Halt` bounds latency to one item, `Yield` to the patience cap.

## File tree

`folder.rs` + `folder/`, never `mod.rs`. `lib.rs` carries the explicit public API only — no glob re-exports; `traits.rs` is deleted.

```
crates/bascet-core/src/
├── lib.rs
│
├── apply.rs                     # Apply, ApplyAsync
├── apply/
│   ├── execute.rs               # Work — sealed sync/async unifier (Synchronous/Asynchronous markers), Error
│   └── emit.rs                  # Emit, AsyncEmit — twins over one shared core
│
├── pipe.rs                      # Pipe<S, Tail> — the cons-list; nothing runs in it
├── owned.rs                     # Owned<T>
│
├── scheduler.rs                 # Scheduler trait
├── scheduler/
│   ├── auto.rs                  # Auto (per-layer seeds as plain fields)
│   ├── driver.rs                # the event loop (System's resident task), Driver (mechanism handle), mints, teardown
│   ├── event.rs                 # Event, Action, Receipt
│   ├── preempt.rs               # Preempt (checkpoint constants live in consts.rs)
│   ├── layer.rs                 # Layer<U, Out> (worker-shared, immutable)
│   ├── port.rs                  # Port
│   └── load.rs                  # Load (packed), Activity
│
├── runtime.rs                   # Runtime (registry, shutdown, watchdog, with_scheduler)
├── runtime/
│   ├── dispatch.rs              # re-exports
│   ├── dispatch/
│   │   ├── slot.rs              # persistent sync threads: Burn pinned at creation, Job unpinned
│   │   ├── task.rs
│   │   └── system.rs
│   ├── pool.rs                  # Pool, Record — owned by the scheduler behind Driver
│   ├── tier.rs                  # Tier
│   ├── metrics.rs               # Metrics — plain atomics on RuntimeInner
│   ├── shutdown.rs              # Shutdown
│   └── watchdog.rs              # Watchdog (per-edge progress)
│
├── worker.rs                    # Worker (split halves), State, WorkerGuard, Run<A>
├── worker/
│   ├── synchronous.rs           # sync run loop (plain fn)
│   └── asynchronous.rs          # async run loop (async fn)
│
├── pipeline.rs                  # Pipeline (assembly)
├── pipeline/
│   ├── connect.rs               # Connect, segments, zip/merge combinators
│   ├── gather.rs                # Gather — variadic upstream tuples, option-tuple take
│   └── edge.rs                  # Edge (+ parked keeper), Upstream, Downstream, Miss
│
├── runner.rs                    # Runner
│
├── sink.rs
├── sink/
│   ├── channel.rs
│   └── drain.rs
│
├── consts.rs
├── set/                         # existing, + contains; Join/Meet ops (order-preserving dedup) with Union/Intersect aliases; In/Select/Concat, Hit/Miss, And/Or/Not; type-level Subset
├── attr/                        # existing, + Attr::Id, AttrId (derived const), AttrEq, H0–HF/TEq digits (Backing/record design specced separately)
├── arena/                       # existing
└── utils/                       # pressure, patience, send, threading
```

Renames and removals carried by this refactor: `Coordinate` → `Scheduler`, `Tally` → `Load`, `Activity::Backpressure` → `Activity::Blocked`, `AtomicPressure` → `Pressure` (the non-atomic twin is deleted), `Strategy` → `Tier`, worker module files spelled `synchronous`/`asynchronous`. Deleted outright: `Executable` and the `Outcome` GAT (two apply traits over a sealed internal `Work<M>`; the `Sync`/`Async` structs survive only as its private markers, renamed `Synchronous`/`Asynchronous`), the `Execution` and `Scheduler` associated types, `Pull` (demand is credits; descriptors are ordinary items), the old `Run` trait (absorbed by `Connect`), the `Apply for Pipe` fusion impls, `Lease` (a slot is its thread), `Petitioner` and the petition thread (the scheduler loop replaces them), `AtomicPatience` and `Temper` (patience is a run local), `Metrics`' per-counter `Arc`s, `utils/channel/` (edges absorb `pressurised`; `peekable` was crossbeam-based; `monotonic` was a stub), `Contract` (folded into `Apply`), `Schedule`/`Mode`/`Parallelism` (width bounds deferred to scheduler config), `traits.rs`. One more: the old tuple-concatenating `Union` trait survives only as `Concat`, the internal fragment glue inside `Join`/`Meet`; `Union`/`Intersect` are their projection aliases, and `Attr` declares only `type Id` — `const ID` lives on `AttrId`, derived from the digits.

## Public API

```rust
pub use apply::{Apply, ApplyAsync, Error, Emit, AsyncEmit};
pub use owned::Owned;
pub use pipe::Pipe;
pub use scheduler::{Scheduler, Auto, Driver, Event, Action, Receipt, Preempt, Port, Layer, Load, Activity};
pub use worker::{Worker, State};
pub use runtime::{Runtime, Tier, Shutdown};
pub use pipeline::Pipeline;
pub use runner::{Runner, Metrics};
pub use sink::{channel, drain};
pub use set::{Set, Subset, Union, Intersect, Join, Meet};
```

## User flow

One entry point; linear chains and zipped segments are both just builders. The set at `pipeline::<_>` is the `Wants` seed — what the external consumer wants of the last layer's output.

```rust
let runtime = Runtime::builder()
    .with_burn(8)
    .with_jobs(16)
    .with_scheduler(my_scheduler)   // optional; the bare builder constructs an Auto
    .build();

let runner = runtime.pipeline::<MyWorkSet>(
    Pipeline::builder()
        .source(my_source)
        .layer(stage_one)
        .layer(stage_two)
        .sink(my_sink),
);

runner.join();
```

Fan-in:

```rust
let r1 = Pipeline::builder().source(fastq_r1).layer(parse);
let r2 = Pipeline::builder().source(fastq_r2).layer(parse);

let runner = runtime.pipeline::<MyWorkSet>(
    Pipeline::zip((r1, r2)).layer(pair).sink(write),
);

runner.join();
```
