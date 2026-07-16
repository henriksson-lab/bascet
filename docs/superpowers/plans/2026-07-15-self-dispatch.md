# Self-Dispatch Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the central scheduler thread and event plane with worker-owned scheduling per `docs/superpowers/specs/2026-07-13-self-dispatch.md`: batched edges, the participation loop, the `Schedule`/`Scheduler`/`Layer` state, per-layer `Preempt`, and `Waker`-based idling.

**Architecture:** Bottom-up: batch the data plane first (Emit, Upstream, Zip), build the schedule state machine with unit tests, rewrite `Run::drive`, then cut over the runtime wiring and delete the event plane. The crate compiles as a whole again at Task 7 (the cutover); Tasks 1–6 are gated by their own module tests.

**Tech Stack:** Rust, kanal (channels), std only otherwise (`std::task::Wake`, `thread::park`). No new dependencies.

**Design authority:** `docs/superpowers/specs/2026-07-13-self-dispatch.md` (final revision 2026-07-15). Where this plan and the spec disagree, the spec wins.

## Global Constraints

- **The implementer never runs cargo.** Every "Checkpoint" step is run by the project owner, who compiles and tests when they decide. Tasks end at a checkpoint, never at a green-run claim.
- **No git commands at all.** No add, no commit, no status. Tasks end at review checkpoints. (This overrides the plan template's commit steps.)
- No worktrees; implement in place on `dev/2.0`.
- Modules: `folder.rs` + `folder/`, never `mod.rs`. New imports use full `crate::` paths.
- Channel halves are named `*_tx` / `*_rx`. kanal is the only channel crate.
- Zero comments except existing `SAFETY:`/`TODO:` markers, which are preserved verbatim.
- No abbreviations in names; no `#[repr]` additions; no new tuning constants (`DEPTH`, `YIELD_*`, `WATERMARK` already exist and are the complete set — `PATIENCE_*` and `PRESSURE_*` are deleted with their consumers).
- Protocol vocabulary is never deleted for being unexercised: `Tier`, `State`, `Action`, `Preempt` enums stay even where nothing constructs them.
- **Not in scope:** `Ordered<V>` (reserved in the spec — leave the `Emit` flush path as the single seam it would specialize; do not implement), the Task tier, credits, the watchdog, width caps.
- kanal facts this plan relies on: `Receiver::try_recv() -> Result<Option<T>, ReceiveError>` with `Ok(None)` = empty; `Sender::try_send_option(&mut Option<T>) -> Result<bool, SendError>` keeps the value in the `Option` when it returns `Ok(false)`; `Sender::try_send` **drops** the value on `Ok(false)` and must not be used for flush.

## File Structure

```
crates/bascet-core/src/
├── apply.rs                    # MODIFY: Apply gains defaulted finish()
├── apply/emit.rs               # MODIFY: batch buffer + staged remainder + flush
├── apply/execute.rs            # MODIFY: Work::mint replaces Work::launch
├── pipeline/edge.rs            # MODIFY: channels carry Vec<T>; Upstream pending; Zip staging
├── pipeline/gather.rs          # MODIFY: kanal-convention trait; batch refill; zip rows
├── pipeline/connect.rs         # MODIFY: Build produces Layer entries + upstream topology
├── schedule.rs                 # CREATE: Schedule, Scheduler, pick/post/wake/retire, participate, Unpark
├── schedule/layer.rs           # CREATE: Layer entry, Assignment trait, Probe
├── schedule/preempt.rs         # CREATE (move from scheduler/preempt.rs): Preempt
├── worker.rs                   # MODIFY: Worker struct deleted; State enum stays
├── worker/synchronous.rs       # MODIFY: Run rewritten around drive/visit
├── runtime.rs                  # MODIFY: Runtime loses <S>; pipeline() builds Schedule
├── runtime/dispatch.rs         # MODIFY: broadcast() to send one participation job per thread
├── runner.rs                   # MODIFY: join via waiter slot
├── consts.rs                   # MODIFY: PATIENCE_*/PRESSURE_* deleted
├── lib.rs                      # MODIFY: exports
├── scheduler.rs + scheduler/   # DELETE (driver, event, port, load, stride, layer; preempt moves)
└── runtime/pool.rs             # DELETE
crates/bascet-core/tests/
├── pipeline.rs                 # MODIFY: adapt + canaries
└── dispatch.rs                 # MODIFY: adapt
```

---

### Task 1: Batched Emit

**Files:**
- Modify: `crates/bascet-core/src/apply/emit.rs`
- Modify: `crates/bascet-core/src/pipeline/edge.rs` (channel payload only, minimal)

**Interfaces:**
- Consumes: `Downstream<T>` whose `output_tx` becomes `Arc<kanal::Sender<Vec<T>>>` (Task 1 changes the field type; Task 2 finishes the edge).
- Produces: `Emit::push(&mut self, item)`, `Emit::flush(&mut self) -> bool` (true = nothing held back), `Emit::full(&self) -> bool` (a refused batch is staged), `Emit::finished(&self)`, `Emit::orphaned(&self)` — used by Task 5's drive loop.

- [ ] **Step 1: Change the edge channel payload to batches**

In `pipeline/edge.rs`, change every channel type from `T` to `Vec<T>`: `Inner.output_tx: Weak<Sender<Vec<T>>>`, `Inner.input_rx: Weak<Receiver<Vec<T>>>`, `Upstream.input_rx: Arc<Receiver<Vec<T>>>`, `Downstream.output_tx: Arc<Sender<Vec<T>>>`, and `Edge::new`'s `kanal::bounded::<Vec<T>>(depth)`. Leave `Upstream::try_recv`/tests broken for now — Task 2 rewrites them.

- [ ] **Step 2: Write the failing tests**

Replace the test module in `apply/emit.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::edge::{Edge, Upstream};

    fn emit(depth: usize) -> (Emit<u32, ()>, Upstream<u32>) {
        let (up, down) = Edge::<u32>::new(depth);
        (Emit::new(Some(down)), up)
    }

    #[test]
    fn push_buffers_and_flush_sends_one_batch() {
        let (mut out, up) = emit(4);
        out.push(7);
        out.push(9);
        assert!(up.input_rx.is_empty());
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![7, 9]));
    }

    #[test]
    fn refused_flush_stages_the_batch_without_loss() {
        let (mut out, up) = emit(1);
        out.push(1);
        assert!(out.flush());
        out.push(2);
        assert!(!out.flush());
        assert!(out.full());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![1]));
        assert!(out.flush());
        assert!(!out.full());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![2]));
    }

    #[test]
    fn staged_batch_flushes_before_new_pushes() {
        let (mut out, up) = emit(1);
        out.push(1);
        out.flush();
        out.push(2);
        out.flush();
        out.push(3);
        up.input_rx.try_recv().unwrap();
        assert!(out.flush());
        assert_eq!(up.input_rx.try_recv().unwrap(), Some(vec![2, 3]));
    }

    #[test]
    fn orphaned_when_consumer_gone() {
        let (mut out, up) = emit(4);
        drop(up);
        out.push(1);
        out.flush();
        assert!(out.orphaned());
    }

    #[test]
    fn sink_drops_silently() {
        let mut out = Emit::<u32, ()>::new(None);
        out.push(1);
        assert!(out.flush());
        assert!(!out.orphaned());
    }
}
```

Note `Edge::new(depth)` loses its port arguments — Task 7 deletes `Port`; to keep this task self-contained, change the signature now: `pub(crate) fn new(depth: usize) -> (Upstream<T>, Downstream<T>)` and delete the `producer`/`consumer` fields and accessors from `Inner` (their only consumers are the promote paths, which die in Task 2).

Adjust `staged_batch_flushes_before_new_pushes` expectation: after the second refused flush, pushes continue into the buffer; a later flush first sends the staged `vec![2]`, then `vec![3]`. The test above asserts the combined observable order — if the implementation sends them as two batches, assert two receives (`Some(vec![2])`, then flush again and `Some(vec![3])`); it must not merge staged with buffer.

- [ ] **Step 3: Implement**

```rust
pub struct Emit<Out, Wants: Set> {
    downstream: Option<Downstream<Out>>,
    buffer: Vec<Out>,
    staged: Option<Vec<Out>>,
    finished: bool,
    _wants: PhantomData<Wants>,
}

impl<Out, Wants: Set> Emit<Out, Wants> {
    pub(crate) fn new(downstream: Option<Downstream<Out>>) -> Self {
        Self {
            downstream,
            buffer: Vec::new(),
            staged: None,
            finished: false,
            _wants: PhantomData,
        }
    }

    pub fn push(&mut self, item: Out) {
        if self.downstream.is_some() {
            self.buffer.push(item);
        }
    }

    pub(crate) fn flush(&mut self) -> bool {
        let Some(downstream) = &mut self.downstream else {
            self.buffer.clear();
            return true;
        };
        if self.staged.is_some() {
            match downstream.output_tx.try_send_option(&mut self.staged) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(_) => {
                    downstream.exhausted = true;
                    self.staged = None;
                    self.buffer.clear();
                    return true;
                }
            }
        }
        if self.buffer.is_empty() {
            return true;
        }
        self.staged = Some(std::mem::take(&mut self.buffer));
        match downstream.output_tx.try_send_option(&mut self.staged) {
            Ok(true) => true,
            Ok(false) => false,
            Err(_) => {
                downstream.exhausted = true;
                self.staged = None;
                true
            }
        }
    }

    pub(crate) fn full(&self) -> bool {
        self.staged.is_some()
    }

    pub(crate) fn residue(&self) -> bool {
        self.staged.is_some() || !self.buffer.is_empty()
    }

    pub fn wants<A: Attr>(&self) -> bool {
        Wants::contains::<A>()
    }

    pub fn finish(&mut self) {
        self.finished = true;
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished || self.orphaned()
    }

    pub(crate) fn orphaned(&self) -> bool {
        self.downstream
            .as_ref()
            .is_some_and(|downstream| downstream.exhausted)
    }
}
```

Delete `push_async`, `promote`, and the `hit()` call — the event plane dies in Task 7 and `push` no longer touches the channel. If `Downstream::promote`/`hit` now have no callers in this file, leave them for Task 2's edge rewrite.

- [ ] **Step 4: Checkpoint (run by the project owner)**

`cargo test -p bascet-core emit` — expected: the five tests above pass. The rest of the crate does not compile yet; that is expected until Task 7 (test with `--no-run` module filtering is not possible mid-refactor, so this checkpoint may be deferred and batched with Task 2's).

---

### Task 2: Batched Upstream and the Gather convention flip

**Files:**
- Modify: `crates/bascet-core/src/pipeline/edge.rs`
- Modify: `crates/bascet-core/src/pipeline/gather.rs`

**Interfaces:**
- Produces: `pub(crate) struct Closed;` and the trait every consumer of input uses from here on:

```rust
pub(crate) trait Gather: Clone + Send + 'static {
    type Item;
    fn try_recv(&mut self) -> Result<Option<Self::Item>, Closed>;
    fn starved(&self) -> bool;
    fn exhausted(&self) -> bool;
    fn residue(&self) -> bool;
}
```

**The convention flips here, loudly:** `Ok(Some(item))` = item, `Ok(None)` = starved (kanal's own meaning), `Err(Closed)` = end of stream. The old trait meant `Ok(None)` = EOF. Every match arm written in later tasks assumes the new meaning; a silent mistranslation swaps starvation for termination.

- [ ] **Step 1: Write the failing tests**

Replace the test modules in `edge.rs` and `gather.rs`:

```rust
#[test]
fn pending_serves_in_order_then_starves() {
    let (mut up, down) = Edge::<u32>::new(4);
    down.output_tx.send(vec![1, 2]).unwrap();
    assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
    assert!(matches!(Gather::try_recv(&mut up), Ok(Some(2))));
    assert!(matches!(Gather::try_recv(&mut up), Ok(None)));
    assert!(up.starved());
    assert!(!up.residue());
}

#[test]
fn closed_only_after_pending_drains() {
    let (mut up, down) = Edge::<u32>::new(4);
    down.output_tx.send(vec![1]).unwrap();
    drop(down);
    assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
    assert!(matches!(Gather::try_recv(&mut up), Err(Closed)));
    assert!(up.exhausted());
}

#[test]
fn residue_reports_undrained_pending() {
    let (mut up, down) = Edge::<u32>::new(4);
    down.output_tx.send(vec![1, 2, 3]).unwrap();
    assert!(matches!(Gather::try_recv(&mut up), Ok(Some(1))));
    assert!(up.residue());
    drop(down);
}

#[test]
fn source_gather_never_starves() {
    let mut unit = ();
    assert!(matches!(Gather::try_recv(&mut unit), Ok(Some(()))));
    assert!(!Gather::starved(&()));
    assert!(!Gather::exhausted(&()));
    assert!(!Gather::residue(&()));
}
```

- [ ] **Step 2: Implement `Upstream` with the pending queue**

In `edge.rs`:

```rust
pub(crate) struct Upstream<T> {
    pub(crate) input_rx: Arc<Receiver<Vec<T>>>,
    pub(crate) pending: VecDeque<T>,
    pub(crate) exhausted: bool,
    pub(crate) edge: Edge<T>,
}

impl<T> Clone for Upstream<T> {
    fn clone(&self) -> Self {
        Self {
            input_rx: Arc::clone(&self.input_rx),
            pending: VecDeque::new(),
            exhausted: false,
            edge: self.edge.clone(),
        }
    }
}
```

Fresh pending on clone is the approved per-view staging semantics: clones are minted from the pristine wired gather only. Delete `Upstream::promote`, `Downstream::promote`, `Downstream::hit`, and the `Port`/`events_tx` plumbing from this file (`Edge::new(depth)` from Task 1). Keep `done()` as `self.exhausted || self.input_rx.sender_count() == 0`.

In `gather.rs`, the linear impl:

```rust
impl<T: Send + 'static> Gather for Upstream<T> {
    type Item = T;

    fn try_recv(&mut self) -> Result<Option<T>, Closed> {
        if let Some(item) = self.pending.pop_front() {
            return Ok(Some(item));
        }
        if self.exhausted {
            return Err(Closed);
        }
        match self.input_rx.try_recv() {
            Ok(Some(batch)) => {
                self.pending = VecDeque::from(batch);
                Ok(self.pending.pop_front())
            }
            Ok(None) => Ok(None),
            Err(_) => {
                self.exhausted = true;
                Err(Closed)
            }
        }
    }

    fn starved(&self) -> bool {
        self.pending.is_empty() && self.input_rx.is_empty() && !self.done()
    }

    fn exhausted(&self) -> bool {
        self.pending.is_empty() && self.done()
    }

    fn residue(&self) -> bool {
        !self.pending.is_empty()
    }
}
```

`impl Gather for ()`: `try_recv` = `Ok(Some(()))`, `starved` = `false`, `exhausted` = `false`, `residue` = `false`. Keep the 1-tuple passthrough impl, forwarding all four methods. Delete `recv_timeout` and `close` from the trait and all impls; delete the `Starved` struct (replaced by `Ok(None)`); `Closed` moves to `gather.rs` and is re-exported where edge.rs needs it.

- [ ] **Step 3: Checkpoint (run by the project owner)**

`cargo test -p bascet-core edge gather` once Task 3 lands (zip impls share these files). Expected: the tests above pass; kanal canary `tests/kanal.rs` still passes.

---

### Task 3: Zip row batches

**Files:**
- Modify: `crates/bascet-core/src/pipeline/edge.rs` (the `Zip` struct + variadic `From`)
- Modify: `crates/bascet-core/src/pipeline/gather.rs` (the variadic `Gather for Zip`)

**Interfaces:**
- Produces: `Zip` keeps its shared `Arc<Mutex<...>>` staging (members + row), gains a per-clone `pending: VecDeque<Row>`; `Gather::try_recv` yields one row `(Option<A0>, ..)` at a time from the local pending, refilled by assembling the longest aligned prefix under one lock.

- [ ] **Step 1: Adapt the struct**

```rust
pub(crate) struct Zip<T, R> {
    pub(crate) inner: Arc<Mutex<T>>,
    pub(crate) pending: VecDeque<R>,
}

impl<T, R> Clone for Zip<T, R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            pending: VecDeque::new(),
        }
    }
}
```

The variadic `From` impl parameterizes `Zip<((Upstream<A~#>,..),(Option<A~#>,..)), (Option<A~#>,..)>` — the second parameter is the row type. Update the `variadic!` block accordingly (same `N = 2..=16` header, no filters).

- [ ] **Step 2: Write the failing tests** (adapt the two existing zip tests)

```rust
#[test]
fn uneven_batches_pair_in_order() {
    let (mut up_a, down_a) = Edge::<u32>::new(4);
    let (up_b, down_b) = Edge::<u32>::new(4);
    down_a.output_tx.send(vec![1]).unwrap();
    down_a.output_tx.send(vec![2, 3]).unwrap();
    down_b.output_tx.send(vec![10, 20, 30]).unwrap();
    let mut gather = Zip::from((up_a, up_b));
    assert!(matches!(gather.try_recv(), Ok(Some((Some(1), Some(10))))));
    assert!(matches!(gather.try_recv(), Ok(Some((Some(2), Some(20))))));
    assert!(matches!(gather.try_recv(), Ok(Some((Some(3), Some(30))))));
    assert!(matches!(gather.try_recv(), Ok(None)));
    drop(down_a);
    drop(down_b);
}

#[test]
fn survivor_drains_with_none_slots() {
    let (up_a, down_a) = Edge::<u32>::new(4);
    let (up_b, down_b) = Edge::<u32>::new(4);
    down_a.output_tx.send(vec![1]).unwrap();
    down_b.output_tx.send(vec![10, 20]).unwrap();
    drop(down_a);
    drop(down_b);
    let mut gather = Zip::from((up_a, up_b));
    assert!(matches!(gather.try_recv(), Ok(Some((Some(1), Some(10))))));
    assert!(matches!(gather.try_recv(), Ok(Some((None, Some(20))))));
    assert!(matches!(gather.try_recv(), Err(Closed)));
}

#[test]
fn clones_share_staging_but_not_pending() {
    let (up_a, down_a) = Edge::<u32>::new(4);
    let (up_b, down_b) = Edge::<u32>::new(4);
    down_a.output_tx.send(vec![1]).unwrap();
    let mut first = Zip::from((up_a, up_b));
    let mut second = first.clone();
    assert!(matches!(first.try_recv(), Ok(None)));
    down_b.output_tx.send(vec![10]).unwrap();
    assert!(matches!(second.try_recv(), Ok(Some((Some(1), Some(10))))));
    drop(down_a);
    drop(down_b);
    assert!(matches!(first.try_recv(), Err(Closed)));
}
```

- [ ] **Step 3: Implement the variadic `Gather for Zip`**

Inside the `variadic!` block, the shape (`#` expands per member):

```rust
fn try_recv(&mut self) -> Result<Option<Self::Item>, Closed> {
    if let Some(row) = self.pending.pop_front() {
        return Ok(Some(row));
    }
    let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
    let (members, row) = &mut *guard;
    loop {
        let mut starved = false;
        @N[if row.#.is_none() && !members.#.exhausted() {
            match Gather::try_recv(&mut members.#) {
                Ok(Some(item)) => row.# = Some(item),
                Ok(None) => starved = true,
                Err(Closed) => {}
            }
        }]
        if starved {
            break;
        }
        let taken = (@N[row.#.take()](sep=","),);
        if @N[taken.#.is_none()](sep=" && ") {
            return if self.pending.is_empty() { Err(Closed) } else { Ok(self.pending.pop_front()) };
        }
        self.pending.push_back(taken);
        if self.pending.len() >= 64 {
            break;
        }
    }
    if let Some(row) = self.pending.pop_front() {
        return Ok(Some(row));
    }
    Err(Closed) check falls out above; otherwise:
    Ok(None)
}
```

Written out precisely (the sketch above shows intent — implement it as: refill loop assembles rows into `self.pending` until a member starves, all members are drained-and-closed, or 64 rows are staged; then pop). The all-`None` row means every member is closed and drained: return `Err(Closed)` when pending is also empty, else serve pending first. `starved()` = pending empty and any member starved; `exhausted()` = pending empty and all members exhausted and all row slots `None`; `residue()` = `!self.pending.is_empty()`. The `64` is not a new constant: use `crate::consts::DEPTH` as the staging bound — it is an allocation bound in exactly the spec's sense.

- [ ] **Step 4: Checkpoint (run by the project owner)**

`cargo test -p bascet-core edge gather` — expected: Tasks 2 and 3 tests all pass.

---

### Task 4: The Schedule core

**Files:**
- Create: `crates/bascet-core/src/schedule.rs`
- Create: `crates/bascet-core/src/schedule/layer.rs`
- Create: `crates/bascet-core/src/schedule/preempt.rs` (move `scheduler/preempt.rs` verbatim)
- Modify: `crates/bascet-core/src/lib.rs` (add `pub(crate) mod schedule;` — old `scheduler` module untouched until Task 7)

**Interfaces:**
- Produces (consumed by Tasks 5–7):

```rust
// schedule/layer.rs
pub(crate) struct Probe { pub input: bool, pub output: bool, pub exhausted: bool }

pub(crate) trait Assignment: Send {
    fn drive(&mut self, schedule: &Schedule, tier: Tier);
    fn layer(&self) -> usize;
    fn finished(&self) -> bool;
    fn residue(&self) -> bool;
}

pub(crate) type Mint = Arc<Mutex<Box<dyn FnMut() -> Box<dyn Assignment> + Send>>>;

pub(crate) struct Layer {
    pub(crate) mint: Mint,
    pub(crate) probe: Box<dyn Fn() -> Probe + Send>,
    pub(crate) blocked: VecDeque<Box<dyn Assignment>>,
    pub(crate) parked: VecDeque<Box<dyn Assignment>>,
    pub(crate) workers: usize,
    pub(crate) pass: u64,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) mode: Mode,
}

pub(crate) enum Mode { Synchronous, Asynchronous }

// schedule.rs
pub(crate) struct Schedule { pub(crate) scheduler: Mutex<Scheduler> }
pub(crate) struct Scheduler {
    pub(crate) layers: Box<[Option<Layer>]>,
    pub(crate) upstream: Box<[Box<[usize]>]>,
    pub(crate) idle: Vec<Waker>,
    pub(crate) waiter: Option<Waker>,
}
impl Scheduler {
    pub(crate) fn runnable(&self, index: usize, tier: Tier) -> bool;
    pub(crate) fn pick(&self, tier: Tier, previous: Option<usize>) -> Option<usize>;
    pub(crate) fn post(&self, dry: usize);
    pub(crate) fn wake(&mut self);
    pub(crate) fn retire(&mut self, index: usize);
    pub(crate) fn finished(&self) -> bool;   // all layers None
}
```

`Mint` is `Arc<Mutex<..>>` because apply templates are `Clone + Send` but not `Sync`; the participation loop clones the `Arc` under the schedule lock and runs the user `Clone` after unlocking, holding only the mint's own mutex.

- [ ] **Step 1: Write the failing unit tests** (in `schedule.rs`, `#[cfg(test)]`)

Build stub layers from plain closures and flags:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Stub;
    impl Assignment for Stub {
        fn drive(&mut self, _: &Schedule, _: Tier) {}
        fn layer(&self) -> usize { 0 }
        fn finished(&self) -> bool { false }
        fn residue(&self) -> bool { false }
    }

    fn layer(input: Arc<AtomicBool>, output: Arc<AtomicBool>, pass: u64) -> Layer {
        Layer {
            mint: Arc::new(Mutex::new(Box::new(|| Box::new(Stub) as Box<dyn Assignment>))),
            probe: Box::new(move || Probe {
                input: input.load(Ordering::Relaxed),
                output: output.load(Ordering::Relaxed),
                exhausted: false,
            }),
            blocked: VecDeque::new(),
            parked: VecDeque::new(),
            workers: 0,
            pass,
            preempt: Arc::new(AtomicU8::new(Preempt::Continue as u8)),
            mode: Mode::Synchronous,
        }
    }

    fn flags() -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        (Arc::new(AtomicBool::new(true)), Arc::new(AtomicBool::new(true)))
    }

    fn scheduler(layers: Vec<Option<Layer>>, upstream: Vec<Vec<usize>>) -> Scheduler {
        Scheduler {
            layers: layers.into_boxed_slice(),
            upstream: upstream.into_iter().map(Vec::into_boxed_slice).collect(),
            idle: Vec::new(),
            waiter: None,
        }
    }

    #[test]
    fn pick_takes_minimum_pass_among_runnable() {
        let (ia, oa) = flags();
        let (ib, ob) = flags();
        let scheduler = scheduler(
            vec![Some(layer(ia, oa, 5)), Some(layer(ib, ob, 2))],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(Tier::Job, None), Some(1));
    }

    #[test]
    fn probe_gates_the_pick() {
        let (ia, oa) = flags();
        let (ib, ob) = flags();
        ib.store(false, Ordering::Relaxed);
        let scheduler = scheduler(
            vec![Some(layer(ia, oa, 5)), Some(layer(ib, ob, 2))],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(Tier::Job, None), Some(0));
    }

    #[test]
    fn blocked_queue_counts_as_input() {
        let (ia, oa) = flags();
        ia.store(false, Ordering::Relaxed);
        let mut entry = layer(ia, oa, 0);
        entry.blocked.push_back(Box::new(Stub));
        let scheduler = scheduler(vec![Some(entry)], vec![vec![]]);
        assert_eq!(scheduler.pick(Tier::Job, None), Some(0));
    }

    #[test]
    fn ties_break_to_previous_then_downstream() {
        let (ia, oa) = flags();
        let (ib, ob) = flags();
        let scheduler = scheduler(
            vec![Some(layer(ia, oa, 3)), Some(layer(ib, ob, 3))],
            vec![vec![1], vec![]],
        );
        assert_eq!(scheduler.pick(Tier::Job, Some(1)), Some(1));
        assert_eq!(scheduler.pick(Tier::Job, None), Some(0));
    }

    #[test]
    fn post_walks_past_unmanned_to_nearest_manned() {
        let (ia, oa) = flags();
        let (ib, ob) = flags();
        let (ic, oc) = flags();
        let mut source = layer(ic, oc, 0);
        source.workers = 1;
        let scheduler = scheduler(
            vec![Some(layer(ia, oa, 0)), Some(layer(ib, ob, 0)), Some(source)],
            vec![vec![1], vec![2], vec![]],
        );
        scheduler.post(0);
        let posted = scheduler.layers[2].as_ref().unwrap().preempt.load(Ordering::Relaxed);
        assert_eq!(posted, Preempt::Halt as u8);
    }

    #[test]
    fn post_falls_back_to_highest_pass_manned() {
        let (ia, oa) = flags();
        let (ib, ob) = flags();
        let mut other = layer(ib, ob, 9);
        other.workers = 1;
        let scheduler = scheduler(
            vec![Some(layer(ia, oa, 0)), Some(other)],
            vec![vec![], vec![]],
        );
        scheduler.post(0);
        let posted = scheduler.layers[1].as_ref().unwrap().preempt.load(Ordering::Relaxed);
        assert_eq!(posted, Preempt::Halt as u8);
    }

    #[test]
    fn retire_wakes_the_waiter() {
        let woken = Arc::new(AtomicBool::new(false));
        struct Flag(Arc<AtomicBool>);
        impl std::task::Wake for Flag {
            fn wake(self: Arc<Self>) { self.0.store(true, Ordering::Relaxed); }
        }
        let (ia, oa) = flags();
        let mut scheduler = scheduler(vec![Some(layer(ia, oa, 0))], vec![vec![]]);
        scheduler.waiter = Some(Waker::from(Arc::new(Flag(Arc::clone(&woken)))));
        scheduler.retire(0);
        assert!(scheduler.layers[0].is_none());
        assert!(scheduler.finished());
        assert!(woken.load(Ordering::Relaxed));
    }

    #[test]
    fn wake_pops_one_idler_per_unmanned_runnable_layer() {
        let woken = Arc::new(AtomicBool::new(false));
        struct Flag(Arc<AtomicBool>);
        impl std::task::Wake for Flag {
            fn wake(self: Arc<Self>) { self.0.store(true, Ordering::Relaxed); }
        }
        let (ia, oa) = flags();
        let mut scheduler = scheduler(vec![Some(layer(ia, oa, 0))], vec![vec![]]);
        scheduler.idle.push(Waker::from(Arc::new(Flag(Arc::clone(&woken)))));
        scheduler.wake();
        assert!(woken.load(Ordering::Relaxed));
        assert!(scheduler.idle.is_empty());
    }
}
```

- [ ] **Step 2: Implement**

```rust
impl Scheduler {
    pub(crate) fn runnable(&self, index: usize, tier: Tier) -> bool {
        let Some(layer) = self.layers[index].as_ref() else { return false };
        if !layer.mode.compatible(tier) {
            return false;
        }
        let probe = (layer.probe)();
        if !probe.output {
            return false;
        }
        if probe.input || !layer.blocked.is_empty() {
            return true;
        }
        probe.exhausted
            && (!layer.parked.is_empty() || !layer.blocked.is_empty() || layer.workers == 0)
    }

    pub(crate) fn pick(&self, tier: Tier, previous: Option<usize>) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for (index, entry) in self.layers.iter().enumerate() {
            let Some(layer) = entry else { continue };
            if !self.runnable(index, tier) {
                continue;
            }
            let replace = match best {
                None => true,
                Some((_, pass)) => {
                    layer.pass < pass || (layer.pass == pass && Some(index) == previous)
                }
            };
            if replace {
                best = Some((index, layer.pass));
            }
        }
        best.map(|(index, _)| index)
    }

    pub(crate) fn post(&self, dry: usize) {
        let mut at = dry;
        let victim = loop {
            let Some(&up) = self.upstream[at].first() else { break self.busiest() };
            if self.layers[up].as_ref().is_some_and(|layer| layer.workers > 0) {
                break Some(up);
            }
            at = up;
        };
        if let Some(index) = victim
            && let Some(layer) = self.layers[index].as_ref()
        {
            layer.preempt.store(Preempt::Halt as u8, Ordering::Relaxed);
        }
    }

    fn busiest(&self) -> Option<usize> {
        self.layers
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.as_ref().map(|layer| (index, layer)))
            .filter(|(_, layer)| layer.workers > 0)
            .max_by_key(|(_, layer)| layer.pass)
            .map(|(index, _)| index)
    }

    pub(crate) fn wake(&mut self) {
        for index in 0..self.layers.len() {
            let unmanned = self.layers[index]
                .as_ref()
                .is_some_and(|layer| layer.workers == 0);
            if unmanned && self.runnable(index, Tier::Job) {
                match self.idle.pop() {
                    Some(waker) => waker.wake(),
                    None => self.post(index),
                }
            }
        }
    }

    pub(crate) fn retire(&mut self, index: usize) {
        if self.layers[index].take().is_some()
            && let Some(waker) = self.waiter.take()
        {
            waker.wake();
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.layers.iter().all(Option::is_none)
    }
}
```

`Mode::compatible(tier)`: `Synchronous` with `Tier::Burn | Tier::Job`, `Asynchronous` with `Tier::Task`. Zip's `post` walk uses `upstream[at].first()` — multi-member walks refine later if a bench demands it; the topology slice records all members.

Wait for the no-idlers `wake` arm: `post(index)` walks *that layer's* dry chain — matching the spec: the donor serves the unmanned layer's demand.

- [ ] **Step 3: Checkpoint (run by the project owner)**

`cargo test -p bascet-core schedule` — expected: all eight tests pass.

---

### Task 5: `Apply::finish`, `Work::mint`, and the drive loop

**Files:**
- Modify: `crates/bascet-core/src/apply.rs`
- Modify: `crates/bascet-core/src/apply/execute.rs`
- Modify: `crates/bascet-core/src/worker.rs` (delete the `Worker` struct, keep `State`)
- Modify: `crates/bascet-core/src/worker/synchronous.rs`

**Interfaces:**
- Consumes: `Gather` (Task 2/3), `Emit` (Task 1), `Schedule`/`Assignment`/`Probe` (Task 4).
- Produces: `Work<M>::mint(self, gather, downstream, layer, preempt, runtime) -> Box<dyn Assignment>` — the only constructor Task 7's wiring calls.

- [ ] **Step 1: Add the defaulted finalize hook to `Apply` (and `ApplyAsync`)**

```rust
fn finish<W: Set>(&mut self, out: &mut Emit<Self::Output, W>) -> Result<(), Error> {
    let _ = out;
    Ok(())
}
```

- [ ] **Step 2: Rewrite `Run` and `drive` in `worker/synchronous.rs`**

```rust
pub(crate) struct Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    pub(crate) apply: A,
    pub(crate) gather: U,
    pub(crate) emit: Emit<A::Output, W>,
    pub(crate) layer: usize,
    pub(crate) preempt: Arc<AtomicU8>,
    pub(crate) runtime: Weak<RuntimeInner>,
    pub(crate) budget: Patience<u32>,
    pub(crate) round: u32,
    pub(crate) finished: bool,
}

impl<A, U, W> Assignment for Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    fn drive(&mut self, schedule: &Schedule, tier: Tier) {
        loop {
            if self.preempt.load(Ordering::Relaxed) == Preempt::Halt as u8
                && self
                    .preempt
                    .compare_exchange(
                        Preempt::Halt as u8,
                        Preempt::Continue as u8,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                self.budget.miss();
                self.emit.flush();
                if !self.visit(schedule, tier, false, true) {
                    return;
                }
            }
            match self.gather.try_recv() {
                Ok(Some(item)) => {
                    if let Err(error) = self.apply.apply(item, &mut self.emit) {
                        self.emit.flush();
                        self.fail(schedule, error);
                        return;
                    }
                    if self.emit.finished() {
                        self.conclude();
                        return;
                    }
                    self.round += 1;
                    if self.round >= self.budget.patience() && !self.gather.residue() {
                        self.round = 0;
                        self.budget.hit();
                        self.emit.flush();
                        if !self.visit(schedule, tier, true, false) {
                            return;
                        }
                    }
                }
                Ok(None) => {
                    self.emit.flush();
                    if !self.visit(schedule, tier, false, false) {
                        return;
                    }
                }
                Err(Closed) => {
                    self.conclude();
                    return;
                }
            }
        }
    }

    fn layer(&self) -> usize {
        self.layer
    }

    fn finished(&self) -> bool {
        self.finished
    }

    fn residue(&self) -> bool {
        self.emit.residue() || self.gather.residue()
    }
}

impl<A, U, W> Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    fn visit(&mut self, schedule: &Schedule, tier: Tier, credit: bool, claim: bool) -> bool {
        let mut scheduler = schedule.scheduler.lock().unwrap();
        let Some(layer) = scheduler.layers[self.layer].as_mut() else {
            return false;
        };
        if credit {
            layer.pass += 1;
        }
        layer.preempt.store(Preempt::Continue as u8, Ordering::Relaxed);
        scheduler.wake();
        scheduler.runnable(self.layer, tier)
            && ((tier == Tier::Burn && !claim)
                || scheduler.pick(tier, Some(self.layer)) == Some(self.layer))
    }

    fn conclude(&mut self) {
        if !self.finished {
            self.finished = true;
            if let Err(error) = self.apply.finish(&mut self.emit) {
                if let Some(runtime) = self.runtime.upgrade() {
                    runtime.record_error(error);
                }
            }
        }
        if !self.emit.flush() && self.emit.orphaned() {
            tracing::warn!(layer = self.layer, "finalize output discarded: consumer gone");
        }
    }

    fn fail(&mut self, schedule: &Schedule, error: Error) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.record_error(error);
        }
        self.finished = true;
        let mut scheduler = schedule.scheduler.lock().unwrap();
        scheduler.retire(self.layer);
    }
}
```

Borrow note: `visit` re-borrows `scheduler` after the `layer` mutation before calling `wake`/`runnable`/`pick` — structure the body so the `as_mut()` borrow ends first (do the credit and preempt store in a short block).

- [ ] **Step 3: Replace `Work::launch` with `Work::mint` in `apply/execute.rs`**

```rust
pub trait Work<M>: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    fn mint<U, W>(
        &self,
        gather: &U,
        downstream: &Option<Downstream<Self::Output>>,
        layer: usize,
        preempt: &Arc<AtomicU8>,
        runtime: &Weak<RuntimeInner>,
        wants: PhantomData<W>,
    ) -> Box<dyn Assignment>
    where
        U: Gather<Item = Self::Input>,
        W: Set;
}

impl<A: Apply> Work<Synchronous> for A {
    fn mint<U, W>(&self, gather: &U, downstream: &Option<Downstream<A::Output>>, layer: usize, preempt: &Arc<AtomicU8>, runtime: &Weak<RuntimeInner>, _wants: PhantomData<W>) -> Box<dyn Assignment>
    where U: Gather<Item = A::Input>, W: Set,
    {
        Box::new(Run {
            apply: self.clone(),
            gather: gather.clone(),
            emit: Emit::new(downstream.clone()),
            layer,
            preempt: Arc::clone(preempt),
            runtime: runtime.clone(),
            budget: Patience::new(YIELD_START, YIELD_START, YIELD_START)
                .set_min(YIELD_MIN)
                .set_max(YIELD_CAP),
            round: 0,
            finished: false,
        })
    }
}
```

`Work<Asynchronous>::mint` stays `unimplemented!("async worker execution is deferred with the compio tiers")`. Delete the old `scheduler/layer.rs` carrier usage; `PATIENCE_*` construction disappears with the patience field. In `worker.rs`, delete the `Worker` struct, `WorkerGuard` (panic recording moves to the participation loop), `set_activity`, `finish`, and the `activity` helper; keep `pub enum State { .. }` verbatim as protocol vocabulary.

- [ ] **Step 4: Checkpoint (run by the project owner)**

Nothing new is independently runnable here (drive is exercised end-to-end in Task 8); the checkpoint is `cargo check -p bascet-core` deferred until after Task 7's cutover.

---

### Task 6: The participation loop, `Unpark`, and join

**Files:**
- Modify: `crates/bascet-core/src/schedule.rs`

**Interfaces:**
- Produces: `pub(crate) fn participate(schedule: &Arc<Schedule>, runtime: &Weak<RuntimeInner>, tier: Tier)` — the body of the one job Task 7 sends per pool thread; `pub(crate) struct Unpark(pub(crate) Thread)` implementing `std::task::Wake`; `Schedule::join_wait(&self, sink: usize)` used by Task 7's `Runner::join`.

- [ ] **Step 1: Implement**

```rust
pub(crate) struct Unpark(pub(crate) Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub(crate) fn participate(schedule: &Arc<Schedule>, runtime: &Weak<RuntimeInner>, tier: Tier) {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut current: Option<Box<dyn Assignment>> = None;
    let mut previous: Option<usize> = None;
    let mut slept = false;
    loop {
        let mut scheduler = schedule.scheduler.lock().unwrap();
        if slept {
            slept = false;
            scheduler.idle.retain(|idler| !idler.will_wake(&waker));
        }
        let mut starved: Option<usize> = None;
        if let Some(assignment) = current.take() {
            let index = assignment.layer();
            match scheduler.layers[index].as_mut() {
                None => drop(assignment),
                Some(layer) => {
                    layer.workers -= 1;
                    if assignment.finished() {
                        drop(assignment);
                        let empty = {
                            let layer = scheduler.layers[index].as_ref().unwrap();
                            layer.workers == 0
                                && layer.blocked.is_empty()
                                && layer.parked.is_empty()
                        };
                        if empty {
                            scheduler.retire(index);
                        }
                    } else if assignment.residue() {
                        layer.blocked.push_back(assignment);
                    } else {
                        layer.parked.push_back(assignment);
                        starved = Some(index);
                    }
                }
            }
            scheduler.wake();
        }
        if scheduler.finished() {
            return;
        }
        match scheduler.pick(tier, previous) {
            Some(index) => {
                let layer = scheduler.layers[index].as_mut().unwrap();
                layer.workers += 1;
                let assignment = layer
                    .blocked
                    .pop_front()
                    .or_else(|| layer.parked.pop_front());
                let mint = Arc::clone(&layer.mint);
                drop(scheduler);
                let mut assignment =
                    assignment.unwrap_or_else(|| (mint.lock().unwrap())());
                previous = Some(index);
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    assignment.drive(schedule, tier)
                }));
                if outcome.is_err() {
                    if let Some(inner) = runtime.upgrade() {
                        inner.record_error(());
                    }
                    let mut scheduler = schedule.scheduler.lock().unwrap();
                    if let Some(layer) = scheduler.layers[index].as_mut() {
                        layer.workers -= 1;
                    }
                    scheduler.retire(index);
                    continue;
                }
                current = Some(assignment);
            }
            None => {
                if let Some(dry) = starved {
                    scheduler.post(dry);
                }
                match tier {
                    Tier::Burn => {
                        drop(scheduler);
                        std::hint::spin_loop();
                    }
                    _ => {
                        scheduler.idle.push(waker.clone());
                        slept = true;
                        drop(scheduler);
                        std::thread::park();
                    }
                }
            }
        }
    }
}

impl Schedule {
    pub(crate) fn join_wait(&self, sink: usize) {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        loop {
            let mut scheduler = self.scheduler.lock().unwrap();
            if scheduler.layers[sink].is_none() {
                return;
            }
            scheduler.waiter = Some(waker.clone());
            drop(scheduler);
            std::thread::park();
        }
    }
}
```

Panic-path note: the panicked assignment box is dropped by the unwind inside the closure's frame if drive itself panicked — `catch_unwind` returns before `current` is set, and the box is consumed by the closure only by `&mut`; it remains owned by `assignment` and is dropped when the arm `continue`s. The double `workers -= 1` hazard does not exist: the returning-assignment arm is skipped by `continue` (current is `None`).

Error payload: `record_error(())` matches the current `pub type Error = ()` transport (`runtime.rs:41`); when the error taxonomy lands it carries the panic payload.

- [ ] **Step 2: Checkpoint (run by the project owner)**

Compiles as part of Task 7's cutover check. Behavior is covered by Task 8's end-to-end canaries (single-thread liveness, double-EOF).

---

### Task 7: Cutover — wiring, runtime, runner, deletions

**Files:**
- Modify: `crates/bascet-core/src/pipeline/connect.rs`
- Modify: `crates/bascet-core/src/runtime.rs`
- Modify: `crates/bascet-core/src/runtime/dispatch.rs`
- Modify: `crates/bascet-core/src/runner.rs`
- Modify: `crates/bascet-core/src/consts.rs` (delete `PATIENCE_START/MIN/CAP`, `PRESSURE_*`; keep `DEPTH`, `WATERMARK`, `YIELD_*`)
- Modify: `crates/bascet-core/src/lib.rs`
- Delete: `crates/bascet-core/src/scheduler.rs`, `scheduler/driver.rs`, `scheduler/event.rs`, `scheduler/port.rs`, `scheduler/load.rs`, `scheduler/stride.rs`, `scheduler/layer.rs`, `scheduler/preempt.rs` (moved in Task 4), `runtime/pool.rs`

**Interfaces:**
- Consumes: everything above.
- Produces: the public API — `Runtime` (no type parameter), `RuntimeBuilder` (no `with_scheduler`), `Runtime::pipeline::<W>(..) -> Runner`, `Runner::join() -> Result<(), Error>`.

- [ ] **Step 1: Rewrite `Build`/`register` in `connect.rs`**

`Build` collects `Vec<Option<Layer>>` and `Vec<Vec<usize>>` (upstream indices per layer, recorded as each edge is wired: the consumer's entry gains the producer's index). `register` becomes:

```rust
pub(crate) fn register<A, M, W, U>(
    &mut self,
    apply: A,
    gather: U,
    downstream: Option<Downstream<A::Output>>,
    index: usize,
) where
    A: Work<M>,
    A::Output: Send + 'static,
    U: Gather<Item = A::Input>,
    W: Set,
    M: 'static,
{
    let preempt = Arc::new(AtomicU8::new(Preempt::Continue as u8));
    let probe_gather = gather.clone();
    let probe_tx = downstream
        .as_ref()
        .map(|downstream| Arc::clone(&downstream.output_tx));
    let probe: Box<dyn Fn() -> Probe + Send> = Box::new(move || Probe {
        input: !probe_gather.starved(),
        output: probe_tx
            .as_ref()
            .is_none_or(|output_tx| !output_tx.is_full()),
        exhausted: probe_gather.exhausted(),
    });
    let runtime = Arc::downgrade(&self.runtime);
    let mint_preempt = Arc::clone(&preempt);
    let mint: Mint = Arc::new(Mutex::new(Box::new(move || {
        apply.mint(&gather, &downstream, index, &mint_preempt, &runtime, PhantomData::<W>)
    })));
    self.layers[index] = Some(Layer {
        mint,
        probe,
        blocked: VecDeque::new(),
        parked: VecDeque::new(),
        workers: 0,
        pass: 0,
        preempt,
        mode: Mode::Synchronous,
    });
}
```

`probe.input` uses `!starved()`: starved means "empty and not done", so a drained-and-closed input reports `input: false, exhausted: true` and rides the EOF arm. The `Probe` gate plus the blocked-queue check in `Scheduler::runnable` is the complete runnability test from the spec.

- [ ] **Step 2: Rewrite `Runtime` and `pipeline()` in `runtime.rs`**

`Runtime` loses the `S` parameter; `RuntimeBuilder` loses `scheduler`/`with_scheduler`. `RuntimeInner` loses `events_tx`, `events_rx`, `registry`; keeps `dispatch`, `shutdown`, `error`, `burn`, `jobs`, `tasks`. `pipeline()`:

```rust
pub fn pipeline<W: Set>(self, pipeline: impl Assemble<W>) -> Runner {
    let inner = self.inner;
    let mut build = Build {
        runtime: Arc::clone(&inner),
        layers: Vec::new(),
        upstream: Vec::new(),
    };
    let (_count, sink) = pipeline.assemble(&mut build);
    let schedule = Arc::new(Schedule {
        scheduler: Mutex::new(Scheduler {
            layers: build.layers.into_boxed_slice(),
            upstream: build
                .upstream
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect(),
            idle: Vec::new(),
            waiter: None,
        }),
    });
    let closer = Arc::downgrade(&schedule);
    inner.shutdown.register(Box::new(move || {
        if let Some(schedule) = closer.upgrade() {
            let mut scheduler = schedule.scheduler.lock().unwrap();
            for entry in scheduler.layers.iter() {
                if let Some(layer) = entry {
                    layer.preempt.store(Preempt::Halt as u8, Ordering::Relaxed);
                }
            }
            for waker in scheduler.idle.drain(..) {
                waker.wake();
            }
        }
    }));
    let weak = Arc::downgrade(&inner);
    inner.dispatch.broadcast(|tier| {
        let schedule = Arc::clone(&schedule);
        let runtime = weak.clone();
        Box::new(move || crate::schedule::participate(&schedule, &runtime, tier))
    });
    Runner {
        runtime: inner,
        schedule,
        sink,
    }
}
```

The per-edge closers registered inside `Connect` (closing `input_rx` via the weak `Edge`) stay exactly as they are today. `Dispatch::broadcast`:

```rust
pub fn broadcast(&self, mut job: impl FnMut(Tier) -> Job) {
    for burn_tx in self.burn_txs.iter() {
        burn_tx.send(job(Tier::Burn)).ok();
    }
    for job_tx in self.job_txs.iter() {
        job_tx.send(job(Tier::Job)).ok();
    }
}
```

- [ ] **Step 3: Rewrite `Runner::join`**

```rust
pub struct Runner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) schedule: Arc<Schedule>,
    pub(crate) sink: usize,
}

impl Runner {
    pub fn join(self) -> Result<(), Error> {
        self.schedule.join_wait(self.sink);
        self.runtime.shutdown.trigger();
        match self.runtime.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
```

- [ ] **Step 4: Delete the old module tree and fix `lib.rs`**

Delete the files listed above. `lib.rs`: remove `scheduler` exports (`Scheduler`, `Auto`/`Stride`, `Driver`, `Event`, `Action`... note: `Action` the enum lives in the deleted `event.rs` — move `Action` and `Receipt` into `schedule/preempt.rs` beside `Preempt` as retained vocabulary), export `Preempt` from the new path, keep `Runtime`, `Pipeline`, `Runner`, `Tier`, `State`, sinks, sets. `assemble` currently returns `(controls.len(), 0)` with sink at index 0 — verify and keep: the sink is the first port created, index `0`.

- [ ] **Step 5: Checkpoint (run by the project owner)**

`cargo check -p bascet-core` then `cargo test -p bascet-core` — expected: unit tests from Tasks 1–4 pass; `tests/kanal.rs` passes; `tests/pipeline.rs` may fail pending Task 8's adaptations.

---

### Task 8: End-to-end canaries

**Files:**
- Modify: `crates/bascet-core/tests/pipeline.rs`
- Modify: `crates/bascet-core/tests/dispatch.rs` (adapt to `broadcast` if it exercised `send`)

**Interfaces:** none new — this task proves the spec's Verification section.

- [ ] **Step 1: Adapt existing e2e and add canaries**

Existing linear-pipeline and error-cascade tests keep their assertions; the builder calls lose nothing (`Runtime::builder().burn(0).jobs(n).build()`). Add:

```rust
#[test]
fn single_thread_pool_drives_three_layers() {
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(10_000))
            .layer(Double)
            .sink(sink::channel(collect_tx)),
    );
    runner.join().unwrap();
    assert_eq!(collect_rx.len(), 10_000);
}

#[test]
fn flat_map_overshoot_survives() {
    let runtime = Runtime::builder().burn(0).jobs(2).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(64))
            .layer(FanOut { per_item: 5_000 })
            .sink(sink::drain::<u32>()),
    );
    runner.join().unwrap();
}

#[test]
fn finalize_emits_per_assignment() {
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(1_000))
            .layer(Total::default())
            .sink(sink::channel(totals_tx)),
    );
    runner.join().unwrap();
    let sum: u64 = totals_rx.drain().map(u64::from).sum();
    assert_eq!(sum, (0..1_000).sum::<u64>());
}

#[test]
fn double_eof_retires_once_and_join_returns() {
    let runtime = Runtime::builder().burn(0).jobs(4).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(100_000))
            .layer(Double)
            .sink(sink::drain::<u32>()),
    );
    runner.join().unwrap();
}
```

Helper applies to write in the test file: `Count` (shared `Arc<AtomicUsize>` cursor, `out.finish()` at the limit — the existing bench source shape); `Double` (`out.push(input * 2)`); `FanOut` (pushes `per_item` copies — exercises overshoot + remainder, per-apply fan-out far above `budget`); `Total` (accumulates into a plain field, pushes it from `finish` — the per-assignment finalize semantics: the assertion sums *all* emitted partials, which is exactly the explicit-fold contract).

Stranded-residue and demand-post behavior are load-order dependent; they are covered structurally by `flat_map_overshoot_survives` (remainder + blocked queue) and `single_thread_pool_drives_three_layers` (every mechanism through one thread). The timed stall-runner and bursty benches stay owner-run evidence, not tests.

- [ ] **Step 2: Checkpoint (run by the project owner)**

`cargo test -p bascet-core` — expected: everything green. Then the owner's evidence loop: `tiers` (Burn ≥ Jobs), `bursty` (near-zero CPU during stalls in jobs-only config), repeated stall-runs with `sample` on any hang.

---

## Self-Review

**Spec coverage:** batching + flush/remainder (T1–T3), runnability/pick/post/wake/retire (T4), drive/visit/finish/budget coupling (T5), participation/idling/join/Unpark (T6), wiring/shutdown/deletions (T7), verification canaries (T8). `Ordered` intentionally out of scope (spec: reserved). Preempt vocabulary moves, `State`/`Action` retained (T5/T7).

**Known judgment calls encoded here, flagged for review:** `Mint` behind `Arc<Mutex>` because apply templates are not `Sync` (spec says "mint after unlock" — honored via clone-Arc-then-unlock); zip staging bound reuses `DEPTH`; `record_error(())` until the error taxonomy lands; zip `post` walk uses the first upstream member.

**Type consistency:** `Gather::{try_recv, starved, exhausted, residue}` used identically in T2/T3/T5/T7; `Probe { input, output, exhausted }` in T4/T7; `Assignment::{drive, layer, finished, residue}` in T4/T5/T6; `Emit::{push, flush, full, residue, finished, orphaned}` in T1/T5.
