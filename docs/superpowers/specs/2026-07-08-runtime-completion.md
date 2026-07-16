# Runtime Completion — Implementation Delta

Date: 2026-07-08, updated 2026-07-09
Branch: `dev/2.0`
Status: in progress — variadic parity + edge rework landed; scheduler (2a/2b) in flight

**Design authority:** `docs/superpowers/specs/2026-06-27-pipeline-scheduler-refactor-design.md`
(revised 2026-07-08). This document adds no design; it records what is implemented, what
is decided and in flight, and what remains. Where this document and the design doc
disagree, the design doc wins — except where a session decision below explicitly
supersedes a mechanism (each such case is marked).

## Landed this session

### bascet-variadic — parity with main restored

- `emit.rs::expand`: `@N[...]` expands **exactly N times with `#` = 0-based ordinal**,
  matching main's documented semantics (`// @n[...] - iterate n times with # = 0, 1, 2, ...`).
  The dev/2.0 rewrite had silently changed this to "domain values ≤ n", which every
  plain `1..=16`/`2..=16` call site was not written for. The unused env plumbing in
  `Transcriber`/`Frame` is gone.
- Call sites swept for the restored semantics:
  - `pipeline/gather.rs`: zips are `N = 2..=16, for N in N` — plain, no filter.
  - `set/attr_id.rs`: `AttrId` generalized from the 16-only singleton to id widths
    1..=16 (distinct tuple types, no conflicts); shift term is `4 * (N - 1 - #)`.
    The digit sites (binding-only, no `@` repetition) were unaffected.
  - `set/ops.rs`: the 16-wide `Same` singleton dissolved — the digit fold moved onto id
    tuples as `IdEq` (ordinary `1..=16` range) with one plain blanket `Same` on top.
    `Join`/`Meet` recursions rewritten peel-last: `N = 2..=16, M = 1..=15, for (N, M) in
    N.zip(M)`, last element named via `R~M`/`L~M` binding-concat. Same fold order and
    output types; no filter, no product.
  - `set.rs`: the `Set` collision-check recursion likewise peel-last (*last ∉ init +
    init is Set* — the same all-distinct invariant).
  - Unaffected by design: `owned.rs`, `attr.rs` (its `2..=16` now honestly means
    arities 2..=16), `phred.rs` and `attr_id.rs`'s digit product (genuine 2D domains,
    `product().filter()` stays).

### Edges — `Edge` restored, keeper removed (supersedes the design doc's keeper)

- `Edge<T>` is back as the clonable **weak storage handle**: `Arc<Inner>` holding both
  `Arc<Port>`s and `Weak` channel ends. `Edge::new(depth, producer, consumer)` replaces
  the free `wire` fn (returns the strong halves); `Edge::upstream()`/`downstream()` are
  the upgrade constructors per-worker views are minted from — upgrades die when the
  strong halves drop.
- The strong halves are symmetric per-view handles: `Upstream { input_rx: Arc<Receiver>,
  peek, exhausted, edge }` and `Downstream { output_tx: Arc<Sender>, peek, exhausted,
  edge }`. Channel-end liveness is owned by whoever holds the halves — **the layer's
  non-clone part** (the dispatch closure's captures / the registry entry), never by
  storage, never by a keeper. `Downstream.peek` is reserved staging (credits/async).
- **No keeper.** The keeper mechanism is superseded: buffered-drain across producer
  teardown and EOF signaling are carried by an edge-level `exhausted` flag (see 2b).
- `Emit` holds its worker's `Downstream` + `Worker` and push carries its protocol
  duties: `Blocked` activity and a band-gated `Promote` toward the consumer on a full
  channel (`Downstream::promote`, mirror of `Upstream::promote`), demand decay
  otherwise; `orphaned` is the view's `exhausted`.
- `Layer { gather, downstream: Option<Downstream<Out>> }`.
- Sync loop: **halted-exit bug fixed** — a preempted worker breaks out `State::Released`;
  only true EOF is `State::Finished`.
- The per-layer worker-minting closure is named **`Dispatch`**
  (`scheduler/driver.rs`; user rename of `Mint`). Note: the name currently collides
  with the `runtime::dispatch::Dispatch` thread pool when both are imported — resolved
  by inference at the one shared site; renaming one type is open.

### Pruned

- `tests/apply.rs` (compile-only) and the `worker/synchronous.rs` module test
  (duplicated the e2e). `tests/kanal.rs` is the design's permanent canary — stays.

## In flight — 2a: re-mintable layers (approved shape)

- Manual `Clone` on `Upstream`/`Downstream`: share the handles (`Arc` bumps + edge
  clone), fresh staging (`peek: None`, `exhausted: false`). This is the only honest
  `Clone` (`T` isn't `Clone`), and matches kanal's own endpoint-clone semantics.
- `Gather: Clone` as a supertrait — per-worker gathers come from cloning the pristine
  wired gather inside the dispatch closure.
- **Zip is a newtype over the flat member tuple; no cons chains** (supersedes the
  cons-chain shape approved earlier this session). `Zip<G>(Arc<Mutex<G>>)`, `G` the
  flat `(Upstream<A>, ..)` tuple. `Clone` is one generic impl — an `Arc` bump, so all
  workers share one staging, which is the point: per-worker cloned staging scrambles
  positional pairing at width > 1 (independent multi-channel takes interleave) and
  drops staged items on worker exit/revival; the shared inner makes staging outlive
  churn. The newtype also dissolves the >12 `Clone` wall (`Clone` on 13..16 flat
  tuples is unwritable — tuples are always foreign to coherence; the newtype is
  local).
  - `Gather for Zip<(..)>` (variadic 2..=16): `take` locks; stages each empty
    un-exhausted member (`peek()` try_recvs into the slot), promotes each starved
    member (no short-circuit) and returns `Starved`; otherwise extracts the row
    atomically; all-`None` → `Ok(None)`. Drain-the-survivors semantics verbatim;
    starved vs closed-and-drained stay distinct.
  - Critical section: at most N `try_recv`s + N `Option::take`s, never held across a
    wait (waits stay in the worker loop). Lock order Zip mutex → kanal internal,
    acyclic. Poisoning: `into_inner` — staging is structurally valid `Option`s and
    the panic already rode `Released(Panicked)`.
  - Row extraction atomicity is what the lock buys; row *order* across workers stays
    author-land (existing session decision). Linear layers are untouched: bare
    `Upstream` gather, clone = fresh view, kanal's internal synchronization is the
    only coordinator, no lock, no staging on the hot path.
  - The bare-tuple 2..=16 `Gather` impls die with this; the 1-tuple passthrough stays
    for `Connect`. No nested-row/`Flat` boundary exists — `take` assembles the flat
    row under the lock. Alternatives rejected: width-1 checkout minting (needs a
    width-cap mechanism the design deferred, serializes the join apply, saves one
    uncontended CAS per row), assembler layer into a second channel (+channel, +hop,
    +hidden worker).
- `Dispatch` becomes reusable: `FnMut(Arc<Worker>, Patience<u32>) -> Job`; each call
  clones the apply template and builds a fresh `Layer` (`gather.clone()`,
  `downstream.clone()`). Field naming: `dispatches`, not `mints`.

## Next — 2b: the scheduler (walked through, not yet started)

The event plane, `Driver`, `Pool`, and teardown are the design doc's mechanism,
unchanged. The policy is new — morsel-style work-pulling with pure stride fairness,
after Umbra's scheduler (Wagner/Kohn/Neumann, SIGMOD '21, "Self-Tuning Query
Scheduling for Analytical Workloads") — **not** a port of the old width-based
scheduler (`pipeline/scheduler.rs` + `coordinate/auto.rs` at HEAD). The old one's
scaling failures motivated the change, and all traced to one root: band geometry
(logarithmic) leaking into width arithmetic — cheap evidence overspawned the low
bands, exponential evidence demands starved the ramp at high ones, and
success-triggered demotion oscillated. Under work-pulling there is no width to
manage, so the failure class is structurally gone:

- Event loop on its own thread: `recv event → scheduler.schedule(event, &mut driver)`.
  `Scheduler { fn schedule(&mut self, event: Event, driver: &mut Driver) }`;
  `RuntimeBuilder::with_scheduler(..)` is generic, never boxed: `Runtime<S:
  Scheduler = Stride>` holds the scheduler by value **outside** the `Arc`'d inner;
  `pipeline(self, ..)` (already consuming) moves it into the drive thread, so the
  loop monomorphizes over `S`. Workers hold `Arc<RuntimeInner>` (record, shutdown)
  — they never see `S`, so the data plane stays non-generic. The bare builder
  constructs `Stride`.
- `Driver` (mechanism): per-layer `Control` entries (port, dispatch, teardown, source
  flag, and a runnable probe — a boxed edge-state read built at wire-up where the
  types are in scope; the strong-half anchors live here), the `Pool`, completions.
  Methods: spawn / shed / release / teardown.
- `Stride` (the design doc's `Auto`, renamed; policy — **supersedes the design
  doc's width-based Auto**; threads are fixed, layers own no workers, per-layer
  parallelism is however many threads currently run a layer):
  - Rank: **lowest pass among runnable layers wins**; assignment increments pass.
    Pass is a plain per-layer field in `Auto` — an odometer of turns served, used
    only to rank runnable layers against each other; it is not width and never
    decreases. **Increments are strictly positive — the liveness invariant**: every
    serve makes the served layer relatively less entitled, so every runnable waiter
    catches up in bounded turns. A stride of 0 is not a priority, it is a liveness
    violation. Pure stride — no weighting. The pool `Record` dissolves (`pass`
    moves here, `waiting` has no stride equivalent); `Pool` keeps exactly its
    mechanism job — persistent parked threads per tier and free slot lists. Work
    moves to threads; threads are never respawned.
  - Runnable: input edge non-empty **or input exhausted** (sources: not finished),
    and output edge not full — edge state read directly at pick time. The exhausted
    clause is load-bearing: EOF must be observable by a worker (`Ok(None)` →
    `Finished` → teardown), so an exhausted-and-drained layer stays runnable until
    torn down; without it a zero-worker consumer of a finished producer never
    finishes and `join` hangs. Bands never enter the rank; a band crossing is only
    the wake signal ("reconsider this layer"), their correct job. The probe races
    benignly against running consumers — worst case one wasted dispatch that
    starves out and self-corrects; the zip probe may be conservative (any member
    non-empty) at the same cost; the EOF window can over-assign a few workers who
    observe `Ok(None)` and release, bounded and terminal. Never lock edges during
    picks.
  - Scale-to-zero is the runnable predicate's job: a waiting layer leaves the pick
    set, its starved workers exit, and zero workers is its resting state — the
    design doc's "sleeping at zero is valid" survives stride unchanged. A layer
    returning after an idle stretch holds the lowest pass in the room and wins the
    next thread immediately — history does the prioritization.
  - Promote: free thread → assign it to the lowest-pass runnable layer. No free
    thread and `pass[subject] <= pass[running]` → `Preempt::Yield`: the tie counts
    as outranking, because the runner has been served continuously since its last
    bump — with strict `<` and unbounded runs, equal-pass waiters starve forever.
    Victim layer = highest pass among running (ties rotated); victim worker within
    it = most recently assigned, **preferring Job-tier victims — a Burn worker is
    evicted only when no Job victim exists** (Burn is the low-latency hot path;
    its cores change hands only when strictly necessary). Rotation churn is
    rate-limited by band-gated promotes and the round latch. Subject not runnable
    → drop.
  - Ties break by rotation, never randomness, never first-index (pathological):
    `Auto` keeps one scan offset, each pick starting one past the last winner; the
    same offset serves pick ties and victim ties.
  - Released: pass and seed bookkeeping (yield budget and patience fold per layer);
    the freed thread re-picks immediately; `Finished` + no live workers → teardown.
    Orphan wake rides the pick loop — every pick scans all runnable layers, so a
    consumer left sleeping on buffered input by its producer's teardown is served
    by the very `Released` that triggered that teardown. No nudge mechanism exists.
  - Demote becomes vocabulary-only — starvation exits replace idle self-reports;
    Acquire/Yield likewise stay speakable (unused variants cost nothing).
  - Initial state is emergent: the build kicks sources with one `Promote` each, and
    sources are the only runnable layers at start. Sources are coordinated
    implementations by contract (clone-shared cursors, pair-component style), so
    the scheduler treats them like any layer — no source caps. The last-worker
    shed guard dissolves into the runnable scan (a yielded layer with buffered
    input stays in the pick set) plus the patience-expiry re-nudge.
  - Dissolved with width management: desired-width arithmetic, `useful_width`, fair
    share (stride is the fairness), appetite/feedback learning, the tier ladder,
    upgrades, eviction compensation. Burn stays as pinned threads — affinity is a
    thread property, not a policy concern; Task arrives with the async chunk as
    permits in the same stride pick.
- Worker run semantics (**supersedes the design doc's checkpoint-actioned Yield**):
  - `streak` = consecutive items processed without a channel miss; it feeds patience
    (wait behavior, gauge-fold checkpoints) exactly as before — and nothing else.
  - Yield budget `N`: a run processes items in rounds of `N` items total (not
    `N` streak). Rounds count **hits** — the same miss/hit grammar as patience, and
    the budget reuses `Patience<u32>` itself. Completing a round unpreempted grows
    `N`; being preempted shrinks it; clamped to [`YIELD_MIN`, `YIELD_CAP`] — the
    cap is the polite-eviction latency contract. No clocks.
  - There is no latch variable — **the preempt atomic is the latch** (nothing ever
    writes `Continue` back, and every `Worker` is minted fresh per spawn, so reset
    is by construction). One relaxed `preempted()` load per item serves everything:
    `Halt` exits at the next item boundary; `Yield` is honored at round boundaries
    and at channel misses — a missing worker makes no progress and holds no item,
    and without the miss clause a trickle-fed worker's eviction latency is
    unbounded. Yield latency is therefore round-granular (≤ current `N`, capped);
    every scheduled run still gets a full round of progress or its input runs dry —
    young-release thrash cannot happen.
  - Exits are symmetric at item boundaries, and a worker never exits holding an
    item: starved past its wait ladder → `Released`; output edge full at the
    boundary → `Released`. The stall is observed **by push itself** (one post-send
    fullness read where push's protocol branch already lives, recorded as
    `Activity::Blocked` on the worker's own line; plain re-assignment per push, so
    it self-clears when the edge drains); the boundary exit reads the worker's own
    activity — no channel access at the boundary. The post-send read is
    load-bearing: the worker whose send *fills* the edge never parks, so it always
    reaches its next boundary and frees a thread — this closes the deterministic
    all-threads-on-one-producer startup deadlock. Residual: fan-out applies can
    still park mid-item in rare alignment (narrow race, author contract per-item
    fan-out ≪ depth; the watchdog's stall predicate is its detector). Threads park
    only when nothing is runnable (the pool inbox recv is the park).
  - The wait ladder is tiered (supersedes the single-park wording): **Burn** is
    captive-hot — spin with patience-cadenced slowpath (act on preempt, promote,
    fold gauges), never parks, never starve-exits; it leaves only preempted or
    exhausted, and burn cores never idle. **Job** spins for patience, runs the
    slowpath, then parks bounded on the channel — for a linear upstream the woken
    `recv` item *is* the next input (recv-as-take, nothing consumed out of
    protocol); on timeout it exits `Released`. **Task** (async chunk) runs the
    slowpath and parks immediately: `select_biased!(op.await, patience timer)`.
    Zip members have no joint multi-channel wait — zips use the spin/sleep rungs
    only. The wait seam lives on `Gather` (each impl waits its own way, tier
    passed in).
  - `State` refines exit reasons (this is the learning input): `Starved`,
    `Blocked`, `Yielded`, `Halted` join `Finished`/`Panicked` as Released-flavors
    published through the existing `finish()` path, plus **`Failed`** — apply
    returned `Err` — which triggers teardown exactly like `Finished` with the
    error recorded where it is in hand (`record_error`; the guard records only
    panics, since it never holds an error value). The event vocabulary is
    unchanged — the action is still `Released`.
  - Edges stay `kanal::bounded(depth)` exactly as constructed — no slack capacity,
    no soft budgets (a second cap is the first cap with extra steps). A mid-item
    push into a full edge parks in kanal: accepted — bounded is bounded. Author
    contract: per-item fan-out ≪ edge depth.
- **Teardown / EOF (supersedes the design doc's keeper + pull-close):** `Edge` gains an
  `exhausted` flag. Producer-side teardown sets it and drops nothing (anchors live
  until the drive loop ends — buffers survive for draining); `Upstream::try_take` on
  empty checks it — drain first, then EOF. Teardown closes its own *input* edges
  (voiding — no consumer exists), which errors producers' pushes → the upward orphan
  cascade. Sink teardown sends the completion `Runner::join` waits on. `Shutdown` gets
  every edge's closer registered at wire-up, making forced shutdown real.
- Sync worker waits move onto kanal: `wait_input`'s `thread::sleep` polling becomes a
  kanal-parked, patience-bounded wait (`recv_timeout` shape); on expiry the worker
  exits `Released` instead of looping — under stride the thread re-picks and the
  starved layer re-enters the ranking. Edges constructed dual-capable
  (`as_sync()`/`as_async()` views) so the async loop lands on the same channels
  later.
- Tests: scaling e2e (slow middle layer, clone-counting apply, output complete +
  clones > 1) and error-cascade e2e (failing sink → `join()` is `Err`, no hang).

## Still pending after 2b

- Per-edge `Wants` threading through `Connect` (type-level; unchanged plan).
- Async chunk: Task tier (compio dispatcher + permits), System thread hosting the loop,
  `worker/asynchronous.rs`, `Emit`/`AsyncEmit` twins, Burn/Job tier movement. Edge
  waits are `select_biased!(op.await, patience timer)` — the timer arm is the
  checkpoint (demote/preempt cadence). Open: an async zip join's multi-member wait.
- Audit against the design doc (`lib.rs` exports, module layout, `private_bounds` lint
  on `Assemble`, the `Dispatch`/`Dispatch` name collision).

## Deferred / parked

- Credits / demand protocol (`WATERMARK` is its scaffolding — stays). Watchdog.
  `reject(reason)`. Zip/merge builder surface. `Pressure` repack; per-edge
  byte-budgeted depth. Benches (attic untouched).
- `Stride::learn` — shape settled 2026-07-09, adjustment policy benches-gated.
  Three per-layer knobs, each literally a `Patience<u32>`, adjusted at `Released`
  from the `State` exit reasons: patience seed (folds now), budget seed (final `N`
  published beside final patience; layers keep learned round sizes across runs),
  admission threshold `k` (runnable requires `input len >= k`; raised by
  starved-young exits, decayed by completed rounds — self-relaxes to 1 under
  healthy flow, and converts the wasted-dispatch races into a self-tuning cost).
- **Recursive tuples — deliberated 2026-07-09, closed.** Runtime effect is zero (the
  set algebra is phantom; value boundaries are move-only, erased in release), no
  pending feature needs it, and maintainability alone didn't justify the churn.
  Should it ever revive: own engine over bare nested pairs (`(A, (B, ()))`, `()`
  nil), engine traits module-namespaced beside one blanket impl per public trait
  (coherence ignores where-clauses, so engine impls can't share the public trait),
  `Ref` via the `&'a S` engine shape (the `for<'a>` GAT bound is inexpressible).
  frunk re-rejected (inference-driven, presence-only); tuplities rejected (v0.1.4,
  typenum, supplies only the ~14-line boundary the variadic already generates). Zip
  ended up flat behind a newtype — no recursion anywhere.
- `AttrId` cross-width comparison: `IdEq` has no cross-width impls, so mixed-width id
  sets fail as unsatisfiable bounds, not `Miss`; short ids also collide with
  zero-padded ones on const `ID`. Dormant — the derive and `attr_id!` always emit 16
  digits.

## Session decisions (cumulative)

- Ordering/pairing is composed by pipeline authors (ordering layers); the runtime never
  enforces or marks anything.
- Macro semantics = main's: `@N` repeats N times, 0-based `#`. Plain filterless headers
  everywhere; `filter` only on genuine 2D domains; singletons dissolved by
  generalization rather than spelled as degenerate ranges.
- Edge-channel liveness lives on the layer (strong halves in the registry/dispatch
  captures); storage handles are weak; no keeper; edge-level EOF flag is named
  `exhausted` (same word, same meaning as the view-level flag).
- `Clone` is the per-worker instancing mechanism; no bespoke constructor methods.
  Fresh staging for the halves (and thus linear gathers); shared staging for `Zip`
  (`Arc` bump) — each the honest semantics for its shape.
- Recursive tuples: deliberated and closed, no internal recursion anywhere (see
  Deferred). Zip stays flat behind its newtype; the variadic keeps stamping the
  protocol impls.
- Zip pairing is protected by the gather, not the scheduler: `Zip`'s mutex makes row
  extraction atomic at any width. Per-worker staging was rejected (pairing scramble,
  staged-item loss on exit/revival); width caps, checkout minting, and assembler
  layers were rejected (scheduler mechanism / extra channel for less capability).
- kanal reaffirmed as the only data-edge transport, with the real rationale recorded:
  Task-tier wait correctness requires waker-parked channel ops — polling waits cost
  O(waiters) spurious wakeups per timer quantum on a shared executor and `yield_now`
  loops spin (the old loop's `send` did exactly this; not reproduced). Rings,
  doorbell hybrids, and SPSC lanes are rejected for edges at any arity of cleverness.
- Edges are constructed dual-capable — one kanal channel, `as_sync()`/`as_async()`
  views minted per worker world (the old edge's shape); the current sync-only
  `kanal::bounded` construction is a placeholder.
- Worker waits are a tiered ladder: Burn spins captive — `spin_loop` and retry,
  preempt checked every retry, no patience arithmetic (never parks, never
  starve-exits — leaves only preempted or exhausted; burn cores never idle, and
  they change hands only when no Job victim exists); Job parks bounded on the
  channel (`Gather::recv_timeout` — the woken item is the next input for linear;
  timeout → `Released`); Task parks immediately (`select_biased!(op.await,
  patience timer)`, async chunk). Zips have no joint wait — sleep-then-retry. The
  wait seam is on `Gather`; the gather vocabulary is kanal's own (`try_recv`,
  `recv_timeout`, `starved`, `close`). `wait_input`'s `thread::sleep` polling is
  superseded — fix rides 2b.
- Naming: no coined names — `Dispatch` (user's), `exhausted`, `input_rx`/`output_tx`,
  design-doc words elsewhere (`Control`, `Driver`, `Auto`, `Scheduler`).
- Protocol vocabulary is never deleted for being unexercised (`Action::Yield`,
  `Preempt`, `Receipt`, `Event::sender`, `State::Running`, `Shutdown`, `WATERMARK`,
  `Pressure`/`Patience`/`Pool` API all stay).
- Scheduler policy is morsel/stride (Umbra, SIGMOD '21), not width management: fixed
  threads pull work, lowest-pass runnable layer wins, per-layer parallelism is
  emergent. Bands signal, never decide — the old scheduler's scaling failures traced
  to logarithmic band geometry leaking into width arithmetic.
- Preempt is budgeted and item-granular: rounds of `N` total items (hits, reusing
  `Patience<u32>`); `Yield` latches until the current round completes **or the
  first channel miss**, then acts at item granularity; unpreempted rounds grow
  `N`, preemption shrinks it, clamped — patience's shape applied to eviction.
  `streak` stays miss-relative and feeds patience only.
- Starvation is an exit for Job/Task (bounded wait, then `Released`), never for
  Burn (captive by design); symmetric on the output side — full edge at an item
  boundary → promote + exit. A worker never exits holding an item; threads park
  only when nothing is runnable.
- Edges stay `kanal::bounded(depth)` — no slack, no soft budgets. Mid-item parks
  at the cap are accepted (author contract: per-item fan-out ≪ depth).
- Runnable includes exhausted-until-torn-down: EOF is observed by a worker, so an
  exhausted-and-drained layer stays in the pick set until teardown. The residual
  EOF-window over-assignment is benign, bounded, terminal.
- `State` carries the exit reason (`Starved`/`Blocked`/`Yielded`/`Halted` beside
  `Finished`/`Panicked`) through the existing `finish()` publish; the event action
  stays `Released`. Reasons are the learning input.
- `Runtime<S: Scheduler = Stride>`: scheduler by value outside the `Arc`, moved
  into the drive thread by the consuming `pipeline()`; `with_scheduler` stays,
  generic, no boxing; workers hold `Arc<RuntimeInner>`; `Runtime<S>` is not
  `Clone` (nothing needed it). `Auto` is renamed `Stride`.
- Internal names are boring and literal, never cute: the gather probe is
  `starved()` (reusing the domain word, inverted at the call site), teardown's
  input closing is `close()`. Coined or metaphorical names are reserved for
  public API, if anywhere.
- Stride increments are strictly positive — the liveness invariant. Pass is a
  per-layer odometer in `Auto`, never width; scale-to-zero is the runnable
  predicate's job. Outranking is `<=` with one rotating scan offset breaking all
  ties. `Pool` keeps threads and free lists; `Record` dissolves. No teardown
  nudge — orphan wake rides the pick loop.
