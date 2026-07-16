# Self-Dispatch: Worker-Owned Scheduling

Supersedes the central driver event loop of `2026-06-27-pipeline-scheduler-refactor-design.md` and the pressure-scored scheduler on disk. Ownership-EOF, budgeted rounds, and the tier pool carry forward unchanged; the typed data plane carries forward re-denominated in batches (see The data plane).

Revised 2026-07-14 after review: edges carry batches and flush-at-boundary joins the liveness argument (deleting the captive-send class), `pass` counts rounds, `Apply` gains a defaulted `finish`, zip yields aligned row batches, `Ordered` is reserved as an opt-in emit marker (room left, not implemented — sketch on main), and the author/runtime split is stated under Coordination. Rejected in the same review: width caps, `before`/`after` hooks, a batch-level authoring trait, pluggable edge cores. Renamed 2026-07-15: `Driver` → `Schedule`, `Controls` → `Layers`, `Control` → `Layer` — the old two-field `Layer` carrier dissolves into the mint, freeing the name. Amended later the same day: the demand protocol — `Preempt` goes per-layer, `Halt` is posted on demand and answered at item granularity, the regular round-boundary yield visit closes the busy-worker blindness; depth and the buffer cap are demoted to allocation bounds; flush precedes every visit. Amended 2026-07-15: `drive` returns nothing — the participation loop owns park-and-pick; camping and `epoch` are deleted (Burn idles by spin-retrying the pick, Job on the condvar); tiers are placement plus affinity strength; `pass` is a plain field on `Layer`, credited under the lock; runnability is pinned as the demand gate, and a demand-weighted pick (pressure, or a service−demand balance) was examined and rejected — min-pass already orders runnable layers by service deficit, which is backlog by conservation. Reviewed adversarially the same day; fixes folded in: `blocked`/`parked` queue split (parked residue was invisible to the probe — a stranded-tail hang); retire moved to the last drop (finished assignments were re-parked — a livelock); the `Halt` post moved to the participation loop's none-arm (the visit posted on healthy inter-batch starves, jamming the budget at its floor); the wake duty pinned to every visit including stays; a claimed `Halt` breaks Burn's stay for one fair pick, and a no-idlers wake posts `Halt` to the highest-pass manned layer; the condvar, the `Scheduler` trait, and the `down` counter are deleted — one wake type (`Waker`, threads wrapped via `std::task::Wake`), policy hard-coded, all-retired by scan. Post-implementation, from the first live runs: `pass` is credited per quantum granted — handout and stays — because a serve that credits nothing is a stride of zero, the liveness violation behind an EOF mint-livelock; `drive` returns the worker's `State` and `Finished` is clean-only by construction; the first `Finished` drops the mint and the layer joins on its workers; the wake duty's donor arm is deleted — runnable work self-serves within a round or an edge-fill, only trapped demand posts.

## Motivation

Every scheduler failure found during the 2.0 bring-up — band-gate saturation, the spawn-storm livelock, the captive-send wedge, the false-shed Burn regression — had one root: scheduling initiative lived on a different thread than the information and the spare cycles. Workers had to signal a remote scheduler, and no signaling cadence survives the constraint set: clocks are banned, iteration counts are hardware-dependent, per-item gauge updates are hot-path costs, and accumulators without decay ratchet into silence. The conclusion is not a fifth patch; it is that the signaling plane should not exist.

In Umbra's morsel scheduler the thread that just became free is the scheduler, for exactly one decision: what do I do next. Completion is the only event. This spec is that idea adapted to a streaming runtime, where — unlike Umbra's fused pipelines — layers run concurrently over bounded edges and can transiently run dry. The adaptation: a thread with nothing runnable never waits on data — it asks the schedule again; Burn hot (spin-retry), Job cold (sleep until the schedule changes). Nothing in between, and nobody signals.

The dividing rule for primitives, which also answers where channels belong at all:

**Channels carry items — batched; the batch is the channel item. Scheduling reads shared state. Nothing ever waits on a channel: every recv is a `try_recv`, every send a `try_send`; all waiting is against the schedule.**

## Principles

1. **Cost gradient.** Per item: a pending pop, apply, a buffer push, and one relaxed load answering only `Halt` — no channel op, no shared write, no lock. Per round (`budget` = 64–1024 items, adaptive): flush and one lock visit — the regular yield, crediting `pass` under the lock it already holds; the stay fast path keeps it a short critical section with no park. Per boundary (starve, block, finish): the same visit without the stay path. Idle: Burn spin-retries the pick, Job parks as a `Waker` in the idle list — entered only when no compatible work exists anywhere.
2. **No clocks, no new constants.** No `Instant`, no timeouts, no iteration quanta. All cadences are measured in items and bounded by real resources already in the system: edge depth (`DEPTH`), the round budget, the pool size.
3. **Work-conservation first, affinity when free.** A thread never idles while compatible runnable work exists. When nothing exists, it idles against the schedule — Burn by retrying, Job by sleeping — and its parked assignment keeps its place warm for the return.
4. **Ownership carries lifecycle.** Retire = drop the `Layer` entry; kanal refcounts close the streams; shutdown = close the channels. Termination needs no flags and no protocol.
5. **No held output.** Flush precedes every visit — round yield, starve, block, `Halt` claim, finish, retire, error. Unflushed items are invisible; the entire wake protocol reasons over channel state.

## The data plane

Edges carry `Vec<T>`; the batch is the channel item. Authors still think single-item on both sides — the design doc's promise stands: `Emit::push` appends to a per-assignment buffer, and the gather serves items from a per-assignment pending queue refilled one batch `recv` at a time. Per item the channel is never touched; per round it is touched twice.

The round is the flush cadence: the regular yield visit at every `budget` items flushes, credits `pass`, and checks in with the schedule — steady-state batches are round-sized, a posted `Halt` cuts them shorter, boundaries flush partials. The regular yield defers to the batch boundary: a popped batch is the morsel and is never split for fairness — `round` runs past `budget` until the pending drains, so the check-in interval is `[budget, budget + one batch)`, still bounded in items by the producer's own cap. Only `Halt` cuts inside a batch, which is why routine switches park clean and `blocked` stays the exception. Flat-map overshoot is legal — one apply may push past the budget; the buffer grows and flushes at the next item boundary; batch sizes are nominal, not exact. Budget adaptivity's signal is the Patience grammar: reaching the regular yield grows it (`budget.hit()` — the round completed undisturbed), a `Halt` claim shrinks it (`budget.miss()` — demand needed this worker sooner than its round allowed). Layers under recurring demand converge to short rounds and low latency; quiet ones drift toward the cap and low overhead. The buffer's cap is, like depth, an allocation bound — never a metric.

Two rules carry the liveness weight:

- **Flush is non-blocking, always.** Flush is one `try_send` of the whole buffer; a refused flush keeps the buffer as the *remainder*, remainder-held is the `full()` condition and takes the block boundary, and a resumed assignment re-flushes before touching its next item. No code path performs a blocking send — the captive-send class (a worker asleep inside kanal, invisible to every probe, deaf to every wake) is structurally deleted.
- **No held output** (Principle 5): every exit from the hot loop flushes first. Probes and the wake protocol reason over channel state alone; a parked partial batch would be downstream starvation nothing can observe.

Depth is an allocation bound and nothing else — it exists so the channel is not an unbounded list; scheduling never reasons about its value. Generous is fine; it merely must not silently multiply (64 batches ≈ 4096 buffered items).

Zip takes the same shape at its staging: member edges carry batches; the shared tuple holds each member's current batch and offset; one lock per take pairs the fronts and yields the longest aligned prefix as one row batch `Vec<(Option<A>, ..)>` — every slot `Some` until its member is closed *and* drained, `None` after, input ends at all-`None`. The mutex is paid per batch instead of per row. Pairing is positional in edge order; keeping a member branch order-preserving is the author's contract (`Ordered`, under Coordination, is the tool when a branch must run wide).

Batching is not a follow-up optimization: non-blocking flush is load-bearing for the liveness claims under Waiting, so the batch data plane lands with self-dispatch, not after it.

## The worker loop

Each pool thread runs one loop. All scheduling is inline in it; there is no scheduler entity anywhere.

The typed inner loop (`drive`, monomorphized per layer):

```
drive: loop {
    if preempt == Halt {                         // per item: one relaxed load, predicted not taken
        claim (CAS Halt -> Continue)?
            -> budget.miss(); flush; visit       //   demand cut the round short: tighten the cadence
    }
    match gather.try_recv() {                    // pending pop; refill = one batch recv
        Ok(Some(item)) =>                        // fast path: a straight line, all local
            apply(item) -> emit.push(..)         //   append to the buffer
                                                 //   error => flush; return Failed (the loop retires)
            if emit.finished() { return conclude() }
            round += 1
            if round >= budget && !gather.residue() {
                                                 // the regular Yield: finish the batch, then check in
                round = 0; budget.hit()          //   undisturbed round => continue and grow
                flush; visit                     //   leave => return Yielded (Blocked if holding)
            }
        Ok(None) =>                              // starve boundary: pending and channel empty
            flush; visit                         //   never a stay; return Starved (Blocked if holding)
        Err(Closed) =>
            return conclude()                    //   finalize once; Finished, or Blocked if the flush refused
    }
}

visit = lock {
    own layer retired? unlock; return from drive // a Failed sibling took the entry; the loop cleans up
    store Continue on own layer's preempt        // any visit satisfies a pending post
    credit pass on a stay                        // every grant credits; the handout credited the first round
    wake duty: one idler per runnable-unmanned layer — every visit, stays included
    stay = own layer runnable
           && ((Burn && not a claim) || pick(previous) == own layer)
                                                 //   Burn is sticky while runnable: cache stays hot
                                                 //   a Halt claim forces one fair pick, even for Burn
                                                 //   Job re-picks every time: moves when someone needier exists
    stay -> unlock; keep driving                 //   no park, short critical section
    else -> unlock; return from drive            //   the participation loop parks and re-picks
}
```

`drive` returns its `State` — what happened, never what to do next; the loop makes every scheduling decision. A resumed finalized assignment goes straight to `conclude` and only retries its flush.

Exits are flushed-clean by construction: `finish` runs at most once per assignment (a scratch flag); a refused flush on any exit path returns `Blocked` — the assignment parks holding its remainder and completes on resume — so `Finished` is a status a worker can only return clean, and retire only ever drops empty buffers.

The one thing that cannot be inline is entering another layer's code: each layer's loop is monomorphized, so changing layers requires returning to a single type-erased frame rather than calling across (which would grow the stack on every switch). Nothing crosses the return — `drive` hands back only control; the participation loop re-asks the schedule at the moment the thread is actually free:

```
loop {
    lock:
        returning? workers -= 1; file by the returned State:
            its layer retired -> drop it
            Finished -> drop the mint (no new worker will run) and the assignment;
                        the join completes when workers == 0 with both queues empty -> retire
            Blocked  -> blocked.push_back            // holding remainder or pending
            Starved  -> parked.push_back             // the dry park: the Halt post candidate
            Yielded  -> parked.push_back
            Failed   -> drop it; retire the layer
        if every layer is None { return }            // participation ends (a scan, not a counter)
        pick(previous)?
            some -> assignment = blocked.pop_front() or parked.pop_front() or mint
                    workers += 1; pass += 1          // every grant credits: stride zero is a liveness violation
                                                     // the mint (user Clone) runs after unlock
            none -> just parked a dry layer? post Halt on the nearest manned layer up
                    its dry chain (else on the highest-pass manned layer)
                    Burn -> unlock, spin, retry      // idle = retry the pick, hot
                    else -> push own waker to idle; unlock; park; retry
    catch_unwind(assignment.drive())
        // panic: record error via Weak<RuntimeInner>, retire the layer, drop the assignment
}
```

An `Assignment` is a parked `Run` behind a box: the apply clone, gather, and emit handles, with scratch state intact. `drive` returns the worker's `State` — `Finished` (finalized and holding nothing: the only clean way to say it), `Blocked` (holding remainder or pending), `Starved` (input dry), `Yielded` (left at a check-in), `Failed` — and the box exposes one other fact, `layer()`. The loop files by status; it never interrogates the box. Parking instead of dropping is the residency mechanism — a worker returning to a layer resumes a warm instance; the apply clone is paid once per instance, not per visit. Two FIFO queues per layer hold them: `blocked` (returned holding residue — an unsent remainder or unprocessed pending; resumed first, and counted as input by the probe, or a lull would strand the tail inside the box) and `parked` (returned clean). Handle ownership stays layer-scoped: mint, probe, and both queues live inside the `Layer` entry, so retire drops every handle of the layer in one move (the ownership-EOF invariant, restated: handles live in mint, probe, and assignments — all reachable from the `Layer`).

## The Schedule

The central driver is gone entirely; `Schedule` is the shared state worker slow paths lock:

```
Schedule {
    scheduler: Mutex<Scheduler>,        // the only field: workers lock the schedule; inside is the scheduler
}

Scheduler {
    layers: Box<[Option<Layer>]>,       // None = retired; all-retired is a scan, not a counter
    idle:   Vec<Waker>,                 // parked idle workers, any world; visits pop-and-wake
    waiter: Option<Waker>,              // the joiner; any retire takes-and-wakes it, the joiner re-checks
}

Layer {
    mint:    Box<dyn FnMut() -> Assignment + Send>,
    probe:   Box<dyn Fn() -> bool + Send>,     // channel non-empty && output not full
    blocked: VecDeque<Assignment>,             // parked holding residue; FIFO; resumed first
    parked:  VecDeque<Assignment>,             // parked clean; FIFO
    workers: usize,
    pass:    u64,                              // rounds served, ever; plain — every credit site holds the lock
    preempt: Arc<AtomicU8>,                    // the one assignment-reachable field: the hot loop loads it per item
}
```

Every visit — stays included — pops and wakes one idle `Waker` per runnable-unmanned layer. There is no donor arm: runnable work is visible to every picker — a Job worker's next fair pick or a Burn worker's next boundary reaches it within a round or an edge-fill — so only invisible demand, items trapped in a mid-round worker's buffer, warrants a post, and that is the participation loop's none-arm. Retire and shutdown drain the idle list and wake the waiter. That is the entire wake protocol, and it is not signaling in the old sense: wakes are edge-of-lock effects of decisions already made in shared state, and a wake means "look again," never "it's true" — every woken thread re-derives everything from the schedule under the lock. Burn threads never park — an idle Burn core retries the pick and discovers changes by looking.

One wake type serves both worlds: `std::task::Waker`. Each pool thread caches an unpark-waker of itself at spawn (`std::task::Wake` over `Thread::unpark` — the park token makes wake-before-park safe); an async joiner or a Task-tier waiter, later, stores its executor waker in the same slots. There is no condvar: a condvar is a park-list std maintains, and the scheduler holds the list itself, under the lock it already owns, with deterministic pop-one targeting. A spuriously woken sync thread removes its own entry (`will_wake`) before re-parking.

`Preempt` graduates from vocabulary to mechanism, per-layer. `Halt` is the only value ever posted, from two sites: the participation loop's none-arm (demand with nothing pickable — the wanted items are trapped in a mid-round worker's buffer; the fallback donor is the highest-pass manned layer), and forced shutdown — stop latency is one item, not one round. `Yield` is never posted; it is the name of the regular round-boundary visit every worker already performs. Claims CAS `Halt → Continue` at an item boundary — the first worker of the layer to reach one claims, emergently the one closest to its round's end — and any natural visit of the layer stores `Continue`, so a satisfied post cannot shed a second worker. A claim does double duty: it is the budget's `miss()` (recurring demand tightens the layer's check-in cadence by itself; the regular yield is the `hit()`), and it suspends Burn's stay for one fair pick — the strong evidence that moves a sticky core. Per-layer, not per-worker and not per-tier: asynchronous layers live outside this pool entirely, so posting needs no tier scoping.

## Picking

The `Scheduler` trait is deleted, along with `Runtime<S>` and `with_scheduler`: the policy is load-bearing across visits (pass crediting), stay rules, and victim selection — a pick-only plug point was a dishonest seam, and no second policy exists. The pick is a plain function of the scheduler state, called under the lock:

```
fn pick(scheduler: &Scheduler, previous: Option<usize>) -> Option<usize>
```

Runnable is the demand test, binary and current. There is no tier-compatibility clause: asynchronous layers are not entries in this schedule at all — the Task tier, when it lands, brings its own pool and its own arrangement. A layer is in the pick set iff: it has something to hand a thread — the mint still exists or a queue is non-empty; input available — at least one batch in its channel *or a blocked assignment holding residue* (sources trivially pass until finished; zip's probe is conservative, and a wasted pick starves out at the first take and self-corrects); output not full — room for a flush (sinks trivially pass); plus the EOF arm — an exhausted layer stays pickable while it has parked or blocked assignments left to walk through their own `Closed`, or none minted yet to observe it (queues non-empty || workers == 0). Runnable means running it advances the flow *right now*; everything demand-shaped lives in this gate. An admission threshold (`len ≥ k`) stays deferred: threshold one, wasted picks self-correct.

`Stride`, the default, orders what the gate lets through: minimum `pass` among runnable layers. `pass` is a plain field, credited under the lock per quantum granted — once at the handout, once at each stay — so every serve advances it unconditionally: a serve that credited nothing would be a stride of zero, the liveness violation (an EOF-looping source stays minimum-pass forever and threads mint at it while real work starves). It counts rounds served — and by conservation that ordering *is* demand measured as deficit: items consumed ≈ pass × budget, an edge's backlog is proportional to the producer−consumer pass gap, so the min-pass layer is precisely the one with the most input queued. Fairness — no runnable layer waits unboundedly, because credits are strictly positive — is the corollary, not the objective. A layer that could not run could not be credited, so it re-enters the pick set as the minimum and wins immediately: frozen `pass` is wake priority, which is what a starved zip needs. Ties break to `previous` when it is among the minimum (free affinity — mostly decorative for Burn, whose stay rule already keeps it home), else downstream (lower index — indices are topological, sink first). Sources self-throttle: output full ⇒ probe false. The honest weak spot is cross-branch comparison (zip siblings, merge arms), where conservation couples less tightly; the gate and the tie-breaks carry it, and the pick is one function to edit if a bench ever shows a branch imbalance the deficit cannot see.

## Waiting

A thread with nothing runnable idles against the schedule, never against data — the participation loop's `none` arm:

- **Burn spin-retries the pick.** A pinned core's job is instant availability; it loops — spin hint, retry — and discovers new work by looking, not by being told. Pegged while idle, by design. Stickiness returns it to its own parked assignment, still warm, the moment its layer is runnable again.
- **Job parks as a `Waker`.** It pushes its cached unpark-waker into `idle` under the lock, unlocks, parks; a wake means "look again," so it re-locks, removes its own entry if still present (`will_wake` — spurious parks exist), and re-picks. Sleep is the retry loop with the useless scans removed: nothing changed, so don't look. Nothing travels with a wake, and nobody decides for anyone else — the woken thread runs the pick itself. Task, later, parks its executor waker in the same list.

Liveness, without timers, in three claims: (1) runnability is created only by flushes and retires, and the no-held-output rule makes "a flusher's own visit strictly follows its flushes" true by construction — the visit takes newly runnable work itself, wakes an idler for it, or posts `Halt` when no idler exists; (2) the push-waker-then-park sequence cannot lose a wake — pushes and pops happen under the one mutex, and the park token makes wake-before-park return immediately; (3) Burn cannot miss a wake because it never sleeps — polling the schedule observes everything, bounded by its own retry. There is no fourth case: flush is `try_send`, so no worker ever waits inside kanal — the captive blocked `send` (and the filler-escape argument that kept it benign) is deleted with the per-item send itself.

Demand with nothing pickable gets the active path, from the one place that has just proven it — the participation loop's none-arm: it posts `Halt` on the nearest manned layer up the just-parked layer's dry chain — that is where the wanted items sit, in a mid-round worker's local buffer — walking past unmanned layers; dry layers each post one hop up as their own parks conclude, so chains compose. No manned layer on the path (everything in flight elsewhere) ⇒ post on the highest-pass manned layer: the least-deficit work donates. The claimant answers within one item — flush, visit, one fair pick — and its flush plus the wake duty feed and wake the poster. Healthy inter-batch starves post nothing: while anything is pickable the loop picks it instead, so `budget.miss()` fires only on true trapped demand — which is exactly what lets `budget` converge to the largest round that never causes a stall.

The first draft's busy-blindness limitation is closed, not accepted: the regular yield visit bounds every worker's stretch between check-ins by one round plus the batch it finishes, so a runnable-unmanned layer waits at most that of some worker — and concrete demand cuts inside the round via `Halt`. The parked hook (a runnable-unmanned check at round boundaries) stopped being a hook; it is the loop.

## Tiers

`drive` is tier-blind; a tier is exactly four properties:

| | Burn | Job | Task (deferred) |
|---|---|---|---|
| placement | pinned core | OS-scheduled | compio executor |
| affinity | sticky while runnable (a claimed `Halt` forces one fair pick) | fair: re-picks at every visit | fair |
| idle, nothing runnable | spin-retry the pick | park as a `Waker` in the idle list | park its waker in the same list |
| compatible layers | `Synchronous` | `Synchronous` | `Asynchronous` |

That is the intended division of labor: Burn buys instant availability and cache-warm stickiness for latency-critical stages; Job costs nothing while idle and degrades gracefully under oversubscription; Task, when the async chunk lands, runs IO-bound layers at high concurrency on few threads, picking from the same Schedule under the same policy.

## Coordination

The split is strict. The runtime owns batch take, flush and remainder, every boundary decision, `budget` and `pass` accounting, EOF observation, the `finish` call, and retire; apply code is pure data plane, and no scheduling verb is reachable from inside it. Everything coordination-shaped lands on four pieces, three of which already exist:

- **Shared per-layer state rides `Clone`.** `Arc` fields are layer-shared — the mint's template clone makes every assignment alias them; plain fields are per-assignment scratch. Shared cursors (claimed in chunks to amortize the lock), accumulators, dedup sets: one pattern, no runtime surface.
- **`Gather` is the input-face seam.** Fan-in coordination is a `Gather` impl; zip's shared row staging is the standing example.
- **`finish` is the finalize moment.** `Apply`/`ApplyAsync` gain one defaulted method — `fn finish(&mut self, out: &mut Emit<..>) -> Result<(), Error>` — called exactly once per assignment at its own `Closed`, before retire, emissions riding the normal flush and flow control. It exists for the one thing nothing else provides: emitting a computed result after the last item (aggregations, buffered-output drains, sort-then-emit). Finalize is per-assignment; merging partials across assignments is an explicit fold — into the shared `Arc`, or a downstream reduce.
- **`Ordered<V>` is reserved, not built.** The one coordination case that must be runtime-owned: a bounded reorder window's "full" has to become a block boundary — author-side it is either a captive wait (the class this spec deletes) or unbounded memory. Reserved shape, so the room stays open: an opt-in output marker (`type Output = Ordered<V>`; plain outputs are the unspoken default at zero cost), keyed `push(seq, v)` plus `skip(seq)` for filtered ordinals over a dense sequence, release fused with flush so send order is sequence order at any width, window-full ⇒ `full()` ⇒ block boundary, tail drained at finalize, a hole at EOF an error. The mechanism sketch lives on main (`utils/channel/ordered.rs`, 9bfb75d): a dense-index ring — atomic `base` watermark, slot flags over `MaybeUninit` cells — with out-of-window sends spilling to a side queue; under this spec the spill becomes the bounded `full()` backpressure and the receiver's spin-park dissolves into the runtime's boundaries. `Emit` already owns flush and specializes on the output marker; `Connect` sees the marker at wire-up and builds the window into the edge. Release-policy variants stay unspecified until implementation.

Rejected in the 2026-07-14 review, recorded so they stay rejected: per-layer width caps (the capless pick self-limits — threads queueing at a serialized bottleneck had nothing else runnable, and min-pass hands each finisher the downstream work); `before`/`after` batch hooks (every candidate reduced to a constructor, a first-item branch, or per-item shared state; only finalize survived); a batch-granular authoring trait between the loop and `Apply` (nothing requires it, and a blanket impl over `Apply` keeps it addable later without breakage); pluggable edge cores behind a `Flush` trait (coordination is author code over the seams above, not runtime structure).

## Lifecycle

- **Start**: assemble builds mints and probes per layer exactly as today; the runtime wraps them in the shared `Schedule` and sends one participation job per pool thread — the only closure the pool inboxes ever carry. First pickers find sources runnable. Nothing is kicked, spawned, or seeded.
- **EOF / finalize / join / retire**: `finish` runs once per assignment at its own end of input, and a worker returns `Finished` only clean — a refused final flush returns `Blocked`, and the resume retries it. The first `Finished` drops the layer's mint: no new worker will run. The layer then joins on its workers: queued assignments stay pickable exactly to drain — walked to their own `Closed`, flush retries — and the return that leaves `workers == 0` with both queues empty completes the join and retires the layer, where assignments die, so no corpse is ever parked. Retire drops the `Layer` entry — probe and whatever remains — kanal closes, downstream drains-then-EOFs, the wake duty rouses idlers for the cascade, and the waiter is woken to re-check. Participation ends when every layer has joined.
- **Errors**: first error wins the `RuntimeInner` slot (transport only, per the error-taxonomy note); the failing layer retires immediately — queued siblings drop unfinalized, `finish` never runs on a failure path (error is abort semantics, finalize is completion semantics) — while layers merely *orphaned* by a downstream failure still run `finish` on their way out: the emissions land in a closed edge and are discarded with one `warn!` per layer, intended; the cascade proceeds by ownership; `join` returns `Err`. Panics take the same path through the participation loop's `catch_unwind`.
- **Shutdown / join**: `Runner::join` parks on the `waiter` slot until the sink's entry is gone — any retire takes-and-wakes the waiter, the joiner re-checks `layers[sink]` under the lock and re-parks unless it was the sink (at most one spurious wake per layer, and retire needs no sink-index plumbing) — then triggers shutdown: closers close all edges, post `Halt` on every layer (busy workers stop within one item, not one round), and the idle list is drained and woken (idlers wake into the all-retired exit; idle Burn threads see it at their next retry). Participation jobs return; threads go back to the pool's job wait; the Runner's reference remains the last strong `RuntimeInner`, so thread teardown still happens on the caller's thread.

## Deletions and retentions

Deleted: the scheduler thread and event loop; the events channel, `Event` emission, `Port::petition`; every promote path (`Upstream::promote`, `Downstream::promote`, the promote arm inside `Gather::try_recv`, `Emit`'s spin petition) and the push-side `hit()` — `push` becomes a buffer append, and the only channel ops left are the round's batch `recv` and `try_send`; the pressure-argmax `needy`/`victim`/`shed` machinery; the wait ladder in `next()`, the output spin, `Gather::recv_timeout`, and `patience` with its consts (its only consumer was the park length); per-worker spawn dispatch, the Pool shelves, and the completions channel; `Worker`'s atomics (state, activity, preempt) and `Load` accounting; the port registry. `port.pressure` loses its last consumer and goes with the event plane — flagged explicitly since the rename was recent. With ports carrying nothing, `Port` dissolves into the layer index and `Edge` retains only the weak channel halves for shutdown closers. Camping and the `epoch` counter go too — Burn idles by retrying the pick, and targeted `Halt` covers what defection did — as does `pass`-as-shared-atomic: a plain field now, every credit site holding the lock. The 2026-07-15 review deletes three more: the `Scheduler` trait with `Runtime<S>` and `with_scheduler` (policy is load-bearing across visits, stays, and victim choice — a pick-only plug point was a dishonest seam; the pick is one function to edit), the idle condvar (a condvar is a park-list std maintains; the scheduler holds the list itself as `Waker`s, with deterministic pop-one targeting), and the `down` counter (all-retired is a sixteen-`Option` scan under a lock already held).

Retained: `Tier`, `Action` as protocol vocabulary (unused is not dead); `State`, graduated to mechanism — the worker's return status, filed by the participation loop; `Preempt`, graduated to mechanism — per-layer, `Halt` posted by demand and shutdown, `Yield` the regular visit's name, never posted; `budget` as the round and accounting quantum (still a `utils::Patience` — hit at the regular yield, miss at a `Halt` claim); `utils::Pressure` dormant, tested, available to future admission/learn work; the `Gather` trait reduced to `try_recv` and `starved`, adopting kanal's own convention — `Ok(None)` is starved, `Err` is closed. Flagged loudly: the trait on disk means `Ok(None)` as EOF, and a silent mistranslation in the port swaps starvation for termination. Added: `Apply::finish` (defaulted no-op), the per-assignment emit buffer and gather pending queue, batch-denominated per-edge depth; the `blocked`/`parked` queues with the `layer()`/`finished()`/`residue()` accessors on `Assignment`; the idle waker list, the waiter slot, and the per-thread unpark-waker (`std::task::Wake` over `Thread::unpark`). `WATERMARK` is deleted with the credits protocol it scaffolded — demand's jobs all landed elsewhere (probes, frozen `pass`, `Halt`).

## Verification

The existing evidence loop is acceptance: the full cargo suite; the tiers stall-runner (repeated timed runs, `sample` any hang — expected: zero stalls, and no scheduler thread left to wedge); burn-vs-jobs throughput (expected: Burn ≥ Jobs again — stickiness and parked assignments restore warm residency, both configs shed the event plane entirely); bursty's square wave (stalls at near-zero CPU in job-tier configs — idle Burn spinners are pegged cores by design — with idlers parked, bursts fanning out through repicks); debug logging moves to the Schedule's visits — assignments, parks, retires, posts — replacing spawn/shed counts.

New canaries for this revision: a single-worker pool over a three-layer pipeline (everything flows through one worker's switching — the cheapest liveness proof there is); a flat-map whose per-apply fan-out exceeds the budget (overshoot, remainder, no captive wait); stranded-partial detection (starve-park a producer mid-batch, assert downstream item totals — the no-held-output rule); zip alignment under uneven member batch boundaries; finalize e2e (an aggregating layer emits exactly one result per assignment — width-1 exactly once; `Failed` emits none); the demand post (a starved sink under a mid-round producer is fed within one item — the `Halt` claim path, and the victim's budget shrinks); stranded residue (block-park a producer mid-batch, drain its consumer, assert the tail arrives with no new input — the `blocked` queue counting as input in the probe); the double-EOF race (two workers observe `Closed` together — every assignment finalizes exactly once, retire fires exactly once, `join` returns).
