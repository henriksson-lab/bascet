# Scheduling Feedback Design

## Goal

Worker promotion and demotion should be driven by cold-path evidence without flooding the scheduler petition channel. The scheduler owns scheduling policy at the logical group level. Workers and channels only report compact signals that the group is becoming unhealthy.

The scheduler should tune future sensitivity from group-level feedback:

- `Eager`: added capacity was not useful.
- `Late`: pressure repeated while the group already had extra capacity.
- `Stable`: added capacity did useful work; no tuning change.

## Current Problem

`Petition::Promote` exists, and the scheduler has promotion handlers, but worker cold paths do not currently emit promotion requests.

Naively sending `Promote` from every cold-path miss is wrong. Cold paths can loop while the same unhealthy condition remains true, which would enqueue duplicate scheduler requests. The signal must represent a higher pressure band before it reaches `petition_tx`.

Send blocking is also directional. A full output channel is usually evidence that the downstream consumer group is saturated, not that the sender group needs more workers.

## Principles

- Promotion is a group-level operation. Keep the petition name `Promote`; group scope is implied by scheduler handling.
- Do not add per-worker one-off latches for promotion dedupe.
- Do not use wall-clock settling windows.
- Do not add scheduler polling or spinning.
- Only cold paths should pay for signal evaluation.
- Sources are out of scope for this change. Start with stage groups.
- New scheduling sensors should report pressure-band transitions from accumulated evidence, not continuous repeated saturation of the same band.
- Do not gate promotion with scheduler-side "already promoted" latches. Deduplication belongs to the pressure semantics.

## Terminology

`Group` is one logical source or stage registered with the scheduler.

`Worker` is one live executor for a group.

`Lease` is a transient scheduler allocation/release value. It does not live on `Worker`.

`Leases` is the group-owned tier/resource index for live workers.

`Pressure` is the scheduling sensor primitive. It accumulates unhealthy cold-path evidence and emits a signal only when pressure crosses into a higher pressure band.

`Demand` is the group-local use of `Pressure` for accepted downstream demand that cannot yet be satisfied by upstream output.

Channel pressure is the channel-edge use of `Pressure` for send blocking. It belongs to the consumer group's input edge, not to a worker.

Sensors expose tunable parameters. The scheduler owns feedback classification and decides how to tune those parameters.

`Feedback` is the scheduler's classification of recent scheduling quality:

```rust
enum Feedback {
    Eager,
    Late,
    Stable,
}
```

`Motivation` identifies the signal source:

```rust
enum Motivation {
    Demand,
    Pressure,
}
```

## Petitions

Keep the promotion action named `Promote`, but make it group scoped:

```rust
enum Petition {
    Register {
        mode: Mode,
        strategy: Strategy,
        spawn: Box<dyn Spawn>,
        pressure: Arc<Pressure>,
    },
    Demote {
        group_idx: usize,
    },
    Retire {
        group_idx: usize,
        id: Id,
        processed: u64,
    },
    Release {
        group_idx: usize,
        id: Id,
        processed: u64,
    },
    Promote {
        group_idx: usize,
        motivation: Motivation,
    },
}
```

`Promote` remains the action name. The payload makes the group explicit for routing, but the scheduler treats promotion as a group operation.

`Demote` is a group-level scheduling request. It means the group has idle surplus and the scheduler should choose the worker to remove.

`Retire` and `Release` are terminal worker events. `Retire` means the worker exits while it still owns an active lease; the scheduler should return that lease to the resource pool. `Release` means the worker exits after a scheduler-requested demotion; the lease was already reclaimed or reassigned when demotion began, so the terminal event only completes worker accounting and feedback.

`processed` is terminal worker accounting. It lets the scheduler classify unused added capacity without adding per-promotion probe state. A worker can maintain this as a local counter or local boolean-derived count and include it only on terminal petitions.

Worker scheduling targets mirror the terminal distinction:

```rust
enum Decision {
    Retire,
    Release,
}
```

`Decision::Retire` asks a worker to exit and return its active lease through `Petition::Retire`. `Decision::Release` asks a worker to exit after the scheduler has already reclaimed its lease; it responds with `Petition::Release`.

## Pressure Sensor

`Pressure` should not wrap `AtomicPatience`. It is an unhealthy-evidence signal with its own semantics.

Use one atomic pressure counter and one atomic emitted level:

```rust
pub struct Pressure {
    pressure: AtomicU32,
    level: AtomicU32,
    growth: AtomicU32,
    decay: AtomicU32,
    strain: NonZeroU32,
    min: u32,
}
```

`strain` is the pressure required for band 1. `min` is the recovery floor after decay. `growth` and `decay` are the tunable pressure semantics. `Pressure` does not know about `Feedback`; the scheduler maps feedback into changes to these fields.

The same primitive backs both group-local demand pressure and channel-edge pressure. The difference is ownership and routing, not counter behavior.

Channel-edge pressure is owned by the consumer group's input edge. Sender-side clones of that edge must retain the consumer `group_idx` or equivalent routing metadata. A saturated output send promotes the consumer group, not the sender group.

Producer-side send blocking records pressure. `miss` returns `true` only when this event moves the sensor into a higher pressure band, so repeated signals require increasing pressure rather than repeated crossing of the same strain point.

```rust
impl Pressure {
    pub fn miss(&self) -> bool {
        let strain = self.strain.get();
        let growth = self.growth.load(Ordering::Relaxed);
        let mut next = self.min;

        self.pressure
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                next = old.saturating_add(growth);
                Some(next)
            })
            .ok();

        let next_level = band(next, strain);

        loop {
            let level = self.level.load(Ordering::Acquire);
            if next_level <= level {
                return false;
            }

            if self
                .level
                .compare_exchange(level, next_level, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn hit(&self) {
        let decay = self.decay.load(Ordering::Relaxed);
        let strain = self.strain.get();
        let mut next = self.min;

        self.pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                next = old.saturating_sub(decay).max(self.min);
                Some(next)
            })
            .ok();

        self.lower(band(next, strain));
    }

    fn lower(&self, target: u32) {
        self.level
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (target < old).then_some(target)
            })
            .ok();
    }
}

fn band(pressure: u32, strain: u32) -> u32 {
    if pressure < strain {
        0
    } else {
        1 + (pressure / strain).ilog2()
    }
}
```

This is pressure-level escalation. With `strain = 1024`, the first signal fires at pressure `1024`, the second at `2048`, the third at `4096`, and so on. Continued blocking can still request more capacity, but each additional request needs a higher pressure band. There is no boolean latch and no scheduler-side outstanding-promotion gate.

When `miss` returns `true`, the scheduler receives:

```rust
Petition::Promote {
    group_idx: consumer_group_idx,
    motivation: Motivation::Pressure,
}
```

This keeps send blocking symmetrical with demand blocking while still promoting the group associated with the pressure band transition.

## Demand

`Demand` is the group-local pressure source for cold demand misses after real demand exists. Current success-path behavior can stay cheap.

`Motivation::Demand` means a worker has accepted downstream demand and is now waiting for upstream output. It is not the idle "no request yet" case.

The worker cold path records demand pressure only after it has accepted real downstream demand. Idle "no request yet" paths still standby or request group demotion.

Expected usage:

```rust
if w.demand.miss() {
    w.petition_tx
        .send(Petition::Promote {
            group_idx: w.group_idx,
            motivation: Motivation::Demand,
        })
        .ok();
}
```

Do not introduce a separate `Demand` sensor type. `demand` is a `Pressure` field whose routing target is the current group.

## Scheduler Sensors

`Group` owns scheduling state for the signals it can tune:

```rust
struct Worker {
    id: Id,
    sched: Arc<Scheduling>,
    decision: Sender<Decision>,
    motivation: Option<Motivation>,
}

struct Leases {
    task: Vec<Id>,
    job: Vec<Id>,
    burn: Vec<(Id, CoreId)>,
    releasing: Vec<(Id, Lease)>,
}

struct Group {
    mode: Mode,
    strategy: Strategy,
    spawn: Box<dyn Spawn>,
    countof_active: Arc<AtomicU32>,
    leases: Leases,
    workers: Vec<Worker>,
    // Pressure from accepted demand that cannot yet be satisfied.
    demand: Arc<Pressure>,
    // Pressure for the group's input edge.
    pressure: Arc<Pressure>,
}
```

`Worker` intentionally has no `lease` field. Worker records are runtime/control records. Tier ownership and occupied resources belong to `Group::leases`.

`task` and `job` store worker ids even though those tiers do not occupy a specific resource. `burn` stores `(Id, CoreId)` because the scheduler must release the occupied core without reading worker state.

`Leases` is scheduler-owned state, so it does not need atomics. `countof_active` remains the worker-visible mirror used by scheduling decisions outside the scheduler loop; it is updated when `Leases` changes.

Expected helpers:

```rust
impl Leases {
    fn add(&mut self, id: Id, lease: Lease);
    fn take(&mut self, id: Id) -> Option<Lease>;
    fn demote(&mut self, id: Id) -> Option<Lease>;
    fn release(&mut self, id: Id) -> Option<Lease>;
    fn len(&self) -> usize;
    fn weakest(&self) -> Option<Strategy>;
}
```

`add` records a new active lease. `take` removes an active lease and returns it for normal retirement. `demote` moves an active lease into `releasing` and returns the lease immediately so the scheduler can free or reassign the resource with eventual consistency. `release` removes the releasing record after the worker acknowledges `Decision::Release`; it does not free the resource again. `len` counts active leases, not releasing workers. `weakest` returns `Task`, then `Job`, then `Burn` based on active occupied tiers.

`Group` exposes scheduling quantities rather than one-off boolean gates:

```rust
impl Group {
    fn active(&self) -> usize {
        self.leases.len()
    }

    fn headroom(&self) -> usize {
        if self.manual() {
            0
        } else {
            (self.mode.countof_max().get() as usize).saturating_sub(self.active())
        }
    }

    fn surplus(&self) -> usize {
        if self.manual() {
            0
        } else {
            self.active()
                .saturating_sub(self.mode.countof_min().get() as usize)
        }
    }
}
```

`headroom` is promotion capacity below `countof_max`. `surplus` is demotion capacity above `countof_min`.

`demand` is not idle waiting. It is the group-level pressure sensor for accepted demand that cannot yet be satisfied by upstream output.

Feedback is scheduler-level policy. Sensors do not expose feedback methods. The scheduler learns from terminal worker events by tuning the relevant underlying parameters:

```rust
fn learn(&mut self, motivation: Motivation, feedback: Feedback) {
    match (motivation, feedback) {
        (Motivation::Pressure, Feedback::Eager) => {
            scale(&self.pressure.growth, DOWN, PRESSURE_GROWTH_MIN, PRESSURE_GROWTH_MAX);
            scale(&self.pressure.decay, UP, PRESSURE_DECAY_MIN, PRESSURE_DECAY_MAX);
        }
        (Motivation::Pressure, Feedback::Late) => {
            scale(&self.pressure.growth, UP, PRESSURE_GROWTH_MIN, PRESSURE_GROWTH_MAX);
            scale(&self.pressure.decay, DOWN, PRESSURE_DECAY_MIN, PRESSURE_DECAY_MAX);
        }
        (Motivation::Demand, Feedback::Eager) => {
            scale(&self.demand.growth, DOWN, DEMAND_GROWTH_MIN, DEMAND_GROWTH_MAX);
            scale(&self.demand.decay, UP, DEMAND_DECAY_MIN, DEMAND_DECAY_MAX);
        }
        (Motivation::Demand, Feedback::Late) => {
            scale(&self.demand.growth, UP, DEMAND_GROWTH_MIN, DEMAND_GROWTH_MAX);
            scale(&self.demand.decay, DOWN, DEMAND_DECAY_MIN, DEMAND_DECAY_MAX);
        }
        (_, Feedback::Stable) => {}
    }
}

const UP: (u32, u32) = (11, 10);
const DOWN: (u32, u32) = (10, 11);

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
```

`UP` and `DOWN` are scheduler sensitivity constants. The example uses a 10% multiplicative adjustment. They can be scheduler defaults or builder-configured constants, but they are not extra sensor state.

For `Pressure`, `Late` makes blocked sends accumulate faster and decay slower. `Eager` makes pressure accumulate slower and decay faster.

For `Demand`, `Late` makes accepted-demand misses accumulate faster and decay slower. `Eager` makes accepted-demand misses accumulate slower and decay faster.

`Stable` does not change sensitivity.

## Feedback Classification

Feedback should come from regular scheduler events and group accounting. Do not add wall-clock windows or probe-specific fields.

### Late

A `Promote` signal arrives while the group already has extra capacity:

```rust
let late = group.active() > group.mode.countof_min().get() as usize;
if late {
    group.learn(motivation, Feedback::Late);
}
```

This means prior added capacity has not absorbed the unhealthy condition.

### Eager

A worker retires or releases without processing any work:

```rust
if processed == 0 && let Some(motivation) = worker.motivation {
    group.learn(motivation, Feedback::Eager);
}
```

This means capacity existed but was unused. The signal was likely too sensitive.

If the scheduler cannot associate the worker with a specific `Motivation`, do not tune a sensor from that terminal event.

### Stable

A worker retires or releases after processing work:

```rust
if processed > 0 && let Some(motivation) = worker.motivation {
    group.learn(motivation, Feedback::Stable);
}
```

This means capacity was useful enough not to be classified as eager. `Stable` is intentionally a no-op; it records that no retune is needed.

### Blocked Scheduler Action

If `Promote` cannot spawn because `group.headroom() == 0` or no execution slot/victim exists, do not adjust sensitivity. Lack of capacity is not evidence that the sensor was wrong.

The pressure sensor has already paid the band cost, so repeated blocked attempts naturally require enough cold-path evidence to reach a higher pressure band before another petition.

## Promotion And Demotion Semantics

Promotion should grow the group rather than replace the reporting worker.

The reporting worker may hold an in-flight `Pull`; removing it risks dropping demand or requiring requeue logic. Group-level promotion adds another worker. The target tier is computed from the weakest currently occupied tier:

```rust
let target = match group.leases.weakest().unwrap_or(group.strategy) {
    Strategy::Task => Strategy::Job,
    Strategy::Job => Strategy::Burn,
    Strategy::Burn => Strategy::Burn,
};
```

If the group has no workers yet, `group.strategy` is the seed tier. Otherwise, `group.leases` is the source of truth. This keeps promotion based on current group composition rather than the original registration strategy.

Promotion strengthens the weakest occupied tier. A group that already has `Burn` workers but still has weaker workers should first lift the weak side.

Spawn helpers may fall back when the target resource is unavailable, but `target` is the policy input and should be named as such.

Demotion selects from the weakest occupied tier first:

```rust
if group.surplus() == 0 {
    return;
}

let tier = group.leases.weakest();
```

Within that tier, resolve the leased ids to `Worker` records and choose the laziest worker by `worker.sched.countof_idle`. This keeps scale-down pressure on low-value workers before releasing stronger resources.

There are three different scheduler lifecycle actions:

- `Demote`: group-level request to remove idle surplus. The scheduler chooses weakest tier, then laziest worker.
- `Retire`: terminal worker event that returns an active lease to the resource pool.
- `Release`: terminal worker event after scheduler demotion. The lease was already reclaimed or reassigned, so this only removes the worker record and applies feedback.

`Mode::Manual` still blocks auto-promotion.

`countof_max` remains the hard upper bound.

## Stage Cold Path Wiring

No request yet:

```rust
req_rx.try_recv() == Ok(None)
```

This is idle. Keep standby/group-demotion behavior. Do not promote.

If idle patience drains while the group has surplus, send `Petition::Demote { group_idx }` and stop the repeated idle miss loop until either work arrives or the scheduler sends a decision. This avoids a worker-local "sent once" latch by moving the worker into a parked state after the cold-path signal.

Request accepted, upstream result unavailable:

```rust
res_rx.try_recv() == Ok(None)
```

This is unmet downstream demand. Record demand pressure. If demand moves into a higher pressure band, send `Promote { group_idx, motivation: Demand }`.

Output send would block:

```rust
res_tx.try_send_option(&mut opt) == Ok(false)
```

This is downstream pressure. Record it with `res_tx.pressure().miss()`. If it returns `true`, send `Promote { group_idx, motivation: Pressure }` for the consumer group that owns that input-edge pressure sensor.

Fast output send:

```rust
res_tx.try_send_option(&mut opt) == Ok(true)
```

Call `res_tx.pressure().hit()` to decay pressure.

## Performance Notes

- Successful `try_recv` / `try_send_option` paths should stay branch-light.
- Pressure `miss` is cold because it only happens when send would block or accepted demand waits on upstream output.
- Sensor feedback runs only while handling existing scheduler petitions.
- No `Instant::now()`.
- No scheduler scan loop.
- No additional worker-shared atomics for "worked"; use local accounting and terminal petition payloads.

## Implementation Order

1. Introduce `Pressure` as the scheduling sensor primitive with `pressure`, `level`, `min`, `strain`, `growth`, and `decay`.
2. Replace the current channel pressure `Arc<AtomicPatience<AtomicU32>>` with `Arc<Pressure>`.
3. Rename pressure base-band constants from `*_MAX` to `*_STRAIN`; keep `*_GROWTH_MAX` and `*_DECAY_MAX` as tuning bounds.
4. Add group-owned `demand: Arc<Pressure>`, input-edge `pressure: Arc<Pressure>`, and `Feedback`.
5. Change `Petition::Register` to carry the group's input-edge pressure.
6. Change `Petition::Promote` to carry `group_idx` and `Motivation`.
7. Remove scheduler-side outstanding-promotion gates such as `Group::has_promotion`; escalation is handled by pressure bands.
8. Split worker scheduling targets into `Decision::Retire` and `Decision::Release`.
9. Split worker terminal petitions into `Retire { group_idx, id, processed }` and `Release { group_idx, id, processed }`.
10. Add group-level `Petition::Demote { group_idx }`.
11. Add `Group::leases` and remove `lease` from `Worker`.
12. Maintain `Leases` in spawn, retire, demote, release, and eviction paths with `add`, `take`, `demote`, `release`, `len`, and `weakest`.
13. Add `Group::active`, `Group::headroom`, and `Group::surplus`; use `headroom` for promotion and `surplus` for demotion/victim eligibility.
14. Change scheduler promotion to group-growth semantics using `group.leases.weakest()` and a local `target`.
15. Change demotion and scale-down selection to choose the weakest occupied tier, then the laziest worker in that tier.
16. Wire stage accepted-demand misses to `Promote { motivation: Demand }`.
17. Wire idle surplus cold paths to `Demote { group_idx }` and park rather than repeatedly enqueueing demotion requests.
18. Wire send blocking to `Pressure::miss` and fast sends to `Pressure::hit`.
19. Apply feedback in scheduler event handling through `Group::learn`.

No tests are added as part of this spec because the current instruction is production/spec work only.
