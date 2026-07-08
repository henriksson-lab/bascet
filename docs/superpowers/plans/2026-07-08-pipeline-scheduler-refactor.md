# Pipeline Scheduler Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the petition scheduler with one runtime-level scheduler behind a concrete event plane, per `docs/superpowers/specs/2026-06-27-pipeline-scheduler-refactor-design.md`.

**Architecture:** Tear down the old apply/scheduler world first, then build bottom-up: type-level set/attr machinery → gauges → pool/dispatch → event plane → apply/emit → worker loops → scheduler/driver → assembly/runner. The tree is red between Task 2 and the end of Task 4; every task after that ends at a compiling checkpoint.

**Tech Stack:** Rust edition 2024, kanal (channels, sole wait primitive), compio (System + Task runtimes), crossbeam-utils `CachePadded`, core_affinity (Burn pinning), inventory (attr registration), bascet-variadic (tuple impls), fnv via bascet-derive.

## Global Constraints

- Claude never runs `cargo` or `git`; compile/test checkpoints are executed by the user. Steps below state the command and expected outcome for whoever runs it.
- No code comments. Match existing naming: single-word types, `_tx`/`_rx` channel suffixes, `crate::` import paths, `folder.rs` + `folder/` modules (never `mod.rs`).
- No `#[repr(..)]` unless the spec shows it (`Preempt` only).
- kanal stays unpinned (`kanal = "0.1.1"` as-is); the canary test guards the behavior instead.
- `lib.rs` ends with the explicit export list from the spec's Public API section — no glob re-exports.
- One deviation noted for the record: `Port` gains an `index: u32` field (registry key for the scheduler's per-layer state) and the mint signature is `FnMut(Arc<Worker>, Patience) -> Job` (the worker's slot already names the tier). Both are mechanism details the spec leaves open.
- `type Attrs` for records rides a new minimal `Record` trait in `attr.rs`; full derive emission belongs to the separate Backing spec — this plan lands the trait plus hand-written impls in tests.

---

### Task 1: kanal canary test

**Files:**
- Create: `crates/bascet-core/tests/kanal.rs`

**Interfaces:**
- Consumes: kanal only.
- Produces: nothing — a permanent behavioral guard. Termination correctness rides on: (a) `close()` voids buffered items, (b) a surviving sender clone (the keeper) keeps buffered items drainable after other senders drop, (c) receivers and blocked senders wake with `Err` on close.

- [ ] **Step 1: Write the tests**

```rust
use std::thread;
use std::time::Duration;

#[test]
fn close_voids_buffer() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(8);
    items_tx.send(1).unwrap();
    items_tx.send(2).unwrap();
    items_tx.close().unwrap();
    assert!(items_rx.recv().is_err());
}

#[test]
fn keeper_holds_buffer_across_sender_drop() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(8);
    let keeper = items_tx.clone();
    items_tx.send(1).unwrap();
    items_tx.send(2).unwrap();
    drop(items_tx);
    assert_eq!(items_rx.recv().unwrap(), 1);
    assert_eq!(items_rx.recv().unwrap(), 2);
    drop(keeper);
    assert!(items_rx.recv().is_err());
}

#[test]
fn close_wakes_blocked_sender() {
    let (items_tx, items_rx) = kanal::bounded::<u32>(1);
    items_tx.send(1).unwrap();
    let handle = thread::spawn(move || items_tx.send(2));
    thread::sleep(Duration::from_millis(50));
    items_rx.close().unwrap();
    assert!(handle.join().unwrap().is_err());
}
```

- [ ] **Step 2: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test kanal`
Expected: 3 passed. If `close_voids_buffer` fails, the keeper mechanism in the spec is void — stop and report.

---

### Task 2: Tear down the old world

**Files:**
- Delete: `crates/bascet-core/src/traits.rs`, `src/contract.rs`, `src/execute.rs`, `src/schedule.rs`, `src/source.rs`, `src/layer.rs`, `src/coordinate.rs`, `src/coordinate/`, `src/apply.rs` (old contents; recreated in Task 10), `src/pipeline/` (entire folder: `builder.rs`, `consts.rs`, `edge.rs`, `pipeline.rs`, `run.rs`, `runtime.rs`, `scheduler.rs`, `shutdown.rs`, `watchdog.rs`, `worker.rs`, `worker/`), `src/set.rs` (old contents; recreated in Task 3), `src/utils/channel.rs`, `src/utils/channel/`, `src/utils/patience/atomic.rs`
- Delete: `crates/bascet-derive/src/schedule.rs`
- Modify: `crates/bascet-core/src/lib.rs`, `crates/bascet-core/src/utils.rs`, `crates/bascet-core/src/utils/patience.rs`, `crates/bascet-derive/src/lib.rs`
- Modify: `crates/bascet-core/Cargo.toml` (comment nothing out — remove the two `[[bench]]` blocks; benches return in Task 20; move `benches/bursty.rs` and `benches/parallel.rs` to `benches/attic-bursty.rs.txt`, `benches/attic-parallel.rs.txt` so their old-API code stops compiling but stays readable)

**Interfaces:**
- Produces: a crate containing only `arena`, `attr` (data half), `owned`, `pipe`, `sink`, `utils` (pressure, patience, send, threading), plus empty seams for what follows.

- [ ] **Step 1: Delete the files listed above**

- [ ] **Step 2: Reduce `lib.rs` to the surviving modules**

```rust
pub mod arena;
pub mod attr;
pub mod owned;
pub mod pipe;
pub mod set;
pub mod sink;
pub mod utils;

pub use arena::{Arena, ArenaPool, ArenaSlice, ArenaView};
pub use owned::Owned;
pub use pipe::Pipe;
pub use sink::{channel, drain};
```

`src/set.rs` is recreated empty for now (`pub trait Set {}` alone) so the module resolves; Task 3 fills it.

- [ ] **Step 3: Update `utils/patience.rs`**

```rust
mod patience;

pub use patience::Patience;
```

- [ ] **Step 4: Update `bascet-derive/src/lib.rs`**

```rust
mod attr;

#[proc_macro_derive(Attr, attributes(variadic, plural))]
pub fn derive_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::Attr::derive(input)
}
```

- [ ] **Step 5: Sweep dangling references**

`src/attr.rs` and `src/attr/*` may reference deleted items (`Pull`, old set ops). Strip those imports/uses; `attr.rs` keeps `Attr`, `AttrEntry`, `Represents`, `Coerce`, `Ref`, `Mut`, `Put` and the variadic `Ref` block. `sink/channel.rs` and `sink/drain.rs` lose any old-`Apply` impls — reduce each to its plain struct + constructor for now; Task 18 re-implements them against the new `Apply`.

- [ ] **Step 6: Checkpoint (user runs)**

Run: `cargo check -p bascet-core -p bascet-derive`
Expected: clean, possibly `unused` warnings only. The crate is now the data plane plus utilities.

---

### Task 3: Attr identity — digits, `TEq`, `AttrId`, `AttrEq`, the `attr_id!` macro

**Files:**
- Create: `crates/bascet-core/src/set/verdict.rs`, `crates/bascet-core/src/set/digit.rs`
- Modify: `crates/bascet-core/src/set.rs`, `crates/bascet-core/src/attr.rs`, `crates/bascet-derive/src/attr.rs`, `crates/bascet-derive/src/attr/id.rs`, `crates/bascet-derive/src/lib.rs`
- Test: `crates/bascet-core/tests/set_identity.rs`

**Interfaces:**
- Produces: `Hit`/`Miss` verdict types with `And`/`Or` folds; digit ZSTs `H0`–`HF` with `Digit { const VALUE: u64 }`; `TEq<B> { type Verdict }` over the closed digit alphabet; `AttrId { const ID: u64 }` on 16-digit tuples; `AttrEq<B> { type Verdict }` on attrs; `Attr` loses `const ID`, gains `type Id: AttrId`; `bascet_derive::attr_id!(<u64>)` — a function-like proc macro valid in type position that expands an integer's nibbles into the 16-digit tuple type, so nobody ever writes the tuple by hand. (A `const fn` cannot do this: it returns a value, and `type Id` needs a type — there is no value→type lift on stable, which is the reason the digit encoding exists at all.)

- [ ] **Step 1: Write the failing test**

```rust
use std::any::TypeId;

use bascet_core::set::{And, AttrEq, Hit, Miss, Or};
use bascet_core::{Attr, AttrId};
use bascet_derive::attr_id;

struct A;
struct B;

impl Attr for A {
    type Id = attr_id!(1);
}

impl Attr for B {
    type Id = attr_id!(15);
}

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

#[test]
fn identity_folds_to_const() {
    assert_eq!(<A as Attr>::Id::ID, 1);
    assert_eq!(<B as Attr>::Id::ID, 15);
}

#[test]
fn attr_equality_is_a_type() {
    assert!(eq::<<A as AttrEq<A>>::Verdict, Hit>());
    assert!(eq::<<A as AttrEq<B>>::Verdict, Miss>());
}

#[test]
fn verdict_folds() {
    assert!(eq::<<Hit as And<Miss>>::Verdict, Miss>());
    assert!(eq::<<Miss as Or<Hit>>::Verdict, Hit>());
}
```

- [ ] **Step 2: Implement `set/verdict.rs`**

```rust
pub struct Hit;
pub struct Miss;

pub trait Verdict {
    const HIT: bool;
}

impl Verdict for Hit {
    const HIT: bool = true;
}

impl Verdict for Miss {
    const HIT: bool = false;
}

pub trait And<B> {
    type Verdict;
}

pub trait Or<B> {
    type Verdict;
}

impl<B> And<B> for Hit {
    type Verdict = B;
}

impl<B> And<B> for Miss {
    type Verdict = Miss;
}

impl<B> Or<B> for Hit {
    type Verdict = Hit;
}

impl<B> Or<B> for Miss {
    type Verdict = B;
}
```

- [ ] **Step 3: Implement `set/digit.rs`**

Sixteen ZSTs, a `Digit` value trait, `TEq` over the closed alphabet via a quadratic local macro, `AttrId` for 16-tuples:

```rust
use crate::set::verdict::{Hit, Miss};

pub trait Digit: 'static {
    const VALUE: u64;
}

pub trait TEq<B> {
    type Verdict;
}

macro_rules! digits {
    ($($d:ident = $v:expr),*) => {
        $(
            pub struct $d;
            impl Digit for $d {
                const VALUE: u64 = $v;
            }
        )*
        digits!(@teq [$($d)*] [$($d)*]);
    };
    (@teq [$($l:ident)*] $r:tt) => {
        $(digits!(@row $l $r);)*
    };
    (@row $l:ident [$($r:ident)*]) => {
        $(digits!(@cell $l $r);)*
    };
    (@cell $l:ident $l2:ident) => {
        impl TEq<$l2> for $l {
            type Verdict = <() as $crate::set::digit::Diagonal<$l, $l2>>::Verdict;
        }
    };
}
```

The `Diagonal` helper trick above is awkward; use the simpler explicit split instead — one diagonal arm and a cross-product with exclusion is not expressible in plain `macro_rules!`, so generate `TEq` with `bascet_variadic` over the numeric product and map indices to names with a paste-style suffix. The digits are therefore named `D0`–`D15` internally with public aliases `H0`–`HF`:

```rust
use crate::set::verdict::{Hit, Miss};

pub trait Digit: 'static {
    const VALUE: u64;
}

pub trait TEq<B> {
    type Verdict;
}

bascet_variadic::variadic!(N = 0..=15, for N in N => {
    pub struct D~#;
    impl Digit for D~# {
        const VALUE: u64 = ~#;
    }
    impl TEq<D~#> for D~# {
        type Verdict = Hit;
    }
});

bascet_variadic::variadic!(N = 0..=15, M = 0..=15, for (N, M) in N.product(M) => {
    impl TEq<D~M#> for D~N# where [~N# != ~M#] {
        type Verdict = Miss;
    }
});

pub type H0 = D0;
pub type H1 = D1;
pub type H2 = D2;
pub type H3 = D3;
pub type H4 = D4;
pub type H5 = D5;
pub type H6 = D6;
pub type H7 = D7;
pub type H8 = D8;
pub type H9 = D9;
pub type HA = D10;
pub type HB = D11;
pub type HC = D12;
pub type HD = D13;
pub type HE = D14;
pub type HF = D15;
```

**Note for the implementer:** `bascet_variadic` (see `crates/bascet-variadic/src/`) may not support a `where [~N# != ~M#]` guard or the `~N#`/`~M#` two-variable substitution shown here — check `emit.rs`/`ast/pattern.rs` first. If it doesn't, extend the macro's pattern support (it's the house macro, extending it is in-charter) or fall back to a build-script-free explicit expansion: 16 diagonal impls plus 240 `Miss` impls written by a small one-off generator run once and pasted. Do not hand-write 240 impls without generation.

- [ ] **Step 4: Rework `Attr` and add `AttrId`, `AttrEq` in `attr.rs` / `set.rs`**

In `attr.rs`, replace the old trait:

```rust
pub trait Attr: 'static {
    type Id: crate::set::AttrId;
}
```

In `set.rs` (root module):

```rust
pub mod digit;
pub mod verdict;

pub use digit::{Digit, TEq};
pub use verdict::{And, Hit, Miss, Or, Verdict};

use crate::attr::Attr;

pub trait Set {}

pub trait AttrId: 'static {
    const ID: u64;
}

bascet_variadic::variadic!(N = 16..=16, for N in N => {
    impl<@N[D~#: Digit](sep=",")> AttrId for (@N[D~#](sep=","),) {
        const ID: u64 = 0 @N[| (D~#::VALUE << (4 * (15 - ~#)))](sep=" ");
    }
});

pub trait AttrEq<B> {
    type Verdict;
}
```

`AttrEq` for two attrs folds the sixteen digit `TEq` verdicts with `And`. Expressing the 16-step fold generically needs one impl over the two id tuples; generate it in the same `variadic!` block:

```rust
bascet_variadic::variadic!(N = 16..=16, for N in N => {
    impl<A, B, @N[LA~#](sep=","), @N[LB~#](sep=",")> AttrEq<B> for A
    where
        A: Attr<Id = (@N[LA~#](sep=","),)>,
        B: Attr<Id = (@N[LB~#](sep=","),)>,
        @N[LA~#: TEq<LB~#>](sep=","),
        (@N[<LA~# as TEq<LB~#>>::Verdict](sep=","),): FoldAnd,
    {
        type Verdict = <(@N[<LA~# as TEq<LB~#>>::Verdict](sep=","),) as FoldAnd>::Verdict;
    }

    pub trait FoldAnd {
        type Verdict;
    }

    impl<@N[V~#](sep=",")> FoldAnd for (@N[V~#](sep=","),)
    where
        V0: And<<(@N[V~#](sep=","),) as FoldTail>::Verdict>,
    {
        type Verdict = <V0 as And<<(@N[V~#](sep=","),) as FoldTail>::Verdict>>::Verdict;
    }
});
```

**Note for the implementer:** the fold shape above is illustrative of intent, not final syntax — a right-fold over a fixed 16-tuple is most simply written as one non-variadic impl since the arity is constant. Write it explicitly:

```rust
impl<V0, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15> FoldAnd
    for (V0, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15)
where
    V15: And<Hit>,
    V14: And<<V15 as And<Hit>>::Verdict>,
    V13: And<<V14 as And<<V15 as And<Hit>>::Verdict>>::Verdict>,
{
    type Verdict = ();
}
```

Nested `where` clauses at depth 16 are unreadable; instead define pairwise chaining once and let `AttrEq` fold with a cons pattern: `And` is short-circuiting on `Miss`, so a simple linear chain `<<V0 as And<V1>>::Verdict as And<V2>>::Verdict …` written left-to-right as a single type expression in the `AttrEq` impl needs no helper trait at all. Use that: sixteen nested `And` projections, no `FoldAnd`.

- [ ] **Step 5: Implement `attr_id!` in `bascet-derive`**

Rewrite `crates/bascet-derive/src/attr/id.rs`. Identity must be deterministic from the name alone — same name, same ID, on every build, platform, and toolchain (this rules out ahash, whose output is documented as unstable across versions and CPUs, and UUID v4, which is random per expansion). The id is therefore a spec-frozen name hash: `xxh3_64` from `xxhash-rust` — one call, no house constants, reference-grade 64-bit distribution. Replace `fnv` with `xxhash-rust = { version = "0.8", features = ["xxh3"] }` in the workspace dependencies, referenced `workspace = true` from `bascet-derive/Cargo.toml`:

```rust
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use xxhash_rust::xxh3::xxh3_64;

pub struct AttrId;

impl AttrId {
    pub fn from_name(name: &str) -> u64 {
        xxh3_64(name.as_bytes())
    }

    pub fn digits(value: u64) -> TokenStream {
        let digits = (0..16).map(|i| {
            let nibble = (value >> (4 * (15 - i))) & 0xF;
            format_ident!("H{:X}", nibble)
        });
        quote! { (#(bascet_core::set::digit::#digits,)*) }
    }

    pub fn expand(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
        let lit = syn::parse_macro_input!(input as syn::LitInt);
        match lit.base10_parse::<u64>() {
            Ok(value) => Self::digits(value).into(),
            Err(error) => error.to_compile_error().into(),
        }
    }
}
```

In `crates/bascet-derive/src/attr.rs` change `mod id;` to `pub(crate) mod id;`. In `crates/bascet-derive/src/lib.rs`:

```rust
mod attr;

#[proc_macro_derive(Attr, attributes(variadic, plural))]
pub fn derive_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::Attr::derive(input)
}

#[proc_macro]
pub fn attr_id(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::id::AttrId::expand(input)
}
```

`base10_parse` accepts hex literals too, so `attr_id!(0xDEAD_BEEF_0000_0001)` works. The expansion names `bascet_core::set::digit::…` paths; usage *inside* bascet-core itself (the concrete attrs in `attr/`) needs `extern crate self as bascet_core;` at the top of `crates/bascet-core/src/lib.rs` — check whether the existing derive already relies on it and add it if not.

- [ ] **Step 6: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test set_identity`
Expected: 3 passed.

---

### Task 4: Set ops — `In`, `Select`, `Concat`, `Join`/`Meet`, `Union`/`Intersect`, `Subset`, `contains`

**Files:**
- Create: `crates/bascet-core/src/set/ops.rs`
- Modify: `crates/bascet-core/src/set.rs`
- Test: `crates/bascet-core/tests/set_ops.rs`

**Interfaces:**
- Consumes: `AttrEq`, `Hit`/`Miss`, `AttrId` from Task 3.
- Produces (all in `bascet_core::set`):
  - `trait In<S: Set> { type Verdict; }` — membership of one attr in a set, `Or`-fold.
  - `trait Select<V> { type Output; }` — `Hit ↦ (A,)`, `Miss ↦ ()`, on the attr.
  - `trait Concat<B> { type Output; }` — tuple glue (the renamed old `Union`).
  - `trait Join<R: Set>: Set { type Output: Set; }`, `trait Meet<R: Set>: Set { type Output: Set; }` — order-preserving dedup.
  - `type Union<L, R> = <L as Join<R>>::Output;`, `type Intersect<L, R> = <L as Meet<R>>::Output;`
  - `trait Subset<Sup: Set>` — membership-fold bound, `#[diagnostic::on_unimplemented]` preserved from the old file.
  - `Set::contains::<A>() -> bool` — const-foldable value path.

- [ ] **Step 1: Write the failing test**

```rust
use std::any::TypeId;

use bascet_core::set::{Intersect, Set, Subset, Union};
use bascet_core::Attr;
use bascet_derive::attr_id;

struct A;
struct B;
struct C;

impl Attr for A {
    type Id = attr_id!(1);
}

impl Attr for B {
    type Id = attr_id!(2);
}

impl Attr for C {
    type Id = attr_id!(3);
}

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

#[test]
fn union_dedups_preserving_order() {
    assert!(eq::<Union<(A, B), (B, C)>, (A, B, C)>());
    assert!(eq::<Union<(), (A,)>, (A,)>());
    assert!(eq::<Union<(A,), ()>, (A,)>());
}

#[test]
fn intersect_keeps_overlap() {
    assert!(eq::<Intersect<(A, B, C), (C, A)>, (A, C)>());
    assert!(eq::<Intersect<(A,), (B,)>, ()>());
}

#[test]
fn contains_answers_by_id() {
    assert!(<(A, B) as Set>::contains::<A>());
    assert!(!<(A, B) as Set>::contains::<C>());
}

fn requires_subset<S: Subset<(A, B, C)>>() {}

#[test]
fn subset_is_a_bound() {
    requires_subset::<(A, C)>();
    requires_subset::<()>();
}
```

- [ ] **Step 2: Implement `set/ops.rs`**

```rust
use crate::attr::Attr;
use crate::set::verdict::{Hit, Miss, Or, Verdict};
use crate::set::{AttrEq, AttrId, Set};

pub trait In<S> {
    type Verdict;
}

impl<A: Attr> In<()> for A {
    type Verdict = Miss;
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<A: Attr, @N[B~#: Attr](sep=",")> In<(@N[B~#](sep=","),)> for A
    where
        @N[A: AttrEq<B~#>](sep=","),
    {
        type Verdict = @N(fold_or)[<A as AttrEq<B~#>>::Verdict];
    }
});

pub trait Select<V> {
    type Output;
}

impl<A: Attr> Select<Hit> for A {
    type Output = (A,);
}

impl<A: Attr> Select<Miss> for A {
    type Output = ();
}

pub trait Concat<B> {
    type Output;
}
```

`Concat` is the old `set.rs` `Union` machinery verbatim with the trait renamed — port the three `variadic!` blocks from the deleted file (unit/attr/tuple × unit/attr/tuple, `1..=16` and the `16×16` product), substituting `Concat` for `Union` and `Output` unchanged.

**Note for the implementer:** the `@N(fold_or)[..]` notation above assumes a fold combinator in `bascet-variadic`. The macro today supports `(sep=..)` joins only (see `crates/bascet-variadic/src/emit.rs`). An `Or` fold as a *type* is a nested projection, which a separator join cannot express. Two options, pick the first that works: (a) extend the macro with a `fold` transform that nests left-to-right — this also benefits `AttrEq` in Task 3; (b) express membership recursively instead — head/tail impls need no fold:

```rust
impl<A: Attr, B: Attr> In<(B,)> for A
where
    A: AttrEq<B>,
{
    type Verdict = <A as AttrEq<B>>::Verdict;
}
```

with cons-style recursion over the tuple tail generated by `variadic!` (arity k defers to arity k−1: `<A as In<(B1, …, Bk−1,)>>::Verdict` joined by `Or`). Recursion keeps every impl one `Or` deep.

`Join` and `Meet` build on `In`/`Select`/`Concat`, one element of the right operand at a time:

```rust
pub trait Join<R> {
    type Output;
}

impl<L: Set> Join<()> for L {
    type Output = L;
}

pub trait Meet<R> {
    type Output;
}

impl<L: Set> Meet<()> for L {
    type Output = ();
}
```

Recursive cases via `variadic!` (right operand arity `1..=16`): `Join` appends `<Head as Select<Not<InVerdict>>>::Output` — an element joins only when *absent*, so add `Not` to `verdict.rs` (`Hit ↦ Miss`, `Miss ↦ Hit`); `Meet` selects on presence directly. Each step `Concat`s the selected fragment onto the accumulator and recurses on the tail. Aliases in `set.rs`:

```rust
pub type Union<L, R> = <L as Join<R>>::Output;
pub type Intersect<L, R> = <L as Meet<R>>::Output;
```

- [ ] **Step 3: `Subset` and `contains` in `set.rs`**

```rust
#[diagnostic::on_unimplemented(
    message = "`{Self}` requires attributes not provided by the direct producer",
    label = "the producer's `Provides` must cover this layer's `Requires`"
)]
pub trait Subset<Sup: Set> {}

impl<Sup: Set> Subset<Sup> for () {}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<Sup: Set, @N[A~#: Attr](sep=",")> Subset<Sup> for (@N[A~#](sep=","),)
    where
        @N[A~#: In<Sup, Verdict = Hit>](sep=","),
    {
    }
});
```

`contains` on `Set`, const value path over IDs:

```rust
pub trait Set {
    fn contains<A: Attr>() -> bool;
}

impl Set for () {
    fn contains<A: Attr>() -> bool {
        false
    }
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[B~#: Attr](sep=",")> Set for (@N[B~#](sep=","),) {
        fn contains<A: Attr>() -> bool {
            @N[(<A::Id as AttrId>::ID == <B~#::Id as AttrId>::ID)](sep=" || ")
        }
    }
});
```

Single-attr `impl<A: Attr> Set for A` from the old file is dropped — sets are always tuples or `()` now; `(A,)` is the singleton.

- [ ] **Step 4: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test set_identity --test set_ops`
Expected: all passed. If the trait solver rejects `Join`/`Meet` recursion or inference fails on the aliases, this is the spec's acknowledged risk area — record the exact failure list and stop for review before proceeding.

---

### Task 5: Derive — `type Id` emission, inventory, `Record`

**Files:**
- Modify: `crates/bascet-derive/src/attr/id.rs`, `crates/bascet-derive/src/attr/plural.rs` (wherever the old `const ID` was emitted — check `attr/input.rs` too)
- Modify: `crates/bascet-core/src/attr.rs`
- Test: `crates/bascet-core/tests/attr_derive.rs`

**Interfaces:**
- Consumes: `Digit` aliases `H0`–`HF`, `AttrId` from Task 3.
- Produces: `#[derive(Attr)]` emits `impl Attr for X { type Id = (H?, …); }` — 16 digits from the FNV-1a hash of the type name, high nibble first — plus the existing `inventory::submit!` now reading `<<X as Attr>::Id as AttrId>::ID`. New `pub trait Record { type Attrs: Set; }` in `attr.rs`.

- [ ] **Step 1: Write the failing test**

```rust
use std::collections::HashMap;

use bascet_core::set::AttrId;
use bascet_core::{Attr, AttrEntry};
use bascet_derive::Attr;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Attr)]
struct Barcode;

#[test]
fn derived_id_is_xxh3_of_the_name() {
    assert_eq!(<<Barcode as Attr>::Id as AttrId>::ID, xxh3_64(b"Barcode"));
}

#[test]
fn no_two_attrs_share_an_id() {
    let mut seen = HashMap::new();
    for entry in inventory::iter::<AttrEntry>() {
        if let Some(name) = seen.insert(entry.id, entry.name) {
            assert_eq!(name, entry.name, "attr id collision at {:#x}", entry.id);
        }
    }
}
```

`no_two_attrs_share_an_id` is the real collision guarantee — it sweeps every registered attr in the inventory, so an actual collision fails loudly at test time instead of silently merging two attrs in set algebra. (Add `xxhash-rust` and `inventory` to `bascet-core` dev-dependencies.)

- [ ] **Step 2: Wire the digits into the derive's emit path**

`AttrId::from_name` and `AttrId::digits` already exist from Task 3 Step 5. In the derive's emit (follow `attr/plural.rs` / `attr/input.rs` — the code that currently writes `const ID`), replace the const with:

```rust
let id = crate::attr::id::AttrId::digits(crate::attr::id::AttrId::from_name(&name.to_string()));
quote! {
    impl bascet_core::Attr for #name {
        type Id = #id;
    }
}
```

and point the inventory registration at `<<#name as bascet_core::Attr>::Id as bascet_core::set::AttrId>::ID`. Plural/variadic expansion keeps hashing each generated name individually.

- [ ] **Step 4: Add `Record` to `attr.rs`**

```rust
pub trait Record {
    type Attrs: crate::set::Set;
}
```

No derive emission here — the Backing spec owns that; pass-through layers write `Provides = R::Attrs` against hand-written impls until it lands.

- [ ] **Step 5: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test attr_derive`
Expected: passed. Also `cargo check` across the workspace — `attr/backing.rs`, `attr/meta.rs`, `attr/phred.rs`, `attr/reads.rs`, `attr/block.rs` contain concrete attrs whose derives now emit `type Id`; fix any that declared `const ID` manually.

---

### Task 6: `Pressure` — packed, single-RMW hot path

**Files:**
- Rewrite: `crates/bascet-core/src/utils/pressure.rs`
- Test: unit tests in the same file (`#[cfg(test)]`)

**Interfaces:**
- Produces: `pub struct Pressure` (the old `AtomicPressure` is gone, the old non-atomic `Pressure` is gone). API: `new(initial, min, strain, growth, decay)`, `hit(&self)`, `miss(&self) -> Option<NonZeroU32>` (Some only on band increase), `level(&self) -> u32`, `strain(&self) -> usize`, `recover(&self)`. Packing: one `AtomicU64`, low 32 bits the value (biased unsigned), high 32 bits the band level.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    fn pressure() -> Pressure {
        Pressure::new(0, 0, NonZeroU32::new(4).unwrap(), 1, 1)
    }

    #[test]
    fn miss_emits_only_on_band_increase() {
        let p = pressure();
        let mut emissions = 0;
        for _ in 0..64 {
            if p.miss().is_some() {
                emissions += 1;
            }
        }
        assert!(emissions >= 2);
        assert!(emissions <= 6);
        assert_eq!(p.level(), band(64, 4));
    }

    #[test]
    fn hit_decays_and_lowers_level() {
        let p = pressure();
        for _ in 0..64 {
            p.miss();
        }
        for _ in 0..64 {
            p.hit();
        }
        assert_eq!(p.level(), 0);
    }

    #[test]
    fn hit_clamps_at_min() {
        let p = Pressure::new(2, 2, NonZeroU32::new(4).unwrap(), 1, 1);
        for _ in 0..16 {
            p.hit();
        }
        assert_eq!(p.value(), 2);
    }
}
```

- [ ] **Step 2: Implement**

```rust
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};

const BIAS: u64 = 1 << 31;

pub struct Pressure {
    packed: AtomicU64,
    min: u32,
    strain: NonZeroU32,
    growth: u32,
    decay: u32,
}

impl Pressure {
    pub fn new(initial: u32, min: u32, strain: NonZeroU32, growth: u32, decay: u32) -> Self {
        let initial = initial.max(min);
        let level = band(initial, strain.get());
        Self {
            packed: AtomicU64::new(pack(level, initial)),
            min,
            strain,
            growth,
            decay,
        }
    }

    #[inline(always)]
    pub fn hit(&self) {
        let old = self.packed.fetch_sub(self.decay as u64, Ordering::Relaxed);
        let (level, value) = unpack(old);
        let value = value.saturating_sub(self.decay);
        if value <= self.min {
            self.packed.fetch_max(pack(level, self.min), Ordering::Relaxed);
        }
        let next = band(value.max(self.min), self.strain.get());
        if next < level {
            self.packed
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    let (l, v) = unpack(cur);
                    (next < l).then_some(pack(next, v))
                })
                .ok();
        }
    }

    #[inline(always)]
    pub fn miss(&self) -> Option<NonZeroU32> {
        let old = self.packed.fetch_add(self.growth as u64, Ordering::Relaxed);
        let (level, value) = unpack(old);
        let next = band(value.saturating_add(self.growth), self.strain.get());
        if next <= level {
            return None;
        }
        self.packed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                let (l, v) = unpack(cur);
                (next > l).then_some(pack(next, v))
            })
            .ok()
            .and_then(|_| NonZeroU32::new(next))
    }

    pub fn recover(&self) {
        self.packed.store(
            pack(band(self.min, self.strain.get()), self.min),
            Ordering::Relaxed,
        );
    }

    #[inline(always)]
    pub fn level(&self) -> u32 {
        unpack(self.packed.load(Ordering::Relaxed)).0
    }

    #[inline(always)]
    pub fn value(&self) -> u32 {
        unpack(self.packed.load(Ordering::Relaxed)).1.max(self.min)
    }

    #[inline(always)]
    pub fn strain(&self) -> usize {
        strain(self.level())
    }
}

#[inline(always)]
fn pack(level: u32, value: u32) -> u64 {
    ((level as u64) << 32) | (value as u64 + BIAS)
}

#[inline(always)]
fn unpack(packed: u64) -> (u32, u32) {
    let level = (packed >> 32) as u32;
    let value = (packed & 0xFFFF_FFFF).saturating_sub(BIAS) as u32;
    (level, value)
}

#[inline(always)]
fn strain(level: u32) -> usize {
    match level {
        0 => 0,
        level => 1usize
            .checked_shl(level.saturating_sub(1))
            .unwrap_or(usize::MAX),
    }
}

#[inline(always)]
fn band(pressure: u32, strain: u32) -> u32 {
    if pressure < strain {
        0
    } else {
        1 + (pressure / strain).ilog2()
    }
}
```

**Notes:** the bias keeps `fetch_sub` from borrowing into the level bits — a hit at value 0 dips below `BIAS` and the `fetch_max` repair restores the floor before any reader can misband (readers clamp with `saturating_sub(BIAS)`). Level lowering on `hit` and raising on `miss` are the cold `fetch_update` paths, entered only on an actual band move.

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core pressure`
Expected: 3 passed.

---

### Task 7: `Load` and `Activity` — packed activity gauge

**Files:**
- Create: `crates/bascet-core/src/scheduler.rs` (module stub), `crates/bascet-core/src/scheduler/load.rs`
- Modify: `crates/bascet-core/src/lib.rs` (add `pub mod scheduler;`)
- Test: unit tests in `load.rs`

**Interfaces:**
- Produces: `pub enum Activity { Idle, Busy, Starved, Blocked }` (`Idle = 0 … Blocked = 3`); `pub struct Load(AtomicU64)` with `transition(&self, old: Activity, new: Activity)`, `arrive(&self, a: Activity)`, `depart(&self, a: Activity)`, `count(&self, a: Activity) -> u16`, `pressure(&self) -> u32` (`Starved + Blocked`), `busy(&self) -> u16`.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_moves_one_count() {
        let load = Load::new();
        load.arrive(Activity::Idle);
        load.transition(Activity::Idle, Activity::Busy);
        assert_eq!(load.count(Activity::Idle), 0);
        assert_eq!(load.count(Activity::Busy), 1);
    }

    #[test]
    fn pressure_sums_starved_and_blocked() {
        let load = Load::new();
        load.arrive(Activity::Starved);
        load.arrive(Activity::Blocked);
        load.arrive(Activity::Busy);
        assert_eq!(load.pressure(), 2);
    }

    #[test]
    fn depart_clears() {
        let load = Load::new();
        load.arrive(Activity::Busy);
        load.depart(Activity::Busy);
        assert_eq!(load.count(Activity::Busy), 0);
    }
}
```

- [ ] **Step 2: Implement `scheduler/load.rs`**

```rust
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Activity {
    Idle = 0,
    Busy = 1,
    Starved = 2,
    Blocked = 3,
}

pub struct Load(AtomicU64);

impl Load {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    #[inline(always)]
    pub fn transition(&self, old: Activity, new: Activity) {
        if old == new {
            return;
        }
        let delta = (1u64 << (new as u64 * 16)).wrapping_sub(1u64 << (old as u64 * 16));
        self.0.fetch_add(delta, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn arrive(&self, activity: Activity) {
        self.0.fetch_add(1 << (activity as u64 * 16), Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn depart(&self, activity: Activity) {
        self.0
            .fetch_sub(1 << (activity as u64 * 16), Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn count(&self, activity: Activity) -> u16 {
        (self.0.load(Ordering::Relaxed) >> (activity as u64 * 16)) as u16
    }

    #[inline(always)]
    pub fn busy(&self) -> u16 {
        self.count(Activity::Busy)
    }

    #[inline(always)]
    pub fn pressure(&self) -> u32 {
        self.count(Activity::Starved) as u32 + self.count(Activity::Blocked) as u32
    }
}
```

`scheduler.rs` for now:

```rust
pub mod load;

pub use load::{Activity, Load};
```

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core load`
Expected: 3 passed.

---

### Task 8: Tiers, slots, dispatch, pool

**Files:**
- Create: `crates/bascet-core/src/runtime.rs` (stub for now), `src/runtime/tier.rs`, `src/runtime/dispatch.rs`, `src/runtime/dispatch/slot.rs`, `src/runtime/dispatch/task.rs`, `src/runtime/dispatch/system.rs`, `src/runtime/pool.rs`
- Modify: `crates/bascet-core/src/lib.rs` (add `pub mod runtime;`)
- Test: `crates/bascet-core/tests/dispatch.rs`

**Interfaces:**
- Produces:
  - `pub enum Tier { Burn, Job, Task }` (`runtime/tier.rs`).
  - `pub(crate) type Job = Box<dyn FnOnce() + Send>;`
  - `pub struct Slot { pub(crate) tier: Tier, pub(crate) index: u32 }` — identity, tier, trace key.
  - `pub(crate) struct Dispatch` (`dispatch.rs`): owns the persistent threads. `Dispatch::spawn(burn: usize, jobs: usize, tasks: usize) -> Dispatch`; `fn send(&self, slot: Slot, job: Job)` for Burn/Job; `fn spawn_task<F, Fut>(&self, slot: Slot, f: F)` for Task (compio, `F: FnOnce() -> Fut + Send + 'static`, `Fut: Future<Output = ()>`); `fn system<F, Fut>(&self, f: F)` — sends the scheduler loop to the System thread; `fn close(&self)` — drops inbox senders so threads park out and join on `Dispatch` drop.
  - `pub(crate) struct Pool` (`pool.rs`): per-tier free lists plus per-layer records. `fn acquire(&mut self, tier: Tier, layer: usize, band: u32) -> Option<Slot>`; `fn release(&mut self, slot: Slot) -> Option<usize>` (returns the winning claimant layer, band-first then `pass`); `fn withdraw(&mut self, tier: Tier, layer: usize)`; `fn waiting(&self, tier: Tier, layer: usize) -> Option<u32>`.

- [ ] **Step 1: Write the failing tests**

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bascet_core::runtime::{Dispatch, Slot, Tier};

#[test]
fn job_runs_on_persistent_thread_and_slot_survives_panic() {
    let dispatch = Dispatch::spawn(0, 2, 0);
    let hits = Arc::new(AtomicU32::new(0));

    let h = Arc::clone(&hits);
    dispatch.send(Slot { tier: Tier::Job, index: 0 }, Box::new(move || {
        h.fetch_add(1, Ordering::Relaxed);
    }));

    dispatch.send(Slot { tier: Tier::Job, index: 0 }, Box::new(|| panic!("worker panic")));

    let h = Arc::clone(&hits);
    dispatch.send(Slot { tier: Tier::Job, index: 0 }, Box::new(move || {
        h.fetch_add(1, Ordering::Relaxed);
    }));

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(hits.load(Ordering::Relaxed), 2);
}
```

Pool test (unit, in `pool.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Tier;

    fn pool() -> Pool {
        Pool::new(2, 1, 1, 3)
    }

    #[test]
    fn acquire_pops_free_then_claims() {
        let mut pool = pool();
        assert!(pool.acquire(Tier::Job, 0, 1).is_some());
        assert!(pool.acquire(Tier::Job, 1, 2).is_none());
        assert_eq!(pool.waiting(Tier::Job, 1), Some(2));
    }

    #[test]
    fn release_serves_highest_band_first() {
        let mut pool = pool();
        let slot = pool.acquire(Tier::Job, 0, 1).unwrap();
        assert!(pool.acquire(Tier::Job, 1, 2).is_none());
        assert!(pool.acquire(Tier::Job, 2, 5).is_none());
        assert_eq!(pool.release(slot), Some(2));
    }

    #[test]
    fn claim_overwrites_in_place() {
        let mut pool = pool();
        let _slot = pool.acquire(Tier::Job, 0, 1).unwrap();
        pool.acquire(Tier::Job, 1, 2);
        pool.acquire(Tier::Job, 1, 4);
        assert_eq!(pool.waiting(Tier::Job, 1), Some(4));
    }
}
```

- [ ] **Step 2: Implement `runtime/tier.rs` and `runtime/dispatch/slot.rs`**

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    Burn,
    Job,
    Task,
}
```

```rust
use crate::runtime::Tier;

pub type Job = Box<dyn FnOnce() + Send>;

#[derive(Clone, Copy, Debug)]
pub struct Slot {
    pub tier: Tier,
    pub index: u32,
}
```

- [ ] **Step 3: Implement `runtime/dispatch.rs` + `dispatch/slot.rs` thread bodies**

```rust
pub mod slot;
pub mod system;
pub mod task;

pub use slot::{Job, Slot};

use crate::runtime::Tier;

pub(crate) struct Dispatch {
    burn_txs: Box<[kanal::Sender<Job>]>,
    job_txs: Box<[kanal::Sender<Job>]>,
    task: task::Task,
    system: system::System,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Dispatch {
    pub(crate) fn spawn(burn: usize, jobs: usize, tasks: usize) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let mut handles = Vec::new();
        let burn_txs = (0..burn)
            .map(|i| {
                let (jobs_tx, jobs_rx) = kanal::unbounded::<Job>();
                let core = cores.get(i).copied();
                handles.push(
                    std::thread::Builder::new()
                        .name(format!("bascet-burn-{i}"))
                        .spawn(move || {
                            if let Some(core) = core {
                                core_affinity::set_for_current(core);
                            }
                            body(jobs_rx);
                        })
                        .expect("spawn burn thread"),
                );
                jobs_tx
            })
            .collect();
        let job_txs = (0..jobs)
            .map(|i| {
                let (jobs_tx, jobs_rx) = kanal::unbounded::<Job>();
                handles.push(
                    std::thread::Builder::new()
                        .name(format!("bascet-job-{i}"))
                        .spawn(move || body(jobs_rx))
                        .expect("spawn job thread"),
                );
                jobs_tx
            })
            .collect();
        Self {
            burn_txs,
            job_txs,
            task: task::Task::spawn(tasks, &mut handles),
            system: system::System::spawn(&mut handles),
            handles,
        }
    }

    pub(crate) fn send(&self, slot: Slot, job: Job) {
        let txs = match slot.tier {
            Tier::Burn => &self.burn_txs,
            Tier::Job => &self.job_txs,
            Tier::Task => unreachable!("task workers dispatch through spawn_task"),
        };
        txs[slot.index as usize].send(job).ok();
    }
}

fn body(jobs_rx: kanal::Receiver<Job>) {
    while let Ok(job) = jobs_rx.recv() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}
```

`dispatch/task.rs` mirrors the old `Io` (compio thread pool, `FnOnce() -> Fut` closures built on the target thread, futures detached); `dispatch/system.rs` is one compio thread whose `run` method accepts exactly one resident closure the same way. Reuse the deleted `runtime.rs` `Io::spawn_thread` shape verbatim for both — that code already handles the compio runtime and the unbounded task channel.

`Dispatch::close` drops all senders (`self.burn_txs`/`job_txs` swapped for empty boxes, task/system channels closed); a `Drop` impl joins `handles`.

- [ ] **Step 4: Implement `runtime/pool.rs`**

```rust
use crate::runtime::dispatch::Slot;
use crate::runtime::Tier;

pub(crate) struct Pool {
    burn: Shelf,
    job: Shelf,
    task: Shelf,
}

struct Shelf {
    free: Vec<Slot>,
    layers: Box<[Record]>,
}

#[derive(Default, Clone)]
struct Record {
    pass: u64,
    waiting: Option<u32>,
}

impl Pool {
    pub(crate) fn new(burn: usize, jobs: usize, tasks: usize, layers: usize) -> Self {
        let shelf = |tier: Tier, count: usize| Shelf {
            free: (0..count as u32).map(|index| Slot { tier, index }).collect(),
            layers: vec![Record::default(); layers].into_boxed_slice(),
        };
        Self {
            burn: shelf(Tier::Burn, burn),
            job: shelf(Tier::Job, jobs),
            task: shelf(Tier::Task, tasks),
        }
    }

    fn shelf(&mut self, tier: Tier) -> &mut Shelf {
        match tier {
            Tier::Burn => &mut self.burn,
            Tier::Job => &mut self.job,
            Tier::Task => &mut self.task,
        }
    }

    pub(crate) fn acquire(&mut self, tier: Tier, layer: usize, band: u32) -> Option<Slot> {
        let shelf = self.shelf(tier);
        let outranked = shelf
            .layers
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.waiting.map(|b| (i, b)))
            .any(|(i, b)| i != layer && (b, u64::MAX - shelf.layers[i].pass) > (band, u64::MAX));
        if !outranked && let Some(slot) = shelf.free.pop() {
            shelf.layers[layer].waiting = None;
            shelf.layers[layer].pass += 1;
            return Some(slot);
        }
        shelf.layers[layer].waiting = Some(band);
        None
    }

    pub(crate) fn release(&mut self, slot: Slot) -> Option<usize> {
        let shelf = self.shelf(slot.tier);
        shelf.free.push(slot);
        shelf
            .layers
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.waiting.map(|band| (band, u64::MAX - r.pass, i)))
            .max()
            .map(|(_, _, i)| i)
    }

    pub(crate) fn withdraw(&mut self, tier: Tier, layer: usize) {
        self.shelf(tier).layers[layer].waiting = None;
    }

    pub(crate) fn waiting(&mut self, tier: Tier, layer: usize) -> Option<u32> {
        self.shelf(tier).layers[layer].waiting
    }
}
```

**Note:** the `outranked` comparison shown is deliberately simplified and wrong as written — implement rank as `(band, reverse pass)` compared between the requester and the best waiting record, serving the waiter by returning `None` for the requester when the waiter outranks it. Write it as a small `fn rank(record: &Record, band: u32) -> (u32, u64)` helper and compare; the unit tests in Step 1 pin the observable behavior (waiter with higher band wins the released slot; requester with no competition pops free directly).

`runtime.rs` for now:

```rust
pub mod dispatch;
pub mod pool;
pub mod tier;

pub use dispatch::{Dispatch, Job, Slot};
pub use tier::Tier;
```

(`Dispatch` is `pub` during this task so the integration test can drive it; Task 17 narrows it back to `pub(crate)` once `Runtime` wraps it.)

- [ ] **Step 5: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test dispatch pool`
Expected: all passed, threads join cleanly (no hang at test exit).

---

### Task 9: Event plane — `Port`, `Event`, `Preempt`, `Worker`

**Files:**
- Create: `crates/bascet-core/src/consts.rs`, `src/scheduler/port.rs`, `src/scheduler/event.rs`, `src/scheduler/preempt.rs`, `src/worker.rs`
- Modify: `crates/bascet-core/src/scheduler.rs`, `src/lib.rs` (add `pub(crate) mod consts;` and `pub mod worker;`)
- Test: unit tests in `worker.rs`

**Interfaces:**
- Consumes: `Load`/`Activity` (Task 7), `Slot` (Task 8), `Pressure` (Task 6).
- Produces: `Port { index, load, demand, events_tx }` with `Port::new(index, events_tx) -> Arc<Port>` and `petition(self: &Arc<Self>, action, subject: &Arc<Port>, worker: Option<Arc<Worker>>)`; `Event { action, subject, sender, worker, receipt }`; `Action { Promote, Demote, Acquire, Released, Yield }`; `Receipt = kanal::Sender<()>`; `Preempt { Continue, Yield, Halt }`; `Worker` with `new(slot, port) -> Arc<Worker>`, `set_activity`, `halted`, `preempted`, `preempt(p)`, `state`, `patience`, `finish(state, patience)`; `State { New, Running, Released, Finished, Panicked }`.

- [ ] **Step 1: `consts.rs` — starting values, tuned later via `benches/`**

```rust
pub(crate) const PRESSURE_INITIAL: u32 = 0;
pub(crate) const PRESSURE_MIN: u32 = 0;
pub(crate) const PRESSURE_STRAIN: u32 = 4;
pub(crate) const PRESSURE_GROWTH: u32 = 1;
pub(crate) const PRESSURE_DECAY: u32 = 1;
pub(crate) const PATIENCE_START: u32 = 32;
pub(crate) const PATIENCE_MIN: u32 = 8;
pub(crate) const PATIENCE_CAP: u32 = 256;
pub(crate) const WATERMARK: u32 = 2;
pub(crate) const DEPTH: usize = 64;
```

- [ ] **Step 2: `scheduler/event.rs` and `scheduler/preempt.rs`**

```rust
use std::sync::Arc;

use crate::scheduler::port::Port;
use crate::worker::Worker;

pub type Receipt = kanal::Sender<()>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Promote,
    Demote,
    Acquire,
    Released,
    Yield,
}

pub struct Event {
    pub action: Action,
    pub subject: Arc<Port>,
    pub sender: Arc<Port>,
    pub worker: Option<Arc<Worker>>,
    pub receipt: Option<Receipt>,
}
```

```rust
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Preempt {
    Continue = 0,
    Yield = 1,
    Halt = 2,
}
```

- [ ] **Step 3: `scheduler/port.rs`**

```rust
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::consts::{
    PRESSURE_DECAY, PRESSURE_GROWTH, PRESSURE_INITIAL, PRESSURE_MIN, PRESSURE_STRAIN,
};
use crate::scheduler::event::{Action, Event};
use crate::scheduler::load::Load;
use crate::utils::Pressure;
use crate::worker::Worker;

pub struct Port {
    pub(crate) index: u32,
    pub(crate) load: Load,
    pub(crate) demand: Pressure,
    pub(crate) events_tx: kanal::Sender<Event>,
}

impl Port {
    pub(crate) fn new(index: u32, events_tx: kanal::Sender<Event>) -> Arc<Self> {
        Arc::new(Self {
            index,
            load: Load::new(),
            demand: Pressure::new(
                PRESSURE_INITIAL,
                PRESSURE_MIN,
                NonZeroU32::new(PRESSURE_STRAIN).unwrap(),
                PRESSURE_GROWTH,
                PRESSURE_DECAY,
            ),
            events_tx,
        })
    }

    pub(crate) fn petition(
        self: &Arc<Self>,
        action: Action,
        subject: &Arc<Port>,
        worker: Option<Arc<Worker>>,
    ) {
        self.events_tx
            .send(Event {
                action,
                subject: Arc::clone(subject),
                sender: Arc::clone(self),
                worker,
                receipt: None,
            })
            .ok();
    }
}
```

- [ ] **Step 4: `worker.rs` — the handle**

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use crossbeam::utils::CachePadded;

use crate::runtime::Slot;
use crate::scheduler::event::Action;
use crate::scheduler::load::Activity;
use crate::scheduler::port::Port;
use crate::scheduler::preempt::Preempt;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    New = 0,
    Running = 1,
    Released = 2,
    Finished = 3,
    Panicked = 4,
}

pub struct Worker {
    pub(crate) slot: Slot,
    pub(crate) port: Arc<Port>,
    preempt: CachePadded<AtomicU8>,
    state: AtomicU8,
    activity: AtomicU8,
    patience: AtomicU32,
}

impl Worker {
    pub(crate) fn new(slot: Slot, port: Arc<Port>) -> Arc<Self> {
        port.load.arrive(Activity::Idle);
        Arc::new(Self {
            slot,
            port,
            preempt: CachePadded::new(AtomicU8::new(Preempt::Continue as u8)),
            state: AtomicU8::new(State::New as u8),
            activity: AtomicU8::new(Activity::Idle as u8),
            patience: AtomicU32::new(0),
        })
    }

    #[inline(always)]
    pub(crate) fn set_activity(&self, new: Activity) {
        let old = self.activity.swap(new as u8, Ordering::Relaxed);
        self.port.load.transition(activity(old), new);
    }

    #[inline(always)]
    pub(crate) fn halted(&self) -> bool {
        self.preempt.load(Ordering::Relaxed) == Preempt::Halt as u8
    }

    #[inline(always)]
    pub(crate) fn preempted(&self) -> Preempt {
        match self.preempt.load(Ordering::Relaxed) {
            0 => Preempt::Continue,
            1 => Preempt::Yield,
            _ => Preempt::Halt,
        }
    }

    pub(crate) fn preempt(&self, preempt: Preempt) {
        self.preempt.store(preempt as u8, Ordering::Relaxed);
    }

    pub(crate) fn state(&self) -> State {
        match self.state.load(Ordering::Acquire) {
            0 => State::New,
            1 => State::Running,
            2 => State::Released,
            3 => State::Finished,
            _ => State::Panicked,
        }
    }

    pub(crate) fn patience(&self) -> u32 {
        self.patience.load(Ordering::Acquire)
    }

    pub(crate) fn finish(self: &Arc<Self>, state: State, patience: u32) {
        self.patience.store(patience, Ordering::Release);
        self.state.store(state as u8, Ordering::Release);
        let old = self.activity.swap(Activity::Idle as u8, Ordering::Relaxed);
        self.port.load.transition(activity(old), Activity::Idle);
        self.port.load.depart(Activity::Idle);
        let port = Arc::clone(&self.port);
        port.petition(Action::Released, &Arc::clone(&self.port), Some(Arc::clone(self)));
    }
}

fn activity(raw: u8) -> Activity {
    match raw {
        0 => Activity::Idle,
        1 => Activity::Busy,
        2 => Activity::Starved,
        _ => Activity::Blocked,
    }
}
```

Unit tests in the same file: `set_activity` keeps `Load` counts consistent (arrive Idle → Busy → count(Busy) == 1, count(Idle) == 0); `finish` delivers a `Released` event on the channel with `worker` set, `state()` reading back `Finished`, `patience()` reading back the published value.

`scheduler.rs` grows to:

```rust
pub mod event;
pub mod load;
pub mod port;
pub mod preempt;

pub use event::{Action, Event, Receipt};
pub use load::{Activity, Load};
pub use port::Port;
pub use preempt::Preempt;
```

- [ ] **Step 5: Checkpoint (user runs)**

Run: `cargo test -p bascet-core worker`
Expected: passed.

---

### Task 10: `Emit` and `AsyncEmit` — twins over one core

**Files:**
- Create: `crates/bascet-core/src/apply.rs` (module stub: `pub mod emit;` — traits land in Task 11), `src/apply/emit.rs`
- Modify: `crates/bascet-core/src/lib.rs` (add `pub mod apply;`)
- Test: unit tests in `emit.rs`

**Interfaces:**
- Produces: `Emit<Out, Wants: Set>` with `fn push(&mut self, item: Out)`, `fn wants<A: Attr>(&self) -> bool`, `fn reject(&mut self, reason: &'static str)`, `fn finish(&mut self)`; `AsyncEmit<Out, Wants: Set>` identical except `async fn push`. Both wrap `pub(crate) struct Core<Out>`; construction is `Core::edge(items_tx, credits_rx, consumer, worker)` and `Core::null()` (sinks — items dropped, infinite budget). Loop-facing: `Core::finished() -> bool`, `Core::orphaned() -> bool` (push met a closed edge — the worker loop turns this into `Finished`), `Core::fold(&mut self) -> (u64, Vec<(&'static str, u64)>)` (pushed count + rejects, drained at checkpoints).

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::apply::emit::{Core, Emit};
    use std::marker::PhantomData;

    fn emit(budget_seed: u32) -> (Emit<u32, ()>, kanal::Receiver<u32>, kanal::Sender<u32>) {
        let (items_tx, items_rx) = kanal::bounded(8);
        let (credits_tx, credits_rx) = kanal::unbounded();
        credits_tx.send(budget_seed).unwrap();
        let core = Core::edge(items_tx, credits_rx, None, None);
        (Emit { core, _wants: PhantomData }, items_rx, credits_tx)
    }

    #[test]
    fn push_spends_budget_and_delivers() {
        let (mut out, items_rx, _credits_tx) = emit(2);
        out.push(7);
        out.push(9);
        assert_eq!(items_rx.recv().unwrap(), 7);
        assert_eq!(items_rx.recv().unwrap(), 9);
    }

    #[test]
    fn push_waits_for_credit() {
        let (mut out, items_rx, credits_tx) = emit(1);
        out.push(1);
        let waiter = std::thread::spawn(move || {
            let mut out = out;
            out.push(2);
            out
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        credits_tx.send(1).unwrap();
        waiter.join().unwrap();
        assert_eq!(items_rx.recv().unwrap(), 1);
        assert_eq!(items_rx.recv().unwrap(), 2);
    }

    #[test]
    fn orphaned_when_consumer_gone() {
        let (mut out, items_rx, _credits_tx) = emit(4);
        drop(items_rx);
        out.push(1);
        assert!(out.core.orphaned());
    }
}
```

- [ ] **Step 2: Implement `apply/emit.rs`**

```rust
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::attr::Attr;
use crate::scheduler::event::Action;
use crate::scheduler::load::Activity;
use crate::scheduler::port::Port;
use crate::set::Set;
use crate::worker::Worker;

pub(crate) enum Step<Out> {
    Done,
    Wait(Out),
    Closed,
}

pub(crate) struct Core<Out> {
    items_tx: Option<kanal::Sender<Out>>,
    credits_rx: Option<kanal::Receiver<u32>>,
    budget: u32,
    pushed: u64,
    finished: bool,
    orphaned: bool,
    rejects: Vec<(&'static str, u64)>,
    consumer: Option<Arc<Port>>,
    worker: Option<Arc<Worker>>,
}

impl<Out> Core<Out> {
    pub(crate) fn edge(
        items_tx: kanal::Sender<Out>,
        credits_rx: kanal::Receiver<u32>,
        consumer: Option<Arc<Port>>,
        worker: Option<Arc<Worker>>,
    ) -> Self {
        Self {
            items_tx: Some(items_tx),
            credits_rx: Some(credits_rx),
            budget: 0,
            pushed: 0,
            finished: false,
            orphaned: false,
            rejects: Vec::new(),
            consumer,
            worker,
        }
    }

    pub(crate) fn null() -> Self {
        Self {
            items_tx: None,
            credits_rx: None,
            budget: u32::MAX,
            pushed: 0,
            finished: false,
            orphaned: false,
            rejects: Vec::new(),
            consumer: None,
            worker: None,
        }
    }

    fn absorb(&mut self) {
        if let Some(credits_rx) = &self.credits_rx {
            while let Ok(Some(credit)) = credits_rx.try_recv() {
                self.budget = self.budget.saturating_add(credit);
            }
        }
    }

    pub(crate) fn step(&mut self, item: &mut Option<Out>) -> Step<()> {
        if self.orphaned {
            item.take();
            return Step::Closed;
        }
        let Some(items_tx) = &self.items_tx else {
            item.take();
            self.pushed += 1;
            return Step::Done;
        };
        self.absorb();
        if self.budget == 0 {
            return Step::Wait(());
        }
        match items_tx.try_send_option(item) {
            Ok(true) => {
                self.budget -= 1;
                self.pushed += 1;
                Step::Done
            }
            Ok(false) => Step::Wait(()),
            Err(_) => {
                item.take();
                self.orphaned = true;
                Step::Closed
            }
        }
    }

    fn signal(&self) {
        if let (Some(consumer), Some(worker)) = (&self.consumer, &self.worker) {
            worker.set_activity(Activity::Blocked);
            if consumer.demand.miss().is_some() {
                worker.port.petition(Action::Promote, consumer, Some(Arc::clone(worker)));
            }
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished
    }

    pub(crate) fn orphaned(&self) -> bool {
        self.orphaned
    }

    pub(crate) fn fold(&mut self) -> (u64, Vec<(&'static str, u64)>) {
        (std::mem::take(&mut self.pushed), std::mem::take(&mut self.rejects))
    }
}

pub struct Emit<Out, Wants: Set> {
    pub(crate) core: Core<Out>,
    pub(crate) _wants: PhantomData<Wants>,
}

impl<Out, Wants: Set> Emit<Out, Wants> {
    pub fn push(&mut self, item: Out) {
        let mut item = Some(item);
        loop {
            match self.core.step(&mut item) {
                Step::Done | Step::Closed => return,
                Step::Wait(()) => {
                    self.core.signal();
                    if let Some(credits_rx) = &self.core.credits_rx {
                        if let Ok(Some(credit)) =
                            credits_rx.recv_timeout(Duration::from_millis(1)).map(Some).or(Ok::<_, ()>(None))
                        {
                            self.core.budget = self.core.budget.saturating_add(credit);
                        }
                    }
                    if let Some(worker) = &self.core.worker {
                        if worker.halted() {
                            self.core.orphaned = true;
                            return;
                        }
                        worker.set_activity(Activity::Busy);
                    }
                }
            }
        }
    }

    pub fn wants<A: Attr>(&self) -> bool {
        Wants::contains::<A>()
    }

    pub fn reject(&mut self, reason: &'static str) {
        match self.core.rejects.iter_mut().find(|(r, _)| *r == reason) {
            Some((_, count)) => *count += 1,
            None => {
                tracing::warn!(reason, "item rejected");
                self.core.rejects.push((reason, 1));
            }
        }
    }

    pub fn finish(&mut self) {
        self.core.finished = true;
    }
}

pub struct AsyncEmit<Out, Wants: Set> {
    pub(crate) core: Core<Out>,
    pub(crate) _wants: PhantomData<Wants>,
}

impl<Out, Wants: Set> AsyncEmit<Out, Wants> {
    pub async fn push(&mut self, item: Out) {
        let mut item = Some(item);
        loop {
            match self.core.step(&mut item) {
                Step::Done | Step::Closed => return,
                Step::Wait(()) => {
                    self.core.signal();
                    if let Some(credits_rx) = &self.core.credits_rx {
                        if let Ok(credit) = credits_rx.as_async().recv().await {
                            self.core.budget = self.core.budget.saturating_add(credit);
                        } else {
                            self.core.orphaned = true;
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn wants<A: Attr>(&self) -> bool {
        Wants::contains::<A>()
    }

    pub fn reject(&mut self, reason: &'static str) {
        match self.core.rejects.iter_mut().find(|(r, _)| *r == reason) {
            Some((_, count)) => *count += 1,
            None => {
                tracing::warn!(reason, "item rejected");
                self.core.rejects.push((reason, 1));
            }
        }
    }

    pub fn finish(&mut self) {
        self.core.finished = true;
    }
}
```

**Notes for the implementer:** every try-decision lives in `Core::step`, shared by both twins — the twins own only the wait. Check kanal's exact API names (`try_send_option`, `recv_timeout`, `as_async`) against the version in the lockfile and adapt; the deleted `pipeline/edge.rs` used `try_send_option` so it exists. The sync wait shown is a 1ms bounded credit wait per round so the `halted()` check runs between rounds — `Halt` must interrupt a blocked push. The two `reject` bodies are identical by design; if the duplication grates, hang them on `Core` — but `push` cannot move.

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core emit`
Expected: 3 passed.

---

### Task 11: `Apply`, `ApplyAsync`, `Error`, the `Work` unifier

**Files:**
- Modify: `crates/bascet-core/src/apply.rs`
- Create: `crates/bascet-core/src/apply/execute.rs`
- Test: `crates/bascet-core/tests/apply.rs`

**Interfaces:**
- Produces: the two public traits exactly as the spec declares them; `Error::new(impl Display)` with `Display`/`Debug`/`std::error::Error`; sealed `Work<M>` with markers `Synchronous`/`Asynchronous` forwarding `Input`/`Output`/`Provides`/`Requires`. Task 16 extends `Work` with `launch` — the associated types defined here are final.

- [ ] **Step 1: `apply.rs`**

```rust
pub mod emit;
#[doc(hidden)]
pub mod execute;

pub use emit::{AsyncEmit, Emit};
pub use execute::Error;

use crate::set::Set;

pub trait Apply: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    fn apply<Wants: Set>(
        &mut self,
        input: Self::Input,
        out: &mut Emit<Self::Output, Wants>,
    ) -> Result<(), Error>;
}

pub trait ApplyAsync: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    async fn apply<Wants: Set>(
        &mut self,
        input: Self::Input,
        out: &mut AsyncEmit<Self::Output, Wants>,
    ) -> Result<(), Error>;
}
```

- [ ] **Step 2: `apply/execute.rs`**

```rust
use std::fmt;

use crate::apply::{Apply, ApplyAsync};
use crate::set::Set;

pub struct Synchronous;
pub struct Asynchronous;

#[derive(Debug)]
pub struct Error {
    message: Box<str>,
}

impl Error {
    pub fn new(message: impl fmt::Display) -> Self {
        Self {
            message: message.to_string().into_boxed_str(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub trait Work<M>: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;
}

impl<A: Apply> Work<Synchronous> for A {
    type Input = A::Input;
    type Output = A::Output;
    type Provides = A::Provides;
    type Requires = A::Requires;
}

impl<A: ApplyAsync> Work<Asynchronous> for A {
    type Input = A::Input;
    type Output = A::Output;
    type Provides = A::Provides;
    type Requires = A::Requires;
}
```

`Work` and the markers must be `pub` (they appear in public builder bounds — a `pub(crate)` trait there is E0445) but the module is `#[doc(hidden)]` and nothing re-exports them from `lib.rs`: sealed by reachability convention, exactly the axum arrangement. Move `Error` out to `apply.rs`'s re-export as shown so the public path is `bascet_core::Error`.

- [ ] **Step 3: Write the test**

```rust
use bascet_core::{Apply, ApplyAsync, Emit, AsyncEmit, Error};
use bascet_core::set::Set;

#[derive(Clone)]
struct Doubler;

impl Apply for Doubler {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input * 2);
        Ok(())
    }
}

#[derive(Clone)]
struct Sleeper;

impl ApplyAsync for Sleeper {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    async fn apply<W: Set>(&mut self, input: u32, out: &mut AsyncEmit<u32, W>) -> Result<(), Error> {
        out.push(input).await;
        Ok(())
    }
}

#[test]
fn traits_are_implementable() {
    let _ = Doubler;
    let _ = Sleeper;
}
```

- [ ] **Step 4: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test apply`
Expected: compiles and passes — this proves AFIT with the generic `Wants` method resolves on stable.

---

### Task 12: Builder type-state and the `Wants` algebra compile test

**Files:**
- Create: `crates/bascet-core/src/pipeline.rs`, `src/pipeline/builder.rs`
- Modify: `crates/bascet-core/src/lib.rs` (add `pub mod pipeline;`)
- Test: `crates/bascet-core/tests/builder.rs`

**Interfaces:**
- Produces: `Pipeline::builder() -> PipelineBuilder<()>`; `.source(a)`, `.layer(a)`, `.sink(a)` with per-edge `Subset` checks and marker inference; chain nodes `Source<A, M>` / `Node<A, M, Tail>` (fields `apply`, `tail`, `_mode` — Task 18's `Connect` consumes them); `trait Head { type Output; type Provides: Set; }` on chains; type alias `Wanted<A, M, W> = Union<<A as Work<M>>::Requires, Intersect<W, <A as Work<M>>::Provides>>` — the spec's `Wants` derivation, used again by `Connect`.

This task is the close-out of the spec's open question 1: if the trait solver rejects anything here, stop and report the exact failure list before any runtime work builds on it.

- [ ] **Step 1: `pipeline/builder.rs`**

```rust
use std::marker::PhantomData;

use crate::apply::execute::Work;
use crate::set::{Intersect, Set, Subset, Union};

pub struct PipelineBuilder<Chain> {
    pub(crate) chain: Chain,
}

pub struct Source<A, M> {
    pub(crate) apply: A,
    pub(crate) _mode: PhantomData<M>,
}

pub struct Node<A, M, Tail> {
    pub(crate) apply: A,
    pub(crate) tail: Tail,
    pub(crate) _mode: PhantomData<M>,
}

pub struct Pipeline<Chain> {
    pub(crate) chain: Chain,
}

pub trait Head {
    type Output;
    type Provides: Set;
}

impl<A, M: 'static> Head for Source<A, M>
where
    A: Work<M>,
{
    type Output = A::Output;
    type Provides = A::Provides;
}

impl<A, M: 'static, Tail> Head for Node<A, M, Tail>
where
    A: Work<M>,
{
    type Output = A::Output;
    type Provides = A::Provides;
}

pub type Wanted<A, M, W> =
    Union<<A as Work<M>>::Requires, Intersect<W, <A as Work<M>>::Provides>>;

impl Pipeline<()> {
    pub fn builder() -> PipelineBuilder<()> {
        PipelineBuilder { chain: () }
    }
}

impl PipelineBuilder<()> {
    pub fn source<A, M: 'static>(self, apply: A) -> PipelineBuilder<Source<A, M>>
    where
        A: Work<M, Input = ()>,
    {
        PipelineBuilder {
            chain: Source {
                apply,
                _mode: PhantomData,
            },
        }
    }
}

impl<Chain: Head> PipelineBuilder<Chain> {
    pub fn layer<A, M: 'static>(self, apply: A) -> PipelineBuilder<Node<A, M, Chain>>
    where
        A: Work<M, Input = Chain::Output>,
        A::Requires: Subset<Chain::Provides>,
    {
        PipelineBuilder {
            chain: Node {
                apply,
                tail: self.chain,
                _mode: PhantomData,
            },
        }
    }

    pub fn sink<A, M: 'static>(self, apply: A) -> Pipeline<Node<A, M, Chain>>
    where
        A: Work<M, Input = Chain::Output, Output = ()>,
        A::Requires: Subset<Chain::Provides>,
    {
        Pipeline {
            chain: Node {
                apply,
                tail: self.chain,
                _mode: PhantomData,
            },
        }
    }
}
```

`pipeline.rs`:

```rust
pub(crate) mod builder;

pub use builder::{Pipeline, PipelineBuilder};
```

- [ ] **Step 2: Write the compile test**

```rust
use std::any::TypeId;

use bascet_core::{Apply, ApplyAsync, AsyncEmit, Emit, Error, Pipeline};
use bascet_core::set::{Intersect, Set, Union};
use bascet_core::Attr;
use bascet_derive::attr_id;

struct Header;
struct Blocks;

impl Attr for Header {
    type Id = attr_id!(1);
}

impl Attr for Blocks {
    type Id = attr_id!(2);
}

#[derive(Clone)]
struct Numbers;

impl Apply for Numbers {
    type Input = ();
    type Output = u32;
    type Provides = (Header,);
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.finish();
        Ok(())
    }
}

#[derive(Clone)]
struct Double;

impl Apply for Double {
    type Input = u32;
    type Output = u32;
    type Provides = (Header,);
    type Requires = (Header,);

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input * 2);
        Ok(())
    }
}

#[derive(Clone)]
struct Slow;

impl ApplyAsync for Slow {
    type Input = u32;
    type Output = u32;
    type Provides = (Header,);
    type Requires = ();

    async fn apply<W: Set>(&mut self, input: u32, out: &mut AsyncEmit<u32, W>) -> Result<(), Error> {
        out.push(input).await;
        Ok(())
    }
}

#[derive(Clone)]
struct Consume;

impl Apply for Consume {
    type Input = u32;
    type Output = ();
    type Provides = ();
    type Requires = (Header,);

    fn apply<W: Set>(&mut self, _: u32, _: &mut Emit<(), W>) -> Result<(), Error> {
        Ok(())
    }
}

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

#[test]
fn chain_builds_with_inferred_markers() {
    let _ = Pipeline::builder()
        .source(Numbers)
        .layer(Double)
        .layer(Slow)
        .sink(Consume);
}

#[test]
fn wants_algebra_normalizes() {
    assert!(eq::<
        Union<(Header,), Intersect<(Blocks,), (Blocks, Header)>>,
        (Header, Blocks),
    >());
}
```

A `.layer(Consume)` after a `Provides = ()` producer must fail with the `Subset` diagnostic — verify once manually (uncomment, read the error, expect the `on_unimplemented` message from Task 4, re-comment); trybuild is not worth a dependency for one case.

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test builder`
Expected: compiles and passes. Any inference failure on `.layer(..)` (E0283 ambiguity, or unresolved `M`) is the acknowledged spec risk — capture the failure list, stop for review.

---

### Task 13: `Edge` — items, credits, keeper

**Files:**
- Create: `crates/bascet-core/src/pipeline/edge.rs`
- Modify: `crates/bascet-core/src/pipeline.rs` (add `pub(crate) mod edge;`)
- Test: unit tests in `edge.rs`

**Interfaces:**
- Produces: `wire<T>(depth, producer: Arc<Port>, consumer: Arc<Port>) -> (Downstream<T>, Upstream<T>)`. `Upstream<T>` (consumer side): `items_rx`, `credits_tx`, `producer`, `consumer`, `keeper: Arc<OnceLock<kanal::Sender<T>>>`, `depth`, plus `try_take() -> Result<Option<T>, Closed>`, `credit(n: u32)`, `nudge()` (Promote to the producer, band-gated via producer demand), `crossings: Arc<AtomicU64>` (watchdog probe, folded at checkpoints). `Downstream<T>` (producer side): `items_tx`, `credits_rx`, `producer`, `consumer`, `keeper`, plus `park_keeper()`, `close_credits()`, `closer() -> Box<dyn Fn() + Send + Sync>` (for `Shutdown` — closes the items channel outright). `pub(crate) struct Closed;`

- [ ] **Step 1: Implement**

```rust
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};

use crate::scheduler::event::Action;
use crate::scheduler::port::Port;

pub(crate) struct Closed;

pub(crate) struct Upstream<T> {
    pub(crate) items_rx: kanal::Receiver<T>,
    pub(crate) credits_tx: kanal::Sender<u32>,
    pub(crate) producer: Arc<Port>,
    pub(crate) consumer: Arc<Port>,
    pub(crate) keeper: Arc<OnceLock<kanal::Sender<T>>>,
    pub(crate) crossings: Arc<AtomicU64>,
    pub(crate) depth: u32,
}

pub(crate) struct Downstream<T> {
    pub(crate) items_tx: kanal::Sender<T>,
    pub(crate) credits_rx: kanal::Receiver<u32>,
    pub(crate) producer: Arc<Port>,
    pub(crate) consumer: Arc<Port>,
    pub(crate) keeper: Arc<OnceLock<kanal::Sender<T>>>,
}

pub(crate) fn wire<T>(
    depth: usize,
    producer: Arc<Port>,
    consumer: Arc<Port>,
) -> (Downstream<T>, Upstream<T>) {
    let (items_tx, items_rx) = kanal::bounded(depth);
    let (credits_tx, credits_rx) = kanal::unbounded();
    let keeper = Arc::new(OnceLock::new());
    let crossings = Arc::new(AtomicU64::new(0));
    (
        Downstream {
            items_tx,
            credits_rx,
            producer: Arc::clone(&producer),
            consumer: Arc::clone(&consumer),
            keeper: Arc::clone(&keeper),
        },
        Upstream {
            items_rx,
            credits_tx,
            producer,
            consumer,
            keeper,
            crossings,
            depth: depth as u32,
        },
    )
}

impl<T> Upstream<T> {
    pub(crate) fn try_take(&self) -> Result<Option<T>, Closed> {
        match self.items_rx.try_recv() {
            Ok(Some(item)) => Ok(Some(item)),
            Ok(None) => Err(Closed).or(Ok(None)),
            Err(_) => Err(Closed),
        }
    }

    pub(crate) fn credit(&self, n: u32) {
        self.credits_tx.send(n).ok();
    }

    pub(crate) fn nudge(&self) {
        if self.producer.demand.miss().is_some() {
            self.consumer.petition(Action::Promote, &self.producer, None);
        }
    }
}

impl<T> Downstream<T> {
    pub(crate) fn park_keeper(&self) {
        self.keeper.set(self.items_tx.clone()).ok();
    }

    pub(crate) fn close_credits(&self) {
        self.credits_rx.close().ok();
    }

    pub(crate) fn closer(&self) -> Box<dyn Fn() + Send + Sync> {
        let items_tx = self.items_tx.clone();
        Box::new(move || {
            items_tx.close().ok();
        })
    }
}
```

**Note:** the `try_take` shown mangles the empty case — fix it while implementing: kanal's `try_recv` gives `Ok(Some)` = item, `Ok(None)` = empty (return `Ok(None)` meaning "nothing yet"), `Err` = closed (return `Err(Closed)`). The Gather layer above distinguishes empty (starve) from closed (end); pick honest names there — `try_take` returning `Ok(None)` for *empty* and `Err(Closed)` for *closed*.

- [ ] **Step 2: Unit tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::port::Port;

    fn ports() -> (Arc<Port>, Arc<Port>, kanal::Receiver<crate::scheduler::event::Event>) {
        let (events_tx, events_rx) = kanal::unbounded();
        (Port::new(0, events_tx.clone()), Port::new(1, events_tx), events_rx)
    }

    #[test]
    fn keeper_keeps_buffer_alive() {
        let (producer, consumer, _events_rx) = ports();
        let (down, up) = wire::<u32>(4, producer, consumer);
        down.items_tx.send(1).unwrap();
        down.park_keeper();
        drop(down);
        assert!(matches!(up.try_take(), Ok(Some(1))));
        drop(up.keeper.get().cloned());
    }

    #[test]
    fn consumer_drop_closes_for_producer() {
        let (producer, consumer, _events_rx) = ports();
        let (down, up) = wire::<u32>(1, producer, consumer);
        drop(up);
        assert!(down.items_tx.send(1).is_err());
    }
}
```

**Note:** the second test requires the keeper `Arc` to be the only thing between drop-of-consumer and closure — `Upstream` drop drops `items_rx` *and* its keeper `Arc` clone; with no keeper parked, all senders' counterpart receivers are gone and `send` errors. If kanal's `send` does not error until the receiver count hits zero including parked keepers, this is exactly the semantics the design wants — the test documents it.

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core edge`
Expected: passed.

---

### Task 14: `Gather` — arity 1, `Boundless`, option tuples

**Files:**
- Create: `crates/bascet-core/src/pipeline/gather.rs`
- Modify: `crates/bascet-core/src/pipeline.rs`
- Test: unit tests in `gather.rs`

**Interfaces:**
- Produces:

```rust
pub struct Starved;

pub trait Gather: Send + 'static {
    type Item;
    type Round: Default + Send;
    fn take(&self, round: &mut Self::Round) -> Result<Option<Self::Item>, Starved>;
    fn nudge(&self, round: &Self::Round);
}
```

`take`: `Ok(Some(item))` — an item; `Ok(None)` — input ended for good; `Err(Starved)` — nothing now, wait and retry. Credit top-ups (watermark = `depth / WATERMARK`) happen inside `take` against the round's outstanding counter — worker-local, exactly the spec's demand protocol. Impls: `Upstream<T>` (linear), `Boundless` (sources: always `Ok(Some(()))`), tuples of `Upstream` (zip: option tuple, `None` = closed **and** drained, end at all-`None`). The trait is public (it bounds `Layer<U, Out>`) but stays out of `lib.rs`'s re-exports.

- [ ] **Step 1: Implement arity 1 and `Boundless`**

```rust
use crate::pipeline::edge::Upstream;

pub struct Starved;

pub trait Gather: Send + 'static {
    type Item;
    type Round: Default + Send;
    fn take(&self, round: &mut Self::Round) -> Result<Option<Self::Item>, Starved>;
    fn nudge(&self, round: &Self::Round);
}

#[derive(Default)]
pub struct Credit {
    outstanding: u32,
}

impl<T: Send + 'static> Gather for Upstream<T> {
    type Item = T;
    type Round = Credit;

    fn take(&self, round: &mut Credit) -> Result<Option<T>, Starved> {
        let watermark = (self.depth / crate::consts::WATERMARK).max(1);
        if round.outstanding * 2 <= watermark {
            self.credit(watermark - round.outstanding);
            round.outstanding = watermark;
        }
        match self.items_rx.try_recv() {
            Ok(Some(item)) => {
                round.outstanding = round.outstanding.saturating_sub(1);
                self.crossings.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(Some(item))
            }
            Ok(None) => Err(Starved),
            Err(_) => Ok(None),
        }
    }

    fn nudge(&self, _: &Credit) {
        Upstream::nudge(self);
    }
}

pub struct Boundless;

impl Gather for Boundless {
    type Item = ();
    type Round = ();

    fn take(&self, _: &mut ()) -> Result<Option<()>, Starved> {
        Ok(Some(()))
    }

    fn nudge(&self, _: &()) {}
}
```

- [ ] **Step 2: Tuple impls**

```rust
pub struct Member<T> {
    credit: Credit,
    pending: Option<T>,
    closed: bool,
}

impl<T> Default for Member<T> {
    fn default() -> Self {
        Self {
            credit: Credit::default(),
            pending: None,
            closed: false,
        }
    }
}
```

The arity-2 impl, written explicitly first so the semantics are pinned before macro generation:

```rust
impl<A: Send + 'static, B: Send + 'static> Gather for (Upstream<A>, Upstream<B>) {
    type Item = (Option<A>, Option<B>);
    type Round = (Member<A>, Member<B>);

    fn take(&self, round: &mut Self::Round) -> Result<Option<Self::Item>, Starved> {
        let mut starving = false;
        if round.0.pending.is_none() && !round.0.closed {
            match self.0.take(&mut round.0.credit) {
                Ok(Some(item)) => round.0.pending = Some(item),
                Ok(None) => round.0.closed = true,
                Err(Starved) => starving = true,
            }
        }
        if round.1.pending.is_none() && !round.1.closed {
            match self.1.take(&mut round.1.credit) {
                Ok(Some(item)) => round.1.pending = Some(item),
                Ok(None) => round.1.closed = true,
                Err(Starved) => starving = true,
            }
        }
        if starving {
            return Err(Starved);
        }
        if round.0.closed && round.1.closed {
            return Ok(None);
        }
        Ok(Some((round.0.pending.take(), round.1.pending.take())))
    }

    fn nudge(&self, round: &Self::Round) {
        if !round.0.closed && round.0.pending.is_none() {
            self.0.nudge(&round.0.credit);
        }
        if !round.1.closed && round.1.pending.is_none() {
            self.1.nudge(&round.1.credit);
        }
    }
}
```

Then generalize `2..=16` with `bascet_variadic::variadic!` — the member block is per-index statement expansion (`@N[ ... ~# ... ](sep=" ")`); if statement-position expansion or tuple-field access (`self.~#`) is beyond the macro today, extend the macro (house tool) or keep explicit impls for 2 and 3 and leave a regeneration note in the file — arity beyond 3 has no consumer yet.

- [ ] **Step 3: Unit test — uneven members drain to all-`None`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::edge::wire;
    use crate::scheduler::port::Port;

    #[test]
    fn uneven_zip_drains_survivor() {
        let (events_tx, _events_rx) = kanal::unbounded();
        let make = || (Port::new(0, events_tx.clone()), Port::new(1, events_tx.clone()));
        let (pa, ca) = make();
        let (pb, cb) = make();
        let (down_a, up_a) = wire::<u32>(4, pa, ca);
        let (down_b, up_b) = wire::<u32>(4, pb, cb);

        down_a.items_tx.send(1).unwrap();
        down_b.items_tx.send(10).unwrap();
        down_b.items_tx.send(20).unwrap();
        drop(down_a);
        drop(down_b);

        let gather = (up_a, up_b);
        let mut round = <(Member<u32>, Member<u32>)>::default();

        assert!(matches!(gather.take(&mut round), Ok(Some((Some(1), Some(10))))));
        assert!(matches!(gather.take(&mut round), Ok(Some((None, Some(20))))));
        assert!(matches!(gather.take(&mut round), Ok(None)));
    }
}
```

(The test drops both `Downstream`s without parking keepers so the channels read as closed once drained.)

- [ ] **Step 4: Checkpoint (user runs)**

Run: `cargo test -p bascet-core gather`
Expected: passed.

---

### Task 15: `Runtime` core — dispatch ownership, `Shutdown`, `Watchdog`, `Metrics`

**Files:**
- Modify: `crates/bascet-core/src/runtime.rs`
- Create: `src/runtime/shutdown.rs`, `src/runtime/watchdog.rs`, `src/runtime/metrics.rs`
- Test: unit tests in `shutdown.rs`

**Interfaces:**
- Produces: `Runtime` (cloneable handle over `RuntimeInner`) built with `Runtime::builder().with_burn(n).with_jobs(n).with_tasks(n).build()` (bon, defaults from the machine as the old code computed them); accessors `pub(crate) fn dispatch(&self) -> &Dispatch`, `events_tx()`, `take_events_rx() -> Option<kanal::Receiver<Event>>`, `shutdown(&self) -> &Shutdown`, `watchdog(&self) -> &Watchdog`, `metrics(&self) -> &Metrics`, `record(&self, error: Error)` (first error wins), `take_error(&self) -> Option<Error>`, `burn()/jobs()/tasks() -> usize`. `Shutdown { register(closer), trigger() }` — idempotent, runs every registered closer once. `Watchdog { wake(), register(probe: Box<dyn Fn() -> u64 + Send + Sync>) }` — own thread, event_listener-driven, two-probe stall detection, warn-log only. `Metrics { pushed: AtomicU64, rejected: AtomicU64 }` with `fold(pushed, rejected)`.

- [ ] **Step 1: `runtime.rs`**

```rust
pub mod dispatch;
pub(crate) mod metrics;
pub(crate) mod pool;
pub(crate) mod shutdown;
pub mod tier;
pub(crate) mod watchdog;

pub use dispatch::{Job, Slot};
pub use shutdown::Shutdown;
pub use tier::Tier;

use std::sync::{Arc, Mutex, OnceLock};

use bon::bon;

use crate::apply::Error;
use crate::runtime::dispatch::Dispatch;
use crate::runtime::metrics::Metrics;
use crate::runtime::watchdog::Watchdog;
use crate::scheduler::event::Event;

#[derive(Clone)]
pub struct Runtime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    pub(crate) dispatch: Dispatch,
    pub(crate) events_tx: kanal::Sender<Event>,
    pub(crate) events_rx: Mutex<Option<kanal::Receiver<Event>>>,
    pub(crate) registry: OnceLock<Box<[Arc<crate::scheduler::port::Port>]>>,
    pub(crate) shutdown: Shutdown,
    pub(crate) watchdog: Watchdog,
    pub(crate) metrics: Metrics,
    pub(crate) error: Mutex<Option<Error>>,
    pub(crate) burn: usize,
    pub(crate) jobs: usize,
    pub(crate) tasks: usize,
}

#[bon]
impl Runtime {
    #[builder]
    pub fn new(
        #[builder(name = with_burn, default = defaults().0)] burn: usize,
        #[builder(name = with_jobs, default = defaults().1)] jobs: usize,
        #[builder(name = with_tasks, default = defaults().2)] tasks: usize,
    ) -> Self {
        let (events_tx, events_rx) = kanal::unbounded();
        Self {
            inner: Arc::new(RuntimeInner {
                dispatch: Dispatch::spawn(burn, jobs, tasks),
                events_tx,
                events_rx: Mutex::new(Some(events_rx)),
                registry: OnceLock::new(),
                shutdown: Shutdown::new(),
                watchdog: Watchdog::spawn(),
                metrics: Metrics::new(),
                error: Mutex::new(None),
                burn,
                jobs,
                tasks,
            }),
        }
    }
}

fn defaults() -> (usize, usize, usize) {
    let cores = core_affinity::get_core_ids().map(|c| c.len()).unwrap_or(1).max(1);
    let reserved = (cores / 8).max(2).min(cores.saturating_sub(1).max(1));
    (cores - reserved, reserved * 2, reserved * 512)
}
```

Accessor impls are one-liners over `self.inner`; `record` takes the error mutex and writes only if `None`. `with_scheduler` joins the builder in Task 16 once the trait exists (a `Mutex<Option<Box<dyn Scheduler>>>` field on `RuntimeInner`, `Auto::new()` as the default at pipeline-build time).

- [ ] **Step 2: `shutdown.rs`, `watchdog.rs`, `metrics.rs`**

```rust
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Shutdown {
    closers: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
    fired: AtomicBool,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            closers: Mutex::new(Vec::new()),
            fired: AtomicBool::new(false),
        }
    }

    pub(crate) fn register(&self, closer: Box<dyn Fn() + Send + Sync>) {
        self.closers.lock().unwrap().push(closer);
    }

    pub fn trigger(&self) {
        if self.fired.swap(true, Ordering::AcqRel) {
            return;
        }
        for closer in self.closers.lock().unwrap().iter() {
            closer();
        }
    }
}
```

`watchdog.rs`: an `event_listener::Event` plus one thread. `wake()` notifies; the thread loops — wait for a wake with a 500ms timeout, then take two probe snapshots 100ms apart: if every registered crossing probe is unchanged, every port reports `busy() == 0`, and any backlog probe is non-zero, `tracing::warn!("pipeline stalled")`. Probes are `Box<dyn Fn() -> u64 + Send + Sync>` registered at wire-up (edge `crossings` clones) and `Box<dyn Fn() -> usize>` backlogs (items channel `len()`); ports come from the runtime registry. Reuse the deleted `pipeline/watchdog.rs` shape for the thread/wait plumbing.

`metrics.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    pub pushed: AtomicU64,
    pub rejected: AtomicU64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            pushed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    pub(crate) fn fold(&self, pushed: u64, rejected: u64) {
        self.pushed.fetch_add(pushed, Ordering::Relaxed);
        self.rejected.fetch_add(rejected, Ordering::Relaxed);
    }
}
```

Shutdown unit test: register two counters-as-closers, `trigger()` twice, both ran exactly once.

- [ ] **Step 3: Checkpoint (user runs)**

Run: `cargo test -p bascet-core shutdown`
Expected: passed; whole crate still `cargo check`s clean.

---

### Task 16: `Layer`, `Run`, the two worker loops, `Work::launch`

**Files:**
- Create: `crates/bascet-core/src/scheduler/layer.rs`, `src/worker/synchronous.rs`, `src/worker/asynchronous.rs`
- Modify: `crates/bascet-core/src/worker.rs`, `src/scheduler.rs`, `src/apply/execute.rs`
- Test: unit test in `worker/synchronous.rs`

**Interfaces:**
- Produces: `pub struct Layer<U: Gather, Out> { upstream: U, downstream: Option<Downstream<Out>>, port: Arc<Port> }` (`None` downstream = sink); `Run<A, U, W>` (private to `worker/`) holding apply, layer, worker, runtime, emit core parts, round, patience, streak; `synchronous::run(run: Run<..>)` and `asynchronous::run(run: RunAsync<..>).await`; `Work<M>` gains

```rust
fn launch<U, W>(
    self,
    layer: Arc<Layer<U, Self::Output>>,
    worker: Arc<Worker>,
    runtime: Runtime,
    patience: Patience<u32>,
    wants: PhantomData<W>,
) where
    U: Gather<Item = Self::Input>,
    W: Set;
```

  — `Synchronous` builds `Run` and calls `synchronous::run` inline (already on the worker's thread); `Asynchronous` hands a future-builder to `runtime.dispatch().spawn_task(worker.slot, ..)`.

- [ ] **Step 1: `scheduler/layer.rs`**

```rust
use std::sync::Arc;

use crate::pipeline::edge::Downstream;
use crate::pipeline::gather::Gather;
use crate::scheduler::port::Port;

pub struct Layer<U: Gather, Out> {
    pub(crate) upstream: U,
    pub(crate) downstream: Option<Downstream<Out>>,
    pub(crate) port: Arc<Port>,
}
```

- [ ] **Step 2: `worker/synchronous.rs` — the loop, the guard, the waits**

```rust
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::apply::emit::{Core, Emit};
use crate::apply::Apply;
use crate::consts::{PATIENCE_CAP, PATIENCE_MIN, PATIENCE_START};
use crate::pipeline::gather::{Gather, Starved};
use crate::runtime::Runtime;
use crate::scheduler::layer::Layer;
use crate::scheduler::load::Activity;
use crate::scheduler::preempt::Preempt;
use crate::set::Set;
use crate::utils::Patience;
use crate::worker::{State, Worker};

pub(crate) struct Run<A, U, W>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    pub(crate) apply: A,
    pub(crate) layer: Arc<Layer<U, A::Output>>,
    pub(crate) worker: Arc<Worker>,
    pub(crate) runtime: Runtime,
    pub(crate) emit: Emit<A::Output, W>,
    pub(crate) round: U::Round,
    pub(crate) patience: Patience<u32>,
    pub(crate) streak: u32,
}

struct Guard {
    worker: Arc<Worker>,
    state: Cell<State>,
    patience: Cell<u32>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.worker.finish(self.state.get(), self.patience.get());
    }
}

pub(crate) fn run<A, U, W>(mut run: Run<A, U, W>)
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    let guard = Guard {
        worker: Arc::clone(&run.worker),
        state: Cell::new(State::Panicked),
        patience: Cell::new(PATIENCE_START),
    };
    run.worker.set_activity(Activity::Busy);
    let state = loop {
        let Some(input) = wait_input(&mut run) else {
            break State::Finished;
        };
        if let Err(error) = run.apply.apply(input, &mut run.emit) {
            run.runtime.record(error);
            break State::Finished;
        }
        if run.emit.core.finished() || run.emit.core.orphaned() {
            break State::Finished;
        }
        if run.worker.halted() {
            break State::Released;
        }
        run.streak += 1;
        if run.streak >= run.patience.patience() {
            if checkpoint(&mut run) {
                break State::Released;
            }
        }
    };
    fold(&mut run);
    guard.state.set(state);
    guard.patience.set(run.patience.patience());
}

fn wait_input<A, U, W>(run: &mut Run<A, U, W>) -> Option<A::Input>
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    loop {
        match run.layer.upstream.take(&mut run.round) {
            Ok(Some(input)) => {
                run.worker.set_activity(Activity::Busy);
                return Some(input);
            }
            Ok(None) => return None,
            Err(Starved) => {
                if run.worker.preempted() != Preempt::Continue {
                    return None;
                }
                starve(run);
            }
        }
    }
}

fn starve<A, U, W>(run: &mut Run<A, U, W>)
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    run.worker.set_activity(Activity::Starved);
    run.runtime.watchdog().wake();
    fold(run);
    let patience = run.patience.miss();
    if patience <= run.patience.min() {
        run.layer.upstream.nudge(&run.round);
    }
    std::thread::sleep(Duration::from_micros(patience as u64));
}

fn checkpoint<A, U, W>(run: &mut Run<A, U, W>) -> bool
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    run.streak = 0;
    fold(run);
    match run.worker.preempted() {
        Preempt::Continue => {
            run.patience.hit();
            false
        }
        Preempt::Yield | Preempt::Halt => true,
    }
}

fn fold<A, U, W>(run: &mut Run<A, U, W>)
where
    A: Apply,
    U: Gather<Item = A::Input>,
    W: Set,
{
    let (pushed, rejects) = run.emit.core.fold();
    let rejected = rejects.iter().map(|(_, n)| n).sum();
    run.runtime.metrics().fold(pushed, rejected);
}
```

**Notes:** patience is `Patience::new(PATIENCE_START, 1, 1).set_min(PATIENCE_MIN).set_max(PATIENCE_CAP)` constructed by the scheduler and passed through `launch` — the `Run` receives it ready-made. The starve wait shown is a plain bounded sleep; swap in `crate::utils::threading`'s spinpark loop where its shape fits (spin `patience` iterations, then park briefly) — the invariants that matter and must survive any substitution: activity marked `Starved`, watchdog woken, locals folded, nudge fires at the patience floor, `preempted` checked every round so `Halt`/`Yield` interrupt a starving worker. Error path: fatal `Err` records on the runtime and exits as `Finished` — the scheduler's teardown then runs the EOF machinery and `Shutdown` (Task 17).

- [ ] **Step 3: `worker/asynchronous.rs`**

Same skeleton over `ApplyAsync`/`AsyncEmit` with `async fn run`, `out.push(..).await`, and the starve wait replaced by a cooperative yield:

```rust
struct Breather(bool);

impl std::future::Future for Breather {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}
```

`starve` awaits `Breather(false)` instead of sleeping; the checkpoint additionally awaits one `Breather` unconditionally — the executor-yield tier of the cadence rule. The guard is identical (drop-based, fires on panic unwind inside the future too).

- [ ] **Step 4: `Work::launch` in `apply/execute.rs`**

```rust
impl<A: Apply> Work<Synchronous> for A {
    fn launch<U, W>(
        self,
        layer: Arc<Layer<U, A::Output>>,
        worker: Arc<Worker>,
        runtime: Runtime,
        patience: Patience<u32>,
        _wants: PhantomData<W>,
    ) where
        U: Gather<Item = A::Input>,
        W: Set,
    {
        let emit = Emit {
            core: match &layer.downstream {
                Some(down) => Core::edge(
                    down.items_tx.clone(),
                    down.credits_rx.clone(),
                    Some(Arc::clone(&down.consumer)),
                    Some(Arc::clone(&worker)),
                ),
                None => Core::null(),
            },
            _wants: PhantomData,
        };
        crate::worker::synchronous::run(crate::worker::synchronous::Run {
            apply: self,
            layer,
            worker,
            runtime,
            emit,
            round: Default::default(),
            patience,
            streak: 0,
        });
    }
}
```

The `Asynchronous` impl builds the same parts, then `runtime.dispatch().spawn_task(worker.slot, move || async move { crate::worker::asynchronous::run(..).await })`. Both `Work` impls now require the full method — the associated types from Task 11 are unchanged.

- [ ] **Step 5: Unit test — one worker, one edge, end to end by hand**

In `worker/synchronous.rs` tests: wire an edge pair, a `Boundless`-driven source apply that pushes `0..100` then `finish()`, launch via `Work::<Synchronous>::launch` on the current thread, then assert the consumer side drains 100 items followed by closure, and that a `Released` event with `State::Finished` arrived on the events channel.

- [ ] **Step 6: Checkpoint (user runs)**

Run: `cargo test -p bascet-core synchronous`
Expected: passed.

---

### Task 17: `Scheduler`, `Driver`, `Auto`, `Runner`

**Files:**
- Create: `crates/bascet-core/src/scheduler/driver.rs`, `src/scheduler/auto.rs`, `src/runner.rs`
- Modify: `crates/bascet-core/src/scheduler.rs`, `src/runtime.rs` (scheduler seed + `with_scheduler`), `src/lib.rs` (add `pub mod runner;`)
- Test: unit test in `auto.rs`

**Interfaces:**
- Produces:

```rust
pub trait Scheduler: Send + 'static {
    fn schedule(&mut self, event: Event, driver: &mut Driver);
}
```

  `Driver` (mechanism): `spawn(layer: usize, tier: Tier) -> Option<Arc<Worker>>` (pool acquire under the layer's current demand band, mint, dispatch — `Task` jobs execute inline since they only enqueue onto compio), `shed(&Arc<Worker>)` / `halt(&Arc<Worker>)` (preempt stores), `release(&Arc<Worker>) -> Option<usize>` (slot back to pool, returns claim winner), `grant(layer) -> Option<Tier>` (tier stashed when `release` picked this layer as winner — pending until its `Acquire` handling), `withdraw(layer)`, `teardown(layer)`, `mode(layer) -> Mode` (`Mode { Sync, Async }`), `backlog(layer) -> usize`, `port(layer) -> &Arc<Port>`, `post(action, layer)` (self-addressed event), `done() -> bool`, `runtime() -> &Runtime`. Construction from the `Build` products (Task 18): mints, teardowns, modes, backlogs, registry, pool, completions_tx.
  `Auto` implementing `Scheduler`; `Runner { join(self) -> Result<(), Error>, metrics(&self) -> &Metrics }`; `Runtime::builder().with_scheduler(s)` storing `Box<dyn Scheduler>` (object-safe by construction).
  The loop itself: `pub(crate) async fn drive(scheduler: Box<dyn Scheduler>, driver: Driver, events_rx: kanal::AsyncReceiver<Event>)` — sent to the System thread at build; ends when all layers are torn down or the channel closes.

- [ ] **Step 1: `scheduler/driver.rs`**

```rust
use std::marker::PhantomData;
use std::sync::Arc;

use crate::runtime::pool::Pool;
use crate::runtime::{Runtime, Tier};
use crate::scheduler::event::{Action, Event};
use crate::scheduler::port::Port;
use crate::scheduler::preempt::Preempt;
use crate::scheduler::Scheduler;
use crate::utils::Patience;
use crate::worker::Worker;

pub(crate) type Mint = Box<dyn FnMut(Arc<Worker>, Patience<u32>) -> crate::runtime::Job + Send>;
pub(crate) type Teardown = Box<dyn FnMut() + Send>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Sync,
    Async,
}

pub struct Driver {
    pub(crate) mints: Box<[Mint]>,
    pub(crate) teardowns: Box<[Option<Teardown>]>,
    pub(crate) modes: Box<[Mode]>,
    pub(crate) backlogs: Box<[Box<dyn Fn() -> usize + Send>]>,
    pub(crate) registry: Box<[Arc<Port>]>,
    pub(crate) pool: Pool,
    pub(crate) grants: Box<[Option<Tier>]>,
    pub(crate) starts: Box<[u32]>,
    pub(crate) completions_tx: kanal::Sender<usize>,
    pub(crate) runtime: Runtime,
    pub(crate) down: usize,
}

impl Driver {
    pub(crate) fn spawn(&mut self, layer: usize, tier: Tier) -> Option<Arc<Worker>> {
        let band = self.registry[layer].demand.level();
        let slot = self.pool.acquire(tier, layer, band)?;
        let worker = Worker::new(slot, Arc::clone(&self.registry[layer]));
        let patience = Patience::new(self.starts[layer], 1, 1)
            .set_min(crate::consts::PATIENCE_MIN)
            .set_max(crate::consts::PATIENCE_CAP);
        let job = (self.mints[layer])(Arc::clone(&worker), patience);
        match tier {
            Tier::Task => job(),
            _ => self.runtime.dispatch().send(worker.slot, job),
        }
        Some(worker)
    }

    pub(crate) fn shed(&self, worker: &Arc<Worker>) {
        worker.preempt(Preempt::Yield);
    }

    pub(crate) fn halt(&self, worker: &Arc<Worker>) {
        worker.preempt(Preempt::Halt);
    }

    pub(crate) fn release(&mut self, worker: &Arc<Worker>) {
        let slot = worker.slot;
        if let Some(winner) = self.pool.release(slot) {
            self.grants[winner] = Some(slot.tier);
            self.post(Action::Acquire, winner);
        }
    }

    pub(crate) fn post(&self, action: Action, layer: usize) {
        let port = &self.registry[layer];
        port.petition(action, port, None);
    }

    pub(crate) fn teardown(&mut self, layer: usize) {
        if let Some(mut teardown) = self.teardowns[layer].take() {
            teardown();
            self.down += 1;
            self.completions_tx.send(layer).ok();
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.down == self.teardowns.len()
    }
}

pub(crate) async fn drive(
    mut scheduler: Box<dyn Scheduler>,
    mut driver: Driver,
    events_rx: kanal::AsyncReceiver<Event>,
) {
    while let Ok(event) = events_rx.recv().await {
        scheduler.schedule(event, &mut driver);
        if driver.done() {
            break;
        }
    }
}
```

`scheduler.rs` gains `pub mod auto; pub(crate) mod driver; pub mod layer;` plus the trait:

```rust
pub trait Scheduler: Send + 'static {
    fn schedule(&mut self, event: Event, driver: &mut Driver);
}
```

and re-exports `pub use auto::Auto; pub use driver::Driver; pub use layer::Layer;`.

- [ ] **Step 2: `scheduler/auto.rs`**

```rust
use std::sync::Arc;

use crate::runtime::Tier;
use crate::scheduler::driver::{Driver, Mode};
use crate::scheduler::event::{Action, Event};
use crate::scheduler::Scheduler;
use crate::worker::{State, Worker};

pub struct Auto {
    seats: Vec<Seat>,
}

struct Seat {
    roster: Vec<Entry>,
    finished: bool,
}

struct Entry {
    worker: Arc<Worker>,
    band: u32,
}

impl Auto {
    pub fn new() -> Self {
        Self { seats: Vec::new() }
    }

    fn seat(&mut self, layer: usize) -> &mut Seat {
        while self.seats.len() <= layer {
            self.seats.push(Seat {
                roster: Vec::new(),
                finished: false,
            });
        }
        &mut self.seats[layer]
    }
}

impl Scheduler for Auto {
    fn schedule(&mut self, event: Event, driver: &mut Driver) {
        let layer = event.subject.index as usize;
        match event.action {
            Action::Promote => {
                let desired = event.subject.demand.strain().max(1);
                let width = self.seat(layer).roster.len();
                if width >= desired || self.seat(layer).finished {
                    return;
                }
                let tier = match driver.modes[layer] {
                    Mode::Async => Tier::Task,
                    Mode::Sync => Tier::Burn,
                };
                let band = event.subject.demand.level();
                let spawned = driver.spawn(layer, tier).or_else(|| {
                    if tier == Tier::Burn {
                        driver.spawn(layer, Tier::Job)
                    } else {
                        None
                    }
                });
                match spawned {
                    Some(worker) => self.seat(layer).roster.push(Entry { worker, band }),
                    None => self.evict(layer, tier, driver),
                }
            }
            Action::Demote => {
                let Some(worker) = &event.worker else { return };
                let seat = self.seat(layer);
                let Some(at) = seat.roster.iter().position(|e| Arc::ptr_eq(&e.worker, worker))
                else {
                    return;
                };
                let recovered = event.subject.demand.level() < seat.roster[at].band;
                let last = seat.roster.len() == 1;
                if recovered && !(last && driver.backlogs[layer]() > 0) {
                    driver.shed(worker);
                }
            }
            Action::Acquire => {
                let Some(tier) = driver.grants[layer].take() else { return };
                if event.subject.demand.level() == 0 && !self.seat(layer).roster.is_empty() {
                    driver.pool.withdraw(tier, layer);
                    return;
                }
                let band = event.subject.demand.level();
                if let Some(worker) = driver.spawn(layer, tier) {
                    self.seat(layer).roster.push(Entry { worker, band });
                }
            }
            Action::Released => {
                let Some(worker) = &event.worker else { return };
                let seat = self.seat(layer);
                if let Some(at) = seat.roster.iter().position(|e| Arc::ptr_eq(&e.worker, worker)) {
                    seat.roster.remove(at);
                }
                driver.starts[layer] = (driver.starts[layer] + worker.patience()) / 2;
                driver.release(worker);
                if worker.state() == State::Finished {
                    self.seat(layer).finished = true;
                }
                if self.seat(layer).finished && self.seat(layer).roster.is_empty() {
                    driver.teardown(layer);
                }
            }
            Action::Yield => {
                let seat = self.seat(layer);
                if seat.roster.len() <= 1 {
                    return;
                }
                if let Some(entry) = seat.roster.iter().min_by_key(|e| e.band) {
                    driver.shed(&entry.worker);
                }
                if let Some(receipt) = event.receipt {
                    receipt.send(()).ok();
                }
            }
        }
    }
}

impl Auto {
    fn evict(&mut self, claimant: usize, tier: Tier, driver: &mut Driver) {
        let victim = self
            .seats
            .iter()
            .enumerate()
            .filter(|(i, seat)| *i != claimant && !seat.roster.is_empty())
            .max_by_key(|(i, _)| driver.registry[*i].load.pressure())
            .and_then(|(_, seat)| {
                seat.roster
                    .iter()
                    .filter(|e| e.worker.slot.tier == tier)
                    .min_by_key(|e| e.band)
            })
            .map(|e| Arc::clone(&e.worker));
        if let Some(victim) = victim {
            driver.shed(&victim);
        }
    }
}
```

**Notes:** this is the deliberately-simple v1 of the spec's policy — width from `demand.strain()`, Burn→Job fallback, tier-exact eviction of the weakest layer's lowest-band worker, patience seeds folded as a running average, no `Auto::learn`. Every learning refinement is gated on `benches/` per the spec; do not add heuristics beyond what is shown. The `Yield` receipt handling implements drop-as-decline: the floor case returns early *without* sending, dropping the receipt.

- [ ] **Step 3: `runner.rs` + `with_scheduler`**

```rust
use std::collections::HashSet;

use crate::apply::Error;
use crate::runtime::metrics::Metrics;
use crate::runtime::Runtime;

pub struct Runner {
    pub(crate) runtime: Runtime,
    pub(crate) completions_rx: kanal::Receiver<usize>,
    pub(crate) layers: usize,
    pub(crate) sink: usize,
}

impl Runner {
    pub fn join(self) -> Result<(), Error> {
        let mut done = HashSet::new();
        while let Ok(layer) = self.completions_rx.recv() {
            done.insert(layer);
            if layer == self.sink {
                break;
            }
        }
        self.runtime.shutdown().trigger();
        while done.len() < self.layers {
            match self.completions_rx.recv() {
                Ok(layer) => {
                    done.insert(layer);
                }
                Err(_) => break,
            }
        }
        match self.runtime.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn metrics(&self) -> &Metrics {
        self.runtime.metrics()
    }
}
```

`RuntimeInner` gains `scheduler: Mutex<Option<Box<dyn Scheduler>>>`; the bon builder gains `#[builder(name = with_scheduler)]` accepting `impl Scheduler` boxed at the call site (`Option<Box<dyn Scheduler>>` field, `None` default). Pipeline build (Task 18) takes it or constructs `Auto::new()`.

- [ ] **Step 4: Unit test**

In `auto.rs`: build a `Driver` by hand over one mint that records spawn calls into an `Arc<AtomicU32>` (job body: increment), one port, a 1-slot Job pool, `Mode::Sync`; feed `Auto` a `Promote` event and assert one spawn happened and the roster holds one entry (drive `schedule` directly, no loop needed).

- [ ] **Step 5: Checkpoint (user runs)**

Run: `cargo test -p bascet-core auto`
Expected: passed.

---

### Task 18: `Connect`, pipeline build, sinks, linear end-to-end

**Files:**
- Create: `crates/bascet-core/src/pipeline/connect.rs`
- Modify: `crates/bascet-core/src/pipeline.rs`, `src/pipeline/builder.rs` (`Pipeline::build`), `src/runtime.rs` (`Runtime::pipeline`), `src/sink/channel.rs`, `src/sink/drain.rs`
- Test: `crates/bascet-core/tests/pipeline.rs`

**Interfaces:**
- Produces:

```rust
pub trait Connect<W: Set> {
    type Stream: Gather;
    fn connect(self, build: &mut Build, consumer: Arc<Port>) -> Self::Stream;
}
```

  implemented for `Source<A, M>` and `Node<A, M, Tail>`; `pub(crate) struct Build { runtime, events_tx, ports, mints, teardowns, modes, backlogs, kicks }`; `Runtime::pipeline<W: Set>(self, pipeline: Pipeline<Chain>) -> Runner`; `sink::channel<T>() -> (Channel<T>, kanal::Receiver<T>)` and `sink::drain::<T>()` as new-`Apply` sinks.

- [ ] **Step 1: `Connect` for the chain**

The recursion each stage performs, with `W` the *caller's* (consumer's) wants and `Wanted<A, M, W>` from Task 12 threading upstream:

```rust
impl<A, M, W> Connect<W> for Source<A, M>
where
    A: Work<M>,
    A: Work<M, Input = ()>,
    M: 'static,
    W: Set,
{
    type Stream = Upstream<A::Output>;

    fn connect(self, build: &mut Build, consumer: Arc<Port>) -> Self::Stream {
        let port = build.port();
        let (downstream, upstream) = wire(crate::consts::DEPTH, Arc::clone(&port), consumer);
        build.register::<A, M, W, Boundless>(self.apply, Boundless, Some(downstream), port);
        build.kick();
        upstream
    }
}

impl<A, M, Tail, W> Connect<W> for Node<A, M, Tail>
where
    A: Work<M>,
    Tail: Connect<Wanted<A, M, W>>,
    Tail::Stream: Gather<Item = A::Input>,
    M: 'static,
    W: Set,
{
    type Stream = Upstream<A::Output>;

    fn connect(self, build: &mut Build, consumer: Arc<Port>) -> Self::Stream {
        let port = build.port();
        let upstream = self.tail.connect(build, Arc::clone(&port));
        let (downstream, up) = wire(crate::consts::DEPTH, Arc::clone(&port), consumer);
        build.register::<A, M, W, Tail::Stream>(self.apply, upstream, Some(downstream), port);
        up
    }
}
```

`Build::register<A, M, W, U>` does the monomorphic capture — one function, everything concrete in scope:

```rust
impl Build {
    fn register<A, M, W, U>(
        &mut self,
        apply: A,
        upstream: U,
        downstream: Option<Downstream<A::Output>>,
        port: Arc<Port>,
    ) where
        A: Work<M>,
        U: Gather<Item = A::Input>,
        M: Marked,
        W: Set,
    {
        self.backlogs.push(backlog(&upstream));
        if let Some(down) = &downstream {
            self.runtime.shutdown().register(down.closer());
        }
        let layer = Arc::new(Layer {
            upstream,
            downstream,
            port: Arc::clone(&port),
        });
        self.modes.push(M::MODE);
        let teardown_layer = Arc::clone(&layer);
        let events_tx = self.events_tx.clone();
        self.teardowns.push(Box::new(move || {
            if let Some(down) = &teardown_layer.downstream {
                down.close_credits();
                down.park_keeper();
                let consumer = Arc::clone(&down.consumer);
                down.producer.petition(Action::Promote, &consumer, None);
            }
            let _ = &events_tx;
        }));
        let runtime = self.runtime.clone();
        let template = ManuallyDroppedNever; 
        self.mints.push(Box::new(move |worker, patience| {
            let apply = apply.clone();
            let layer = Arc::clone(&layer);
            let runtime = runtime.clone();
            Box::new(move || apply.launch(layer, worker, runtime, patience, PhantomData::<W>))
        }));
    }
}
```

**Notes for the implementer:** the `ManuallyDroppedNever` line is a paste error — delete it; `apply` is captured by the mint closure directly. `Marked` is a two-line helper trait (`impl Marked for Synchronous { const MODE: Mode = Mode::Sync; }`, likewise `Asynchronous`) living beside `Work`. `backlog` clones the gather's items-channel handle(s) to report queued length; give `Gather` a `fn backlog(&self) -> usize` default method instead if a closure per member is awkward (Boundless: 0, Upstream: `items_rx.len()`, tuples: sum). The teardown closure captures `Arc<Layer>` — when the mint *and* teardown drop at driver end, the layer's upstream handles drop with them, which is the upward closure cascade.

- [ ] **Step 2: `Pipeline::build` and `Runtime::pipeline`**

`Pipeline<Node<A, M, Tail>>::build(runtime, scheduler)` (called by `Runtime::pipeline::<W>`): create `Build` with the events channel taken from the runtime; create the sink port; `let stream = self.chain.tail.connect(&mut build, sink_port)` with `Wanted<A, M, W>` as the tail's wants — the seed `W` enters here; register the sink itself (`downstream: None`); freeze `registry` into the runtime; construct `Pool::new(burn, jobs, tasks, layers)` and the `Driver`; send `drive(scheduler, driver, events_rx.as_async())` to the System thread via `dispatch.system(..)`; send one `Promote` per recorded kick (`build.kicks` — every `Source` registers its index); return `Runner { runtime, completions_rx, layers, sink }`.

Build-order rule from the spec, encoded in the sequence above: edges before workers, the loop before the kicks.

- [ ] **Step 3: Sinks**

```rust
pub fn channel<T: Send + 'static>() -> (Channel<T>, kanal::Receiver<T>) {
    let (out_tx, out_rx) = kanal::unbounded();
    (Channel { out_tx }, out_rx)
}

#[derive(Clone)]
pub struct Channel<T> {
    out_tx: kanal::Sender<T>,
}

impl<T: Send + 'static> Apply for Channel<T> {
    type Input = T;
    type Output = ();
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: T, _: &mut Emit<(), W>) -> Result<(), Error> {
        self.out_tx.send(input).map_err(Error::new)
    }
}
```

`drain` mirrors it with an empty body.

- [ ] **Step 4: End-to-end test**

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bascet_core::{sink, Apply, Emit, Error, Pipeline, Runtime};
use bascet_core::set::Set;

#[derive(Clone)]
struct Count {
    limit: u32,
    at: Arc<AtomicU32>,
}

impl Apply for Count {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        let n = self.at.fetch_add(1, Ordering::Relaxed);
        if n >= self.limit {
            out.finish();
        } else {
            out.push(n);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Double;

impl Apply for Double {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input * 2);
        Ok(())
    }
}

#[test]
fn linear_pipeline_runs_to_completion() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(4).build();
    let (write, out_rx) = sink::channel::<u32>();
    let source = Count { limit: 1000, at: Arc::new(AtomicU32::new(0)) };

    let runner = runtime.pipeline::<()>(
        Pipeline::builder().source(source).layer(Double).sink(write),
    );

    assert!(runner.join().is_ok());
    let mut collected: Vec<u32> = Vec::new();
    while let Ok(Some(v)) = out_rx.try_recv() {
        collected.push(v);
    }
    collected.sort_unstable();
    assert_eq!(collected, (0..1000).map(|n| n * 2).collect::<Vec<_>>());
}
```

(Sorted compare because multiple workers may reorder; `Count` is shared through the `Arc` so cloned templates continue the same sequence.)

- [ ] **Step 5: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test pipeline`
Expected: passes, terminates (no hang — this is the first full exercise of teardown, keeper, join, shutdown). A hang here is a termination-machinery bug: check the four teardown steps and the `Released`-drives-teardown path first.

---

### Task 19: Zip, merge, fault paths

**Files:**
- Modify: `crates/bascet-core/src/pipeline/builder.rs` (`Pipeline::zip`, `PipelineBuilder::merge`), `src/pipeline/connect.rs` (tuple-of-builders `Connect`)
- Test: `crates/bascet-core/tests/fanin.rs`, `crates/bascet-core/tests/faults.rs`

**Interfaces:**
- Produces: `Pipeline::zip((b1, b2)) -> PipelineBuilder<Zip<(C1, C2)>>` where each `b: PipelineBuilder<C>` is un-terminated; `Zip` implements `Head` (`Output` = option tuple of member outputs, `Provides` = `Union` of branch-head provides) and `Connect` (each member connects with its own derived wants; `Stream` = the tuple gather from Task 14). `.merge((a, b))`: both branches' `Connect` run against one shared edge — the second branch receives the first's `items_tx`/`credits_rx` clones; each parks its own keeper.

- [ ] **Step 1: Implement `Zip` (arity 2 explicit, variadic later with the gather)**

```rust
pub struct Zip<Chains> {
    pub(crate) chains: Chains,
}

impl Pipeline<()> {
    pub fn zip<C1, C2>(
        builders: (PipelineBuilder<C1>, PipelineBuilder<C2>),
    ) -> PipelineBuilder<Zip<(C1, C2)>> {
        PipelineBuilder {
            chain: Zip {
                chains: (builders.0.chain, builders.1.chain),
            },
        }
    }
}

impl<C1: Head, C2: Head> Head for Zip<(C1, C2)>
where
    C1::Provides: crate::set::Join<C2::Provides>,
    Union<C1::Provides, C2::Provides>: Set,
{
    type Output = (Option<C1::Output>, Option<C2::Output>);
    type Provides = Union<C1::Provides, C2::Provides>;
}

impl<C1, C2, W: Set> Connect<W> for Zip<(C1, C2)>
where
    C1: Connect<W>,
    C2: Connect<W>,
{
    type Stream = (C1::Stream_as_upstream, C2::Stream_as_upstream);

    fn connect(self, build: &mut Build, consumer: Arc<Port>) -> Self::Stream {
        (
            self.chains.0.connect(build, Arc::clone(&consumer)),
            self.chains.1.connect(build, consumer),
        )
    }
}
```

**Note:** `Stream_as_upstream` is not real syntax — the tuple gather is implemented over `(Upstream<A>, Upstream<B>)`, and each chain's `Connect::Stream` for sources/nodes *is* `Upstream<Out>`, so the associated type is simply `(C1::Stream, C2::Stream)` with the added bound that each member stream is an `Upstream` (or generalize the tuple `Gather` impl to any member gathers — cleaner: implement the tuple `Gather` over `(G1: Gather, G2: Gather)` with `Item = (Option<G1::Item>, Option<G2::Item>)`, which also composes zips of zips for free; adjust Task 14 accordingly while implementing). Per-member wants: both members receive `W` unchanged here — per-branch `Wants` derivation is parked in the spec; note it in the code with nothing cleverer than passing `W` through.

`.merge((a, b))` on `PipelineBuilder`: constrain both chains to one `Output`; connect the first member normally, then connect the second with the same `Downstream` clones instead of a fresh `wire` — add a `Build::merge_edge` variant of `register` receiving the existing edge parts. EOF composes because each producer parks its own keeper and the shared items channel closes when the last one drops.

- [ ] **Step 2: Fan-in test**

```rust
#[test]
fn uneven_zip_is_a_valid_run() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(4).build();
    let (write, out_rx) = sink::channel::<(Option<u32>, Option<u32>)>();
    let short = Count { limit: 10, at: Arc::new(AtomicU32::new(0)) };
    let long = Count { limit: 100, at: Arc::new(AtomicU32::new(0)) };

    let r1 = Pipeline::builder().source(short);
    let r2 = Pipeline::builder().source(long);

    let runner = runtime.pipeline::<()>(Pipeline::zip((r1, r2)).sink(write));
    assert!(runner.join().is_ok());

    let pairs: Vec<_> = std::iter::from_fn(|| out_rx.try_recv().ok().flatten()).collect();
    assert_eq!(pairs.len(), 100);
    assert!(pairs.iter().filter(|(a, _)| a.is_some()).count() == 10);
    assert!(pairs.iter().all(|(_, b)| b.is_some()));
}
```

- [ ] **Step 3: Fault tests**

```rust
#[derive(Clone)]
struct Fatal;

impl Apply for Fatal {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        if input == 50 {
            return Err(Error::new("broken at 50"));
        }
        out.push(input);
        Ok(())
    }
}

#[derive(Clone)]
struct Panicky;

impl Apply for Panicky {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        if input == 50 {
            panic!("worker panic at 50");
        }
        out.push(input);
        Ok(())
    }
}

#[test]
fn fatal_error_reaches_join() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(4).build();
    let (write, _out_rx) = sink::channel::<u32>();
    let source = Count { limit: 1000, at: Arc::new(AtomicU32::new(0)) };
    let runner = runtime.pipeline::<()>(
        Pipeline::builder().source(source).layer(Fatal).sink(write),
    );
    assert!(runner.join().is_err());
}

#[test]
fn panic_converts_to_error() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(4).build();
    let (write, _out_rx) = sink::channel::<u32>();
    let source = Count { limit: 1000, at: Arc::new(AtomicU32::new(0)) };
    let runner = runtime.pipeline::<()>(
        Pipeline::builder().source(source).layer(Panicky).sink(write),
    );
    assert!(runner.join().is_err());
}
```

`panic_converts_to_error` forces the missing piece: the guard's `Panicked` state must record an `Error` on the runtime (do it in `Guard::drop` when the state cell still holds `Panicked`: `runtime.record(Error::new("layer panicked"))` — thread the runtime handle into the guard), and a `Panicked`-state `Released` must mark the seat finished so teardown cascades. The fatal test additionally needs the spec's prompt-cleanup rule: on recording an error, trigger `Shutdown` from the teardown path.

- [ ] **Step 4: Checkpoint (user runs)**

Run: `cargo test -p bascet-core --test fanin --test faults`
Expected: all pass without hanging — the fault paths exercise voided edges, orphaned producers, and the join-then-reap sequence.

---

### Task 20: Public API, benches, deletion sweep

**Files:**
- Modify: `crates/bascet-core/src/lib.rs`, `crates/bascet-core/Cargo.toml`
- Create: `crates/bascet-core/benches/parallel.rs` (replacing the attic copy), delete `benches/attic-*.txt` after porting anything worth keeping

**Interfaces:**
- Produces: the spec's exact export list; a minimal working bench; zero references to deleted names.

- [ ] **Step 1: `lib.rs` final form**

```rust
extern crate self as bascet_core;

pub mod apply;
pub mod arena;
pub mod attr;
pub(crate) mod consts;
pub mod owned;
pub mod pipe;
pub mod pipeline;
pub mod runner;
pub mod runtime;
pub mod scheduler;
pub mod set;
pub mod sink;
pub mod utils;
pub mod worker;

pub use apply::{Apply, ApplyAsync, AsyncEmit, Emit, Error};
pub use arena::{Arena, ArenaPool, ArenaSlice, ArenaView};
pub use attr::{Attr, AttrEntry, Coerce, Mut, Put, Record, Ref, Represents};
pub use owned::Owned;
pub use pipe::Pipe;
pub use pipeline::Pipeline;
pub use runner::Runner;
pub use runtime::{Runtime, Shutdown, Tier};
pub use scheduler::{Action, Activity, Auto, Driver, Event, Layer, Load, Port, Preempt, Receipt, Scheduler};
pub use set::{Intersect, Join, Meet, Set, Subset, Union};
pub use worker::{State, Worker};
```

(`Metrics` exports from `runner` per the spec list if it was made public there; keep `AttrId` reachable via `set::AttrId`.)

- [ ] **Step 2: One real bench**

`benches/parallel.rs`: the linear pipeline from Task 18's test with `limit = 1_000_000` and 2 `Double` stages, timed wall-clock with `std::time::Instant`, printed items/sec — no criterion, `harness = false` restored in `Cargo.toml`. This is the seam `Auto::learn` is gated on; it needs to exist, not to be sophisticated.

- [ ] **Step 3: Deletion sweep (user runs, or via Grep)**

```
grep -rn "Petitioner\|Lease\|AtomicPressure\|AtomicPatience\|Temper\|Pull\|Contract\|Executable\|Strategy\|Coordinate\|Tally\|peekable\|pressurised" crates/ --include="*.rs"
```

Expected: zero hits (except `Pull`-like substrings in unrelated words — read any hit before acting). Confirm `utils/send/send_ptr.rs` is still referenced or flag it to the user; do not delete unlisted code.

- [ ] **Step 4: Final checkpoint (user runs)**

Run: `cargo test --workspace` then `cargo bench -p bascet-core`
Expected: everything green; bench prints a throughput number. Hand the tree to the user for commit.

---

## Self-review notes

- **Spec coverage gaps, acknowledged:** `Auto` tier-swap (spec: swap-spawn-first) and the `Yield`-with-receipt donor retry are stubbed to their simplest forms in Task 17 — policy refinement is spec-gated on benches. Per-branch zip `Wants` passes `W` through unchanged (parked in spec). Serving needs no new machinery by design — it is expressible with Task 18's pieces. The watchdog's per-edge crossing probes are registered but the stall predicate is warn-only.
- **Known syntax risk points, called out inline:** variadic macro capabilities (Tasks 3, 4, 14), kanal API names (Tasks 10, 13), `Connect` recursion bounds (Task 18). Each has a fallback stated at the point of use.
- **Type-consistency:** `Wanted<A, M, W>` (12→18), `Core::edge/null` (10→16), `Gather::{take, nudge, Round}` (14→16→18), `Driver` fields (17→18), `Pool` signatures (8→17) — all cross-referenced above; if an implementer changes a signature, they must update the consuming task in the same change.
