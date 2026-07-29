//! Closure-combinator spike (throwaway) — separate bin, leaves main.rs untouched.
//!
//! Question: can `.apply::<Reads, Writes>(closure)` carry a *produced* output attr
//! without E0207? The value-plane spec says `Map<In, F>` fails because `Out` (in the
//! `&mut Emit<Out>` argument) is an unconstrained impl parameter. Fix under test: put
//! the write attr `W` in the struct, so `Out = W::Value` is pinned by the Self type
//! and both the return form and the out-param form become constrained.
//!
//! Run: `cargo run -p bascet-sandbox --bin closure_spike`
//! Expected: `A: 4` then `B: [2, 2]`.
//!
//! `F` is generic because a closure has an unnameable type — it must be a monomorphized
//! type parameter (a `dyn Fn` would box + vtable-call per item). `F` is NOT the E0207
//! risk; `W` was. `F` is constrained by living in `Map<R, W, F>` plus its `Fn` bound.

use std::marker::PhantomData;

pub trait Attr {
    type Value;
}
pub struct Id;
impl Attr for Id {
    type Value = Vec<u8>;
}
pub struct Gc;
impl Attr for Gc {
    type Value = u32;
}

// R = read attr, W = write attr, F = the closure. All three live in the Self type.
pub struct Map<R, W, F> {
    f: F,
    _p: PhantomData<(fn() -> R, fn() -> W)>,
}

pub fn map<R, W, F>(f: F) -> Map<R, W, F> {
    Map { f, _p: PhantomData }
}

// ---- form A: return-shaped producer (1:1 map) ----
pub trait Apply {
    type Requires: Attr;
    type Produces: Attr;
    fn run(&self, input: &<Self::Requires as Attr>::Value) -> <Self::Produces as Attr>::Value;
}

impl<R, W, F> Apply for Map<R, W, F>
where
    R: Attr,
    W: Attr,
    F: Fn(&R::Value) -> W::Value,
{
    type Requires = R;
    type Produces = W;
    fn run(&self, input: &R::Value) -> W::Value {
        (self.f)(input)
    }
}

// ---- form B: out-param producer — the exact `&mut Emit<Out>` shape the spec feared ----
pub trait Emit {
    type Requires: Attr;
    type Produces: Attr;
    fn run(
        &self,
        input: &<Self::Requires as Attr>::Value,
        out: &mut Vec<<Self::Produces as Attr>::Value>,
    );
}

impl<R, W, F> Emit for Map<R, W, F>
where
    R: Attr,
    W: Attr,
    F: Fn(&R::Value, &mut Vec<W::Value>),
{
    type Requires = R;
    type Produces = W;
    fn run(&self, input: &R::Value, out: &mut Vec<W::Value>) {
        (self.f)(input, out)
    }
}

fn main() {
    // return form: `.apply::<Id, Gc>(|id| ...)` — closure sees the raw value, not a Record
    let gc = map::<Id, Gc, _>(|id: &Vec<u8>| id.len() as u32);
    println!("A: {}", Apply::run(&gc, &b"ACGT".to_vec()));

    // out-param form (Emit<Out>): same struct, closure writes 0..n outputs into `out`
    let dup = map::<Id, Gc, _>(|id: &Vec<u8>, out: &mut Vec<u32>| {
        out.push(id.len() as u32);
        out.push(id.len() as u32);
    });
    let mut out = Vec::new();
    Emit::run(&dup, &b"AC".to_vec(), &mut out);
    println!("B: {:?}", out);
}
