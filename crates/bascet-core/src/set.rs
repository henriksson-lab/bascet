pub mod attr_id;
pub mod ops;

pub use attr_id::AttrId;
pub use ops::{In, Join, Meet};

use crate::attr::Attr;

pub struct Hit;
pub struct Miss;

pub trait Bool {
    type And<B: Bool>: Bool;
    type Or<B: Bool>: Bool;
    type Not: Bool;
    const HIT: bool;
}

impl Bool for Hit {
    type And<B: Bool> = B;
    type Or<B: Bool> = Hit;
    type Not = Miss;
    const HIT: bool = true;
}

impl Bool for Miss {
    type And<B: Bool> = Miss;
    type Or<B: Bool> = B;
    type Not = Hit;
    const HIT: bool = false;
}

pub trait Set: 'static {
    fn contains<A: Attr>() -> bool;
}

impl Set for () {
    fn contains<A: Attr>() -> bool {
        false
    }
}

impl<B1: Attr> Set for (B1,) {
    fn contains<A: Attr>() -> bool {
        <A::Id as AttrId>::ID == <B1::Id as AttrId>::ID
    }
}

bascet_variadic::variadic!(N = 2..=16, M = 1..=15, for (N, M) in N.zip(M) => {
    impl<@N[B~#: Attr](sep=",")> Set for (@N[B~#](sep=","),)
    where
        B~M: In<(@M[B~#](sep=","),), Verdict = Miss>,
        (@M[B~#](sep=","),): Set,
    {
        fn contains<A: Attr>() -> bool {
            @N[(<A::Id as AttrId>::ID == <B~#::Id as AttrId>::ID)](sep=" || ")
        }
    }
});

#[diagnostic::on_unimplemented(
    message = "`{Self}` requires attributes not provided upstream",
    label = "the producer's `Provides` must cover this layer's `Requires`"
)]
pub trait Subset<Sup: Set> {}

impl<Sup: Set> Subset<Sup> for () {}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<Sup: Set, @N[A~#: Attr](sep=",")> Subset<Sup> for (@N[A~#](sep=","),)
    where
        @N[A~#: In<Sup, Verdict = Hit>](sep=","),
    {}
});

pub type Union<L, R> = <L as Join<R>>::Output;
pub type Intersect<L, R> = <L as Meet<R>>::Output;
