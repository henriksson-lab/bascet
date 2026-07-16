use crate::attr::Attr;
use crate::set::attr_id::{Digit, DigitEq};
use crate::set::{Bool, Hit, Miss};

pub trait Same<B> {
    type Verdict: Bool;
}

pub trait IdEq<B> {
    type Verdict: Bool;
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[XD~#: Digit](sep=","), @N[BD~#: Digit](sep=",")> IdEq<(@N[BD~#](sep=","),)> for (@N[XD~#](sep=","),)
    where
        @N[XD~#: DigitEq<BD~#>](sep=","),
    {
        type Verdict = @N[<] Hit @N[ as Bool>::And<<XD~# as DigitEq<BD~#>>::Verdict>];
    }
});

impl<X, B> Same<B> for X
where
    X: Attr,
    B: Attr,
    X::Id: IdEq<B::Id>,
{
    type Verdict = <X::Id as IdEq<B::Id>>::Verdict;
}

pub trait In<S> {
    type Verdict: Bool;
}

impl<X: Attr> In<()> for X {
    type Verdict = Miss;
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<X: Attr, @N[B~#: Attr](sep=",")> In<(@N[B~#](sep=","),)> for X
    where
        @N[X: Same<B~#>](sep=","),
    {
        type Verdict = @N[<] Miss @N[ as Bool>::Or<<X as Same<B~#>>::Verdict>];
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

impl Concat<()> for () {
    type Output = ();
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[B~#: Attr](sep=",")> Concat<(@N[B~#](sep=","),)> for () {
        type Output = (@N[B~#](sep=","),);
    }
    impl<@N[A~#: Attr](sep=",")> Concat<()> for (@N[A~#](sep=","),) {
        type Output = (@N[A~#](sep=","),);
    }
});

bascet_variadic::variadic!(N = 1..=16, M = 1..=16, for (N, M) in N.product(M) => {
    impl<@N[A~#: Attr](sep=","), @M[B~#: Attr](sep=",")> Concat<(@M[B~#](sep=","),)> for (@N[A~#](sep=","),) {
        type Output = (@N[A~#](sep=","), @M[B~#](sep=","),);
    }
});

pub trait Absorb<L> {
    type Output;
}

impl<A: Attr, L> Absorb<L> for A
where
    A: In<L>,
    A: Select<<<A as In<L>>::Verdict as Bool>::Not>,
    L: Concat<<A as Select<<<A as In<L>>::Verdict as Bool>::Not>>::Output>,
{
    type Output =
        <L as Concat<<A as Select<<<A as In<L>>::Verdict as Bool>::Not>>::Output>>::Output;
}

#[doc(hidden)]
pub trait Keep<R> {
    type Output;
}

impl<A: Attr, R> Keep<R> for A
where
    A: In<R>,
    A: Select<<A as In<R>>::Verdict>,
{
    type Output = <A as Select<<A as In<R>>::Verdict>>::Output;
}

pub trait Join<R> {
    type Output;
}

impl<L> Join<()> for L {
    type Output = L;
}

impl<L, R1: Attr> Join<(R1,)> for L
where
    R1: Absorb<L>,
{
    type Output = <R1 as Absorb<L>>::Output;
}

bascet_variadic::variadic!(N = 2..=16, M = 1..=15, for (N, M) in N.zip(M) => {
    impl<L, @N[R~#: Attr](sep=",")> Join<(@N[R~#](sep=","),)> for L
    where
        L: Join<(@M[R~#](sep=","),)>,
        R~M: Absorb<<L as Join<(@M[R~#](sep=","),)>>::Output>,
    {
        type Output = <R~M as Absorb<<L as Join<(@M[R~#](sep=","),)>>::Output>>::Output;
    }
});

pub trait Meet<R> {
    type Output;
}

impl<R> Meet<R> for () {
    type Output = ();
}

impl<R, L1: Attr> Meet<R> for (L1,)
where
    L1: Keep<R>,
{
    type Output = <L1 as Keep<R>>::Output;
}

bascet_variadic::variadic!(N = 2..=16, M = 1..=15, for (N, M) in N.zip(M) => {
    impl<R, @N[L~#: Attr](sep=",")> Meet<R> for (@N[L~#](sep=","),)
    where
        L~M: Keep<R>,
        (@M[L~#](sep=","),): Meet<R>,
        <(@M[L~#](sep=","),) as Meet<R>>::Output: Concat<<L~M as Keep<R>>::Output>,
    {
        type Output = <<(@M[L~#](sep=","),) as Meet<R>>::Output as Concat<<L~M as Keep<R>>::Output>>::Output;
    }
});
