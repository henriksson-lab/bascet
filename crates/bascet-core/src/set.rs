use crate::Attr;

pub trait Set {}

#[diagnostic::on_unimplemented(
    message = "`{Self}` requires attributes not provided by any upstream layer",
    label = "add a layer whose `Provides` covers the missing attributes"
)]
pub trait Subset<Sup: Set> {
    const OK: bool;
}

pub trait Union<B: Set>: Set {
    type Output;
}

impl<S: Set> Union<()> for S {
    type Output = S;
}

impl<A: Attr, B: Attr> Union<B> for A {
    type Output = (A, B);
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<A: Attr, @N[B~#: Attr](sep=",")> Union<(@N[B~#](sep=","),)> for A {
        type Output = (A, @N[B~#](sep=","),);
    }

    impl<@N[A~#: Attr](sep=","), B: Attr> Union<B> for (@N[A~#](sep=","),) {
        type Output = (@N[A~#](sep=","), B);
    }
});

bascet_variadic::variadic!(N = 1..=16, M = 1..=16, for (N, M) in N.product(M) => {
    impl<@N[A~#: Attr](sep=","), @M[B~#: Attr](sep=",")> Union<(@M[B~#](sep=","),)> for (@N[A~#](sep=","),) {
        type Output = (@N[A~#](sep=","), @M[B~#](sep=","),);
    }
});

impl Set for () {}

impl<A: Attr> Set for A {}

impl<Superset: Set> Subset<Superset> for () {
    const OK: bool = true;
}

impl<B: Attr> Subset<()> for B {
    const OK: bool = false;
}

impl<A: Attr, B: Attr> Subset<A> for B {
    const OK: bool = A::ID == B::ID;
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[A~#: Attr](sep=",")> Set for (@N[A~#](sep=","),) { }

    impl<B: Attr, @N[A~#: Attr](sep=",")> Subset<(@N[A~#](sep=","),)> for B {
        const OK: bool = @N[(B::ID == A~#::ID)](sep=" || ");
    }

    impl<@N[A~#: Attr](sep=","), Sup: Set> Subset<Sup> for (@N[A~#](sep=","),)
    where
        @N[A~#: Subset<Sup>](sep=","),
    {
        const OK: bool = @N[<A~# as Subset<Sup>>::OK](sep=" && ");
    }
});
