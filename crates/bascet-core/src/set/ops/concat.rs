use crate::attr::Attr;

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
