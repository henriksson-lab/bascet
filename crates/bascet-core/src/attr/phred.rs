use bascet_derive::Attr;
use derive_more::Display;

#[derive(Attr, Display)]
#[variadic(N = 1..=16)]
#[display("Phred[{N}]")]
pub struct Phred<const N: usize = 1>;

#[derive(Attr, Display)]
#[variadic(N = 1..=16)]
#[display("Phred33[{N}]")]
pub struct Phred33<const N: usize = 1>;

#[derive(Attr, Display)]
#[variadic(N = 1..=16)]
#[display("Phred64[{N}]")]
pub struct Phred64<const N: usize = 1>;

bascet_variadic::variadic!(N = 1..=16, M = 1..=15, for (N, M) in N.product(M).filter(N > M) => {
    impl<@N[V~#](sep=",")> crate::Coerce<Phred33s<N>, Phreds<M>> for (@N[V~#](sep=","),) {
        type Output = (@M[V~#](sep=","),);
        fn coerce(self) -> Self::Output {
            let (@N[_v~#](sep=","),) = self;
            (@M[_v~#](sep=","),)
        }
    }
    impl<@N[V~#](sep=",")> crate::Coerce<Phred64s<N>, Phreds<M>> for (@N[V~#](sep=","),) {
        type Output = (@M[V~#](sep=","),);
        fn coerce(self) -> Self::Output {
            let (@N[_v~#](sep=","),) = self;
            (@M[_v~#](sep=","),)
        }
    }
});
