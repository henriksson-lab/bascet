use crate::set::{Bool, Hit, Miss};

pub trait Digit: 'static {
    const VALUE: u64;
}

pub trait DigitEq<D> {
    type Verdict: Bool;
}

pub trait AttrId: 'static {
    const ID: u64;
}

bascet_variadic::variadic!(N = 0..=15, for N in N => {
    pub struct D~N;
    impl Digit for D~N {
        const VALUE: u64 = N;
    }
    impl DigitEq<D~N> for D~N {
        type Verdict = Hit;
    }
});

bascet_variadic::variadic!(N = 0..=15, M = 0..=15, for (N, M) in N.product(M).filter(N != M) => {
    impl DigitEq<D~M> for D~N {
        type Verdict = Miss;
    }
});

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[T~#: Digit](sep=", ")> AttrId for (@N[T~#](sep=", "),) {
        const ID: u64 = 0 @N[| (T~#::VALUE << (4 * (N - 1 - #)))];
    }
});
