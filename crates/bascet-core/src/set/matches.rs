use crate::attr::attr_id::*;
use crate::set::{Hit, Membership, Miss};

pub trait Matches<Other> {
    type Result: Membership;
}

bascet_variadic::variadic!(N = 0..=15, for N in N => {
    impl Matches<D~N> for D~N {
        type Result = Hit;
    }
});

bascet_variadic::variadic!(N = 0..=15, M = 0..=15, for (N, M) in N.product(M).filter(N != M) => {
    impl Matches<D~M> for D~N {
        type Result = Miss;
    }
});

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[XD~#: Nibble](sep=","), @N[BD~#: Nibble](sep=",")> Matches<(@N[BD~#](sep=","),)> for (@N[XD~#](sep=","),)
    where
        @N[XD~#: Matches<BD~#>](sep=","),
    {
        type Result = @N[<] Hit @N[ as Membership>::And<<XD~# as Matches<BD~#>>::Result>];
    }
});
