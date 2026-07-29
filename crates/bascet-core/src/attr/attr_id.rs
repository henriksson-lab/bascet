pub trait Nibble: 'static {
    const VALUE: u64;
}

pub trait AttrId: 'static {
    const ID: u64;
}

bascet_variadic::variadic!(N = 0..=15, for N in N => {
    pub struct D~N;
    impl Nibble for D~N {
        const VALUE: u64 = N;
    }
});

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[T~#: Nibble](sep=", ")> AttrId for (@N[T~#](sep=", "),) {
        const ID: u64 = 0 @N[| (T~#::VALUE << (4 * (N - 1 - #)))];
    }
});
