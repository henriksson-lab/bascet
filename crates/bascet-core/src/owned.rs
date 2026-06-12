pub trait Owned<T> {
    type Value;
    fn owned(&self) -> Self::Value;
}

impl<S> Owned<()> for S {
    type Value = ();
    fn owned(&self) -> () {}
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<S, @N[A~#](sep=",")> Owned<(@N[A~#](sep=","),)> for S
    where
        @N[S: Owned<A~#>](sep=","),
    {
        type Value = (@N[<S as Owned<A~#>>::Value](sep=","),);
        fn owned(&self) -> Self::Value {
            (@N[<S as Owned<A~#>>::owned(self)](sep=","),)
        }
    }
});
