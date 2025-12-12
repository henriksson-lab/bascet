impl<T, S, A> crate::composite::From<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: Clone,
    <T as crate::Get<A>>::Value: From<<S as crate::Get<A>>::Value>,
{
    #[inline(always)]
    fn from(&mut self, source: &S) {
        *<T as crate::Get<A>>::as_mut(self) =
            (*<S as crate::Get<A>>::as_ref(source)).clone().into();
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<T, S, @n[A~#](sep=",")> crate::composite::From<(@n[A~#](sep=",")), S> for T
    where
        T: @n[crate::Get<A~#> + crate::composite::From<A~#, S>](sep = "+"),
        S: @n[crate::Get<A~#>](sep = "+"),
        @n[A~#: crate::Attr](sep=","),
    {
        fn from(&mut self, source: &S) {
            @n[<T as crate::composite::From<A~#, S>>::from(self, source);]
        }
    }
}
