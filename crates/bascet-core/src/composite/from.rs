impl<T, S, A> crate::composite::FromDirect<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: Clone,
    <T as crate::Get<A>>::Value: From<<S as crate::Get<A>>::Value>,
{
    #[inline(always)]
    fn from_direct(&mut self, source: &S) {
        *<T as crate::Get<A>>::as_mut(self) =
            (*<S as crate::Get<A>>::as_ref(source)).clone().into();
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<T, S, @n[A~#](sep=",")> crate::composite::FromDirect<(@n[A~#](sep=",")), S> for T
    where
        T: @n[crate::Get<A~#> + crate::composite::FromDirect<A~#, S>](sep = "+"),
        S: @n[crate::Get<A~#>](sep = "+"),
        @n[A~#: crate::Attr](sep=","),
    {
        fn from_direct(&mut self, source: &S) {
            @n[<T as crate::composite::FromDirect<A~#, S>>::from_direct(self, source);]
        }
    }
}

impl<T, S, A> crate::composite::FromCollectionIndexed<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: AsRef<[<T as crate::Get<A>>::Value]>,
    <T as crate::Get<A>>::Value: Clone,
{
    fn from_collection_indexed(&mut self, source: &S, index: usize) {
        let collection = source.as_ref().as_ref();
        *self.as_mut() = collection[index].clone();
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<T, S, @n[A~#](sep=",")> crate::composite::FromCollectionIndexed<(@n[A~#](sep=",")), S> for T
    where
        T: @n[crate::composite::FromCollectionIndexed<A~#, S>](sep = "+"),
        @n[A~#: crate::Attr](sep=","),
    {
        fn from_collection_indexed(&mut self, source: &S, index: usize) {
            @n[<T as crate::composite::FromCollectionIndexed<A~#, S>>::from_collection_indexed(self, source, index);]
        }
    }
}
