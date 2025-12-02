impl<T, S, A> crate::FromParsed<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: Copy,
    <T as crate::Get<A>>::Value: From<<S as crate::Get<A>>::Value>,
{
    fn from_parsed(&mut self, source: &S) {
        *<T as crate::Get<A>>::as_mut(self) = (*<S as crate::Get<A>>::as_ref(source)).into();
    }
}

impl_variadic_from_parsed!();
