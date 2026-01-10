pub trait AsCollection {
    type Item;
    type Iter<'a>: Iterator<Item = &'a Self::Item>
    where
        Self: 'a,
        Self::Item: 'a;

    fn push(&mut self, item: Self::Item);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter(&self) -> Self::Iter<'_>;
}

impl<T> AsCollection for Vec<T> {
    type Item = T;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    #[inline]
    fn push(&mut self, item: T) {
        Vec::push(self, item)
    }

    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }
}

pub trait Push<A, S> {
    fn push(&mut self, source: &S);
}

impl<T, S, A> Push<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: Clone,
    <T as crate::Get<A>>::Value: AsCollection<Item = <S as crate::Get<A>>::Value>,
{
    #[inline(always)]
    fn push(&mut self, source: &S) {
        AsCollection::push(
            <T as crate::Get<A>>::as_mut(self),
            Clone::clone(<S as crate::Get<A>>::as_ref(source)),
        );
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<T, S, @n[A~#](sep=",")> Push<(@n[A~#](sep=",")), S> for T
    where
        T: @n[Push<A~#, S>](sep = "+"),
        @n[A~#: crate::Attr](sep=","),
    {
        fn push(&mut self, source: &S) {
            @n[<T as Push<A~#, S>>::push(self, source);]
        }
    }
}
