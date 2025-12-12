pub trait Collection {
    type Item;
    fn push(&mut self, item: Self::Item);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Collection for Vec<T> {
    type Item = T;

    #[inline]
    fn push(&mut self, item: T) {
        Vec::push(self, item)
    }

    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
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
    <T as crate::Get<A>>::Value: Collection<Item = <S as crate::Get<A>>::Value>,
{
    #[inline(always)]
    fn push(&mut self, source: &S) {
        Collection::push(
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

// pub trait Empty<A> {
//     fn is_empty(&self) -> bool;
// }

// impl<T, A> Empty<A> for T
// where
//     T: crate::Get<A>,
//     A: crate::Attr,
//     <T as crate::Get<A>>::Value: Collection,
// {
//     #[inline(always)]
//     fn is_empty(&self) -> bool {
//         Collection::is_empty(<T as crate::Get<A>>::as_ref(self))
//     }
// }

// bascet_variadic::variadic! {
//     #[expand(n = 2..=16)]
//     impl<T, @n[A~#](sep=",")> Empty<(@n[A~#](sep=","))> for T
//     where
//         T: @n[Empty<A~#>](sep = "+"),
//         @n[A~#: crate::Attr](sep=","),
//     {
//         fn is_empty(&self) -> bool {
//             @n[<T as Empty<A~#>>::is_empty(self)](sep = " && ")
//         }
//     }
// }

// pub trait Len<A> {
//     fn len(&self) -> usize;
// }

// impl<T, A> Len<A> for T
// where
//     T: crate::Get<A>,
//     A: crate::Attr,
//     <T as crate::Get<A>>::Value: Collection<Item = <T as crate::Get<A>>::Value>,
// {
//     #[inline(always)]
//     fn len(&self) -> usize {
//         Collection::len(<T as crate::Get<A>>::as_ref(self))
//     }
// }
