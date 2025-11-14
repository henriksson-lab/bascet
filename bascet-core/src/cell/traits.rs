pub trait Cell: Sized {
    type Builder: crate::Builder<Builds = Self>;
    fn builder() -> Self::Builder;

    fn get_ref<'a, G: crate::GetRef<'a, Self>>(&'a self) -> G::Output {
        G::get_ref(self)
    }

    fn get_mut<'a, G: crate::GetMut<'a, Self>>(&'a mut self) -> G::Output {
        G::get_mut(self)
    }
}
pub trait Provides<A> {
    type Type;
    fn as_ref(&self) -> &Self::Type;
    fn as_mut(&mut self) -> &mut Self::Type;
}

impl<'a, T, A> crate::GetRef<'a, T> for A
where
    A: crate::Attr,
    T: Provides<A>,
    <T as Provides<A>>::Type: 'a,
{
    type Output = &'a <T as Provides<A>>::Type;
    fn get_ref(cell: &'a T) -> Self::Output {
        cell.as_ref()
    }
}

impl<'a, T, A> crate::GetMut<'a, T> for A
where
    A: crate::Attr,
    T: Provides<A>,
    <T as Provides<A>>::Type: 'a,
{
    type Output = &'a mut <T as Provides<A>>::Type;
    fn get_mut(cell: &'a mut T) -> Self::Output {
        cell.as_mut()
    }
}
