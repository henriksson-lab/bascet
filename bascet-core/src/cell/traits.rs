pub trait Cell: Sized {
    type Builder: super::Builder<Builds = Self>;
    fn builder() -> Self::Builder;

    fn get_ref<'a, G: GetRef<'a, Self>>(&'a self) -> G::Output {
        G::get_ref(self)
    }

    fn get_mut<'a, G: GetMut<'a, Self>>(&'a mut self) -> G::Output {
        G::get_mut(self)
    }
}

pub trait GetRef<'a, T> {
    type Output;
    fn get_ref(cell: &'a T) -> Self::Output;
}

pub trait GetMut<'a, T> {
    type Output;
    fn get_mut(cell: &'a mut T) -> Self::Output;
}

pub trait Provides<A> {
    type Type;
    fn as_ref(&self) -> &Self::Type;
    fn as_mut(&mut self) -> &mut Self::Type;
}

pub trait Attr {}

impl<'a, T, A> GetRef<'a, T> for A
where
    A: Attr,
    T: Provides<A>,
    <T as Provides<A>>::Type: 'a,
{
    type Output = &'a <T as Provides<A>>::Type;
    fn get_ref(cell: &'a T) -> Self::Output {
        cell.as_ref()
    }
}

impl<'a, T, A> GetMut<'a, T> for A
where
    A: Attr,
    T: Provides<A>,
    <T as Provides<A>>::Type: 'a,
{
    type Output = &'a mut <T as Provides<A>>::Type;
    fn get_mut(cell: &'a mut T) -> Self::Output {
        cell.as_mut()
    }
}

pub trait Builder: Sized {
    type Builds: super::Cell;
    fn build(self) -> Self::Builds;

    fn with<A: Attr>(self, value: <Self as Build<A>>::Type) -> Self
    where
        Self: Build<A>,
    {
        <Self as Build<A>>::set(self, value)
    }
}

pub trait Build<Attr> {
    type Type;
    fn set(self, value: Self::Type) -> Self;
}
