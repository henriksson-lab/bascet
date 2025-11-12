pub trait Cell: Sized {
    type Builder: super::Builder<Builds = Self>;
    fn builder() -> Self::Builder;

    fn get<'a, G: super::Get<'a, Self>>(&'a self) -> G {
        G::get(self)
    }

    fn get_mut<'a, G: super::GetMut<'a, Self>>(&'a mut self) -> G {
        G::get_mut(self)
    }
}

pub struct ManagedRef<T>(pub T);

impl<'managed_ref, T> super::Get<'managed_ref, T> for ManagedRef<T::Ref>
where
    T: super::marker::UseManagedRef,
{
    fn get(cell: &'managed_ref T) -> Self {
        ManagedRef(<T as super::marker::UseManagedRef>::value(cell))
    }
}

impl<'managed_ref, T> super::GetMut<'managed_ref, T> for ManagedRef<T::Ref>
where
    T: super::marker::UseManagedRef,
{
    fn get_mut(cell: &'managed_ref mut T) -> Self {
        ManagedRef(<T as super::marker::UseManagedRef>::value(cell))
    }
}
