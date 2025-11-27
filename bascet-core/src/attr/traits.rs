pub trait Attr {}

pub struct Tagged<A, T> {
    pub value: T,
    _marker: std::marker::PhantomData<A>,
}

impl<A, T> Tagged<A, T> {
    #[inline(always)]
    pub fn new(value: T) -> Self {
        Self {
            value,
            _marker: std::marker::PhantomData,
        }
    }
}

pub trait Put<Target> {
    fn put(self, target: &mut Target);
}

impl<A, T, Target> Put<Target> for Tagged<A, T>
where
    Target: crate::Get<A, Value = T>,
    A: Attr,
{
    #[inline(always)]
    fn put(self, target: &mut Target) {
        *target.as_mut() = self.value;
    }
}

pub trait Ref<'a, T> {
    type Output;
    fn get_ref(_: &'a T) -> Self::Output;
}

pub trait Mut<'a, T> {
    type Output;
    fn get_mut(_: &'a mut T) -> Self::Output;
}

impl<'a, T, A> Ref<'a, T> for A
where
    A: Attr,
    T: crate::Get<A>,
    <T as crate::Get<A>>::Value: 'a,
{
    type Output = &'a <T as crate::Get<A>>::Value;

    #[inline(always)]
    fn get_ref(cell: &'a T) -> Self::Output {
        cell.as_ref()
    }
}

impl<'a, T, A> Mut<'a, T> for A
where
    A: Attr,
    T: crate::Get<A>,
    <T as crate::Get<A>>::Value: 'a,
{
    type Output = &'a mut <T as crate::Get<A>>::Value;

    #[inline(always)]
    fn get_mut(cell: &'a mut T) -> Self::Output {
        cell.as_mut()
    }
}

impl_variadic_get!();
