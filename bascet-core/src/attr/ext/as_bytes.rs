pub trait Bytes<'a, T> {
    type Output;
    fn get_bytes(_: &'a T) -> Self::Output;
}

impl<'a, T, A> Bytes<'a, T> for A
where
    A: crate::Attr,
    T: crate::Get<A>,
    <T as crate::Get<A>>::Value: AsRef<[u8]> + 'a,
{
    type Output = &'a [u8];

    #[inline(always)]
    fn get_bytes(composite: &'a T) -> Self::Output {
        composite.as_ref().as_ref()
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<'a, T, @n[A~#: Bytes<'a, T>](sep=",")> Bytes<'a, T> for (@n[A~#](sep=",")) {
        type Output = (@n[<A~# as Bytes<'a, T>>::Output](sep=","));
        fn get_bytes(composite: &'a T) -> Self::Output {
            (@n[A~#::get_bytes(composite)](sep=","))
        }
    }
}

pub trait GetBytes {
    #[inline(always)]
    fn get_bytes<'a, G: Bytes<'a, Self>>(&'a self) -> G::Output
    where
        Self: Sized,
    {
        G::get_bytes(self)
    }
}

impl<T: crate::Composite> GetBytes for T {}