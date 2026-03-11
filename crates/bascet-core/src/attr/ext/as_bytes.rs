pub trait AsBytes<'a, T> {
    type Output;
    fn as_bytes(_: &'a T) -> Self::Output;
}

impl<'a, T, A> AsBytes<'a, T> for A
where
    A: crate::Attr,
    T: crate::Get<A>,
    <T as crate::Get<A>>::Value: AsRef<[u8]> + 'a,
{
    type Output = &'a [u8];

    #[inline(always)]
    fn as_bytes(composite: &'a T) -> Self::Output {
        composite.as_ref().as_ref()
    }
}
