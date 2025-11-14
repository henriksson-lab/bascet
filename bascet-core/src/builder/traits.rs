pub trait Builder: Sized {
    type Builds;
    fn build(self) -> Self::Builds;

    fn with<A: crate::Attr>(self, value: <Self as Build<A>>::Type) -> Self
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
