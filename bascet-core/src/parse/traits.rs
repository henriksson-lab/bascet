pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
    Finished,
}

pub trait FromParsed<AttrTuple, Source> {
    fn from_parsed(&mut self, source: &Source);
}

pub trait Parse<T, K> {
    type Item;

    fn parse_aligned<C, A>(
        &mut self,   //
        decoded: &T, //
    ) -> ParseStatus<C, ()>
    where
        C: std::default::Default
            + crate::Composite<Kind = K>
            + crate::FromParsed<A, K::Item>
            + crate::FromBacking<Self::Item, <C as crate::Composite>::Backing>;

    fn parse_spanning<C, A>(
        &mut self,                 //
        decoded_spanning_tail: &T, //
        decoded_spanning_head: &T, //
        alloc: impl FnMut(usize) -> T,
    ) -> ParseStatus<C, ()>
    where
        C: Default
            + crate::Composite
            + crate::FromParsed<A, Self::Item>
            + crate::FromBacking<Self::Item, <C as crate::Composite>::Backing>;

    fn parse_finish<C, A>(
        &mut self,
        // decoded: &T
    ) -> ParseStatus<C, ()>
    where
        C: Default
            + crate::Composite
            + crate::FromParsed<A, Self::Item>
            + crate::FromBacking<Self::Item, <C as crate::Composite>::Backing>;
}
