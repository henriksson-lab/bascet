pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
    Finished,
}

pub trait FromParsed<AttrTuple, Source> {
    fn from_parsed(&mut self, source: &Source);
}

pub trait Context<M> {
    type Context: Default;
    type Marker;
}

pub trait Parse<T, M>
where
    Self: Context<M>,
{
    type Item;

    fn parse_aligned<C, A>(
        &mut self,
        decoded: &T,
        context: &mut <Self as Context<M>>::Context,
    ) -> ParseStatus<C, ()>
    where
        C: crate::Composite<Marker = M>
            + Default
            + crate::FromParsed<A, Self::Item>
            + crate::FromBacking<Self::Item, C::Backing>;

    fn parse_spanning<C, A>(
        &mut self,
        decoded_spanning_tail: &T,
        decoded_spanning_head: &T,
        context: &mut <Self as Context<M>>::Context,
        alloc: impl FnMut(usize) -> T,
    ) -> ParseStatus<C, ()>
    where
        C: crate::Composite<Marker = M>
            + Default
            + crate::FromParsed<A, Self::Item>
            + crate::FromBacking<Self::Item, C::Backing>;

    fn parse_finish<C, A>(
        &mut self,
        context: &mut <Self as Context<M>>::Context,
    ) -> ParseStatus<C, ()>
    where
        C: crate::Composite<Marker = M>
            + Default
            + crate::FromParsed<A, Self::Item>
            + crate::FromBacking<Self::Item, C::Backing>;
}
