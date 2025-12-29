pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
    Finished,
}

pub trait Parse<T> {
    type Item;

    fn parse_aligned(&mut self, decoded: &T) -> ParseStatus<Self::Item, ()>;

    fn parse_spanning(
        &mut self,
        decoded_spanning_tail: &T,
        decoded_spanning_head: &T,
        alloc: impl FnMut(usize) -> T,
    ) -> ParseStatus<Self::Item, ()>;

    fn parse_finish(&mut self) -> ParseStatus<Self::Item, ()>;
}
