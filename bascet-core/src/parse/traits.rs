pub enum ParseResult<T, E> {
    Full(T),
    Partial,
    Error(E),
    Finished,
}

pub trait Parse<T> {
    type Item;

    fn parse_aligned(&mut self, decoded: &T) -> ParseResult<Self::Item, ()>;

    fn parse_spanning<FA>(
        &mut self,
        decoded_spanning_tail: &T,
        decoded_spanning_head: &T,
        alloc: FA,
    ) -> ParseResult<Self::Item, ()>
    where
        FA: FnMut(usize) -> T;

    fn parse_finish(&mut self) -> ParseResult<Self::Item, ()>;
}
