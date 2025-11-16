pub enum DecodeStatus<T, E> {
    Block(T),
    Eof(T),
    Error(E),
}

pub trait Decode {
    type Block;
    fn decode(&mut self) -> DecodeStatus<Self::Block, ()>;
}

pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
}

pub trait Parse<T> {
    fn parse<C, A>(&mut self, block: T) -> ParseStatus<C, ()>
    where
        C: crate::Composite;

    fn parse_finish<C, A>(&mut self, block: T) -> ParseStatus<C, ()>
    where
        C: crate::Composite;
}
