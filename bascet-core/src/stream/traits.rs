pub trait Decode {
    type Block;
    fn decode(&mut self) -> Result<Option<Self::Block>, ()>;
}
pub trait Parse<T> {
    fn parse<C, A>(&mut self, block: T) -> Result<Option<C>, ()>
    where
        C: crate::Composite;
}
