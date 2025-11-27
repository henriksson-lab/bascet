pub trait Get<T> {
    type Value;
    fn as_ref(&self) -> &Self::Value;
    fn as_mut(&mut self) -> &mut Self::Value;
}
