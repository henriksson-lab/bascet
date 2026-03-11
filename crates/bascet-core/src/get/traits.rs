pub trait Get<T> {
    type Value;
    fn as_ref(&self) -> &Self::Value;
    fn as_mut(&mut self) -> &mut Self::Value;
}

impl<T> Get<T> for () {
    type Value = ();

    #[inline(always)]
    fn as_ref(&self) -> &Self::Value {
        self
    }

    #[inline(always)]
    fn as_mut(&mut self) -> &mut Self::Value {
        self
    }
}
