pub enum DecodeStatus<E> {
    Decoded(usize),
    Eof,
    Error(E),
}

pub trait Decode {
    fn sizeof_target_alloc(&self) -> usize;
    fn decode_into<B: AsMut<[u8]>>(&mut self, buf: B) -> DecodeStatus<()>;
}

pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
    Finished,
}

pub trait Parse<T> {
    type Output;

    fn parse_aligned<C, A>(
        &mut self,   //
        decoded: &T, //
    ) -> ParseStatus<C, ()>
    where
        C: Default + crate::Composite + FromParsed<A, Self::Output> + crate::FromBacking<Self::Output, <C as crate::Composite>::Backing>;

    fn parse_spanning<C, A>(
        &mut self,                 //
        decoded_spanning_tail: &T, //
        decoded_spanning_head: &T, //
        alloc: impl FnMut(usize) -> T,
    ) -> ParseStatus<C, ()>
    where
        C: Default + crate::Composite + FromParsed<A, Self::Output> + crate::FromBacking<Self::Output, <C as crate::Composite>::Backing>;

    fn parse_finish<C, A>(
        &mut self,
        // decoded: &T
    ) -> ParseStatus<C, ()>
    where
        C: Default + crate::Composite + FromParsed<A, Self::Output> + crate::FromBacking<Self::Output, <C as crate::Composite>::Backing>;
}

pub trait FromParsed<AttrTuple, Source> {
    fn from(&mut self, source: &Source);
}

impl<T, S, A> FromParsed<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A>,
    A: crate::Attr,
    <S as crate::Get<A>>::Value: Copy,
    <T as crate::Get<A>>::Value: From<<S as crate::Get<A>>::Value>,
{
    fn from(&mut self, source: &S) {
        *<T as crate::Get<A>>::as_mut(self) = (*<S as crate::Get<A>>::as_ref(source)).into();
    }
}

impl_variadic_from_parsed!();
