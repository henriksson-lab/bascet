pub enum DecodeStatus<T, E> {
    Decoded(T),
    Eof,
    Error(E),
}

pub trait Decode {
    type Output: Copy;
    fn decode(&mut self) -> DecodeStatus<Self::Output, ()>;
}

pub enum ParseStatus<T, E> {
    Full(T),
    Partial,
    Error(E),
}

pub trait Parse<T> {
    type Output;

    fn parse<C, A>(&mut self, decoded: T) -> ParseStatus<C, ()>
    where
        C: crate::Composite + Default + ParseFrom<A, Self::Output>;

    fn parse_finish<C, A>(&mut self) -> ParseStatus<C, ()>
    where
         C: crate::Composite + Default + ParseFrom<A, Self::Output>;

    fn parse_reset(&mut self) -> Result<(), ()>;
}

pub trait ParseFrom<AttrTuple, Source> {
    fn from(&mut self, source: &Source);
}

impl<T, S, A> ParseFrom<A, S> for T
where
    T: crate::Get<A>,
    S: crate::Get<A, Value = <T as crate::Get<A>>::Value>,
    A: crate::Attr,
    <T as crate::Get<A>>::Value: Copy,
{
    fn from(&mut self, source: &S) {
        crate::Put::put(crate::Tagged::<A, _>::new(*<S as crate::Get<A>>::attr(source)), self);
    }
}
impl_variadic_parse_from!();

// // real
// use crate::{Get, Tagged, Attr, Put};
// impl<T, S, A1, A2, A3, A4> ParseFrom<(A1, A2, A3, A4), S> for T
// where
//     T: Get<A1> + Get<A2> + Get<A3> + Get<A4>,
//     S:
//         Get<A1, Value = <T as Get<A1>>::Value> +
//         Get<A2, Value = <T as Get<A2>>::Value> +
//         Get<A3, Value = <T as Get<A3>>::Value> +
//         Get<A4, Value = <T as Get<A4>>::Value>,
//     A1: Attr,
//     A2: Attr,
//     A3: Attr,
//     A4: Attr,
//     <T as Get<A1>>::Value: Copy,
//     <T as Get<A2>>::Value: Copy,
//     <T as Get<A3>>::Value: Copy,
//     <T as Get<A4>>::Value: Copy,
// {
//     fn from(&mut self, source: &S) {
//         Tagged::<A1, _>::new(*<S as Get<A1>>::attr(source)).put(self);
//         Tagged::<A2, _>::new(*<S as Get<A2>>::attr(source)).put(self);
//         Tagged::<A3, _>::new(*<S as Get<A3>>::attr(source)).put(self);
//         Tagged::<A4, _>::new(*<S as Get<A4>>::attr(source)).put(self);
//     }
// }