use bascet_variadic::iter;
use std::any::TypeId;
use std::fmt;

pub trait Attr: 'static {
    const ID: TypeId;
}

pub struct Reads<const N: usize>;
impl<const N: usize> Attr for Reads<N> {
    const ID: TypeId = TypeId::of::<Reads<N>>();
}
impl<const N: usize> fmt::Display for Reads<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reads[{N}]")
    }
}

pub struct Read<const N: usize>;
impl<const N: usize> Attr for Read<N> {
    const ID: TypeId = TypeId::of::<Read<N>>();
}

pub trait Implies<T> {}

#[iter(N=2..=16, M=1..=15; for (N, M) in N.zip(M))]
impl Implies<Reads<M>> for Reads<N> {}

#[iter(N=2..=16, M=1..=15; for (N, M) in N.zip(M))]
impl From<Reads<N>> for Reads<M> {
    fn from(_: Reads<N>) -> Self { Self }
}

pub trait Get<T> {
    type Value<'a> where Self: 'a;
    fn get<'a>(&'a self) -> Self::Value<'a>;
}

#[iter(N=1..=16; for N in N)]
impl<S> Get<Reads<N>> for S
where
    @N[S: Get<Read<#>>](sep=" , "),
{
    type Value<'a> = (@N[<S as Get<Read<#>>>::Value<'a>](sep=", "),) where S: 'a;
    fn get<'a>(&'a self) -> Self::Value<'a> {
        (@N[Get::<Read<#>>::get(self)](sep=", "),)
    }
}

// pub mod meta {
//     bascet_derive::define_attr!(Id, Umi, Depth, Countsketch);
// }

// pub mod block {
//     bascet_derive::define_attr!(Offset, Header, Compressed, Trailer);
// }